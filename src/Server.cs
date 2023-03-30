﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Demikernel.Interop;
using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Socket = Demikernel.Interop.Socket;
namespace Demikernel;

public abstract class Server : IDisposable, IAsyncDisposable
{
    /// <summary>
    /// Initialize the API
    /// </summary>
    public static unsafe void Initialize(params string[] args)
    {
        args ??= Array.Empty<string>();
        int len = args.Length; // for the NUL terminators
        if (args.Length > 128) throw new ArgumentOutOfRangeException(nameof(args), "Too many args, sorry");
        foreach (var arg in args)
        {
            len += Encoding.ASCII.GetByteCount(arg);
        }
        var lease = ArrayPool<byte>.Shared.Rent(len);
        fixed (byte* raw = lease)
        {
            byte** argv = stackalloc byte*[args.Length];
            int byteOffset = 0;
            for (int i = 0; i < args.Length; i++)
            {
                argv[i] = &raw[byteOffset];
                var arg = args[i];
                var count = Encoding.ASCII.GetBytes(arg, 0, arg.Length, lease, byteOffset);
                byteOffset += count;
                lease[byteOffset++] = 0; // NUL terminator
            }
            if (Interlocked.CompareExchange(ref s_running, 1, 0) == 0)
            {
                ApiResult result = LibDemikernel.init(args.Length, argv);
                Volatile.Write(ref s_running, 0);
                result.AssertSuccess(nameof(LibDemikernel.init));
            }
        }
        ArrayPool<byte>.Shared.Return(lease);
    }

    internal static int s_running;

    public abstract void Start();
    public abstract void Stop();
    public virtual void Dispose()
    {
        GC.SuppressFinalize(this);
        Stop();
    }

    public ValueTask DisposeAsync()
    {
        GC.SuppressFinalize(this);
        Stop();
        return new(Shutdown);
    }
    private protected readonly TaskCompletionSource _shutdown = new();
    public Task Shutdown => _shutdown.Task;

    [StructLayout(LayoutKind.Explicit, Pack = 1)]
    private protected readonly struct PendingOperation
    {
        [FieldOffset(0)]
        public readonly Socket Socket;
        [FieldOffset(4)]
        public readonly Opcode Opcode;
        [FieldOffset(8)]
        public readonly ScatterGatherArray Payload; // size: 48

        [FieldOffset(8)]
        public readonly int AddrSize;
        [FieldOffset(12)]
        public readonly byte AddrStart; // max size 44 before starts hitting end

        private const int MaxAddrSize = 44;

        public static PendingOperation Pop(Socket socket) => new(socket, Opcode.Pop);
        public static PendingOperation Connect(Socket socket, ReadOnlySpan<byte> address) => new(socket, Opcode.Connect, address);
        public static PendingOperation Push(Socket socket, in ScatterGatherArray payload) => new(socket, Opcode.Push, in payload);
        public static PendingOperation Accept(Socket socket) => new(socket, Opcode.Connect);

        internal unsafe QueueToken ExecuteDirect()
        {
            Unsafe.SkipInit(out QueueToken qt);
            switch (Opcode)
            {
                case Opcode.Pop:
                    LibDemikernel.pop(&qt, Socket).AssertSuccess(nameof(LibDemikernel.pop));
                    break;
                case Opcode.Connect:
                    fixed (byte* addrStart = &AddrStart)
                    {
                        LibDemikernel.connect(&qt, Socket, addrStart, AddrSize).AssertSuccess(nameof(LibDemikernel.connect));
                    }
                    break;
                case Opcode.Accept:
                    LibDemikernel.accept(&qt, Socket).AssertSuccess(nameof(LibDemikernel.accept));
                    break;
                case Opcode.Push:
                    fixed (ScatterGatherArray* payload = &Payload)
                    {
                        LibDemikernel.push(&qt, Socket, payload).AssertSuccess(nameof(LibDemikernel.push));
                        LibDemikernel.sgafree(payload).AssertSuccess(nameof(LibDemikernel.sgafree));
                    }
                    break;
                default:
                    ThrowInvalid(Opcode);
                    break;
            }
            return qt;
            static void ThrowInvalid(Opcode opcode) => throw new NotImplementedException("Unexpected operation: " + opcode);
        }

        private unsafe PendingOperation(Socket socket, Opcode opcode, ReadOnlySpan<byte> address) : this(socket, opcode)
        {
            AddrSize = address.Length;
            fixed (byte* addrStart = &AddrStart)
            {
                address.CopyTo(new Span<byte>(addrStart, MaxAddrSize)); // this max usage intentional to assert overflow conditions
            }
        }

        private PendingOperation(Socket socket, Opcode opcode, in ScatterGatherArray payload) : this(socket, opcode)
        {
            Payload = payload;
        }

        private PendingOperation(Socket socket, Opcode opcode)
        {
            Unsafe.SkipInit(out this);
            Socket = socket;
            Opcode = opcode;
        }
    }
}
public abstract class Server<TState> : Server
{
    private readonly HashSet<Socket> _liveSockets = new();

    public override sealed void Start()
    {
        if (Shutdown.IsCompleted)
        {
            throw new InvalidOperationException("This message-pump has already completed");
        }
        if (Interlocked.CompareExchange(ref s_running, 1, 0) == 0)
        {
            var thd = new Thread(static s => ((Server<TState>)s!).Execute())
            {
                Name = GetType().Name,
                IsBackground = true
            };
            thd.Start(this);
        }
        else
        {
            throw new InvalidOperationException("A message-pump is already running");
        }
    }

    public override sealed void Stop()
    {
        _keepRunning = false;
        lock (_pendingOperations)
        {   // wake up the loop if we're waiting for work
            Monitor.PulseAll(_pendingOperations);
        }
    }

    private readonly Queue<(TState State, PendingOperation Operation)> _pendingOperations = new();
    private volatile bool _pendingWork, _keepRunning = true;
    private int _messagePumpThreadId = -1;

    private QueueToken[] _liveOperations = Array.Empty<QueueToken>();
    private TState[] _liveStates = Array.Empty<TState>();
    private int _liveOperationCount = 0;

    private unsafe void Execute()
    {
        _messagePumpThreadId = Environment.CurrentManagedThreadId;
        var timeout = TimeSpec.Create(TimeSpan.FromMilliseconds(1)); // entirely arbitrary
        bool haveLock = false;
        try
        {
            Monitor.Enter(LibDemikernel.GlobalLock, ref haveLock);
            OnStart();
            while (_keepRunning)
            {
                // allow other threads to access the global lock for "sgalloc" and "close" (TODO: can we nuke this?)
                Monitor.Wait(LibDemikernel.GlobalLock, 0);

                if (_pendingWork)
                {
                    lock (_pendingOperations)
                    {
                        while (_pendingOperations.TryDequeue(out var tuple))
                        {
                            AddAsServer(tuple.State, in tuple.Operation);
                        }
                        _pendingWork = false;
                    }
                }

                if (_liveOperationCount == 0)
                {
                    lock (_pendingOperations)
                    {
                        if (_pendingOperations.Count != 0)
                        {   // already something to do (race condition)
                            _pendingWork = true; // just in case
                        }
                        else
                        {
                            Monitor.Wait(_pendingOperations);
                        }
                    }
                    continue; // go back and add them
                }

                // so, definitely something to do
                Debug.Assert(_liveOperationCount > 0);
                Unsafe.SkipInit(out QueueResult qr);
                int offset = 0;
                ApiResult result;
                fixed (QueueToken* qt = _liveOperations)
                {
                    result = LibDemikernel.wait_any(&qr, &offset, qt, _liveOperationCount, &timeout);
                }
                if (result == ApiResult.Timeout) continue;
                result.AssertSuccess(nameof(LibDemikernel.wait_any));
                Debug.Assert(offset >= 0 && offset < _liveOperationCount);

                // fixup the pending tokens by moving the last into the space we're vacating
                var state = _liveStates[offset];
                if (_liveOperationCount > 1)
                {
                    _liveOperations[offset] = _liveOperations[_liveOperationCount - 1];
                    _liveStates[offset] = _liveStates[_liveOperationCount - 1];
                }
                // wipe the state to allow collect if necessary (we don't need to wipe the token)
                _liveStates[--_liveOperationCount] = default!;

                bool beginRead = false;
                var readSocket = qr.Socket;
                TState origState = state;
                switch (qr.Opcode)
                {
                    case Opcode.Accept:
                        beginRead = OnAccept(qr.Socket, ref state, in qr.AcceptResult);
                        readSocket = qr.AcceptResult.Socket;
                        break;
                    case Opcode.Connect:
                        beginRead = OnConnect(qr.Socket, ref state);
                        break;
                    case Opcode.Pop:
                        beginRead = OnPop(qr.Socket, ref state, in qr.ScatterGatherArray) && !qr.ScatterGatherArray.IsEmpty;
                        break;
                    case Opcode.Push:
                        OnPush(qr.Socket, ref state);
                        break;
                }
                if (beginRead)
                {
                    // immediately begin a pop; we already know we have space in the array
                    Unsafe.SkipInit(out QueueToken qt);
                    LibDemikernel.pop(&qt, readSocket).AssertSuccess(nameof(LibDemikernel.pop));
                    GrowIfNeeded();
                    _liveOperations[_liveOperationCount] = qt;
                    _liveStates[_liveOperationCount++] = state;

                    if (qr.Opcode == Opcode.Accept)
                    {
                        // accept again, making sure we grow if needed
                        LibDemikernel.accept(&qt, qr.Socket).AssertSuccess(nameof(LibDemikernel.accept));
                        GrowIfNeeded();
                        _liveOperations[_liveOperationCount] = qt;
                        _liveStates[_liveOperationCount++] = origState;
                    }
                }
            }
            PrepareForServerExit(true);
            _shutdown.TrySetResult();
        }
        catch (Exception ex)
        {
            Debug.WriteLine(ex.Message);
            PrepareForServerExit(false);
            _shutdown.TrySetException(ex);
        }
        finally
        {
            if (haveLock)
            {
                Monitor.Exit(LibDemikernel.GlobalLock);
            }
        }
    }

    private void PrepareForServerExit(bool assertCleanShutdown)
    {
        foreach (var socket in _liveSockets)
        {
            var apiResult = LibDemikernel.close(socket);
            if (assertCleanShutdown) apiResult.AssertSuccess(nameof(LibDemikernel.close));
        }
        _liveSockets.Clear();
        Volatile.Write(ref s_running, 0);
        _messagePumpThreadId = -1;
    }

    private bool IsServerThread => _messagePumpThreadId == Environment.CurrentManagedThreadId;

    protected virtual void OnStart() { }
    protected virtual bool OnPop(Socket socket, ref TState state, in ScatterGatherArray payload)
    {
        if (payload.IsEmpty) Close(socket);
        payload.Dispose();
        return false; // true===read again
    }
    protected virtual void OnPush(Socket socket, ref TState state) { }
    protected virtual bool OnConnect(Socket socket, ref TState state) => true; // true===start reading
    protected virtual bool OnAccept(Socket socket, ref TState state, in AcceptResult accept) => true; // true===start reading & accept again

    // requests a read operation from the specified socket
    private void Pop(Socket socket, TState state)
    {
        var pending = PendingOperation.Pop(socket);
        if (IsServerThread)
        {
            AddAsServer(state, in pending);
        }
        else
        {
            AddPending(state, in pending);
        }
    }

    protected void Close(Socket socket)
    {
        AssertServer();
        if (_liveSockets.Remove(socket))
        {
            LibDemikernel.close(socket).AssertSuccess(nameof(LibDemikernel.close));
        }
    }

    // performs a push operation to the specified socket, and releases the payload once written (which may not be immediately)
    protected unsafe void Push(Socket socket, TState state, in ScatterGatherArray payload)
    {
        var pending = PendingOperation.Push(socket, payload);
        if (IsServerThread)
        {
            AddAsServer(state, in pending);
        }
        else
        {
            AddPending(state, in pending);
        }
    }

    private void AddAsServer(TState state, in PendingOperation value)
    {
        GrowIfNeeded();
        _liveOperations[_liveOperationCount] = value.ExecuteDirect();
        _liveStates[_liveOperationCount++] = state;
    }

    private void GrowIfNeeded()
    {
        if (_liveOperationCount == _liveOperations.Length) Grow();
    }
    private void Grow()
    {
        int newLen;
        checked
        {
            newLen = (int)BitOperations.RoundUpToPowerOf2((uint)_liveOperationCount + 1);
        }
        Array.Resize(ref _liveOperations, newLen);
        Array.Resize(ref _liveStates, newLen);
    }

    private void AddPending(TState state, in PendingOperation value)
    {
        lock (_pendingOperations)
        {
            if (!_keepRunning) ThrowDoomed();
            _pendingOperations.Enqueue((state, value));
            _pendingWork = true;
            if (_pendingOperations.Count == 1) Monitor.PulseAll(_pendingOperations);
        }

        static void ThrowDoomed() => throw new InvalidOperationException("The message pump is shutting down");
    }

    private void AssertServer([CallerMemberName] string caller = "")
    {
        if (!IsServerThread) Throw(caller);
        static void Throw(string caller) => throw new InvalidOperationException($"The {caller} method must only be called from the message-pump");
    }

    protected unsafe Socket Accept(TState state, AddressFamily addressFamily, SocketType socketType, ProtocolType protocol, EndPoint endPoint, int backlog, int acceptCount = 1)
    {
        AssertServer();

        var addr = endPoint.Serialize();
        var len = addr.Size;
        if (len > 128) throw new InvalidOperationException($"Endpoint address too large: {len} bytes");
        byte* saddr = stackalloc byte[len];
        for (int i = 0; i < len; i++)
        {
            saddr[i] = addr[i];
        }

        Unsafe.SkipInit(out Socket socket);
        LibDemikernel.socket(&socket, addressFamily, socketType, protocol).AssertSuccess(nameof(LibDemikernel.socket));
        if (!_liveSockets.Add(socket)) throw new InvalidOperationException("Duplicate socket: " + socket);

        LibDemikernel.bind(socket, saddr, len).AssertSuccess(nameof(LibDemikernel.bind));
        LibDemikernel.listen(socket, backlog).AssertSuccess(nameof(LibDemikernel.listen));

        for (int i = 0; i < acceptCount; i++)
        {
            Unsafe.SkipInit(out QueueToken qt);
            LibDemikernel.accept(&qt, socket).AssertSuccess(nameof(LibDemikernel.accept));

            GrowIfNeeded();
            _liveOperations[_liveOperationCount] = qt;
            _liveStates[_liveOperationCount++] = state;
        }
        return socket;
    }
}