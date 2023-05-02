// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers;
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using Demikernel.Interop;
using Socket = Demikernel.Interop.Socket;
namespace Demikernel;

public class Demikernel<T>
{
    internal static int s_running;

    private QueueToken[] _liveOperations = Array.Empty<QueueToken>();
    private T[] _liveStates = Array.Empty<T>();
    private int _liveOperationCount = 0;

    // Callback function for handling the completion of an accept operation.
    private Action<T, Demikernel.Interop.Socket> OnAccept;
    // Callback function for handling the completion of a connect operation.
    private Action<T, Demikernel.Interop.Socket> OnConnect;
    // Callback function for handling the completion of a push operation.
    private Action<T, Demikernel.Interop.Socket> OnPush;
    // Callback function for handling the completion of a pop operation.
    private Action<T, Demikernel.Interop.Socket, Demikernel.Interop.ScatterGatherArray> OnPop;

    // Initializes Demikernel.
    public unsafe Demikernel(
        Action<T, Demikernel.Interop.Socket> OnAccept,
        Action<T, Demikernel.Interop.Socket> OnConnect,
        Action<T, Demikernel.Interop.Socket> OnPush,
        Action<T, Demikernel.Interop.Socket, Demikernel.Interop.ScatterGatherArray> OnPop,
        params string[] args)
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

        // Register callbacks.
        this.OnAccept = OnAccept;
        this.OnConnect = OnConnect;
        this.OnPush = OnPush;
        this.OnPop = OnPop;

        Start();
    }

    private void Start()
    {
        if (Interlocked.CompareExchange(ref s_running, 1, 0) == 0)
        {
            var thread = new Thread(static s => ((Demikernel<T>)s!).Execute())
            {
                Name = GetType().Name,
                IsBackground = true
            };
            thread.Start(this);
        }
        else
        {
            throw new InvalidOperationException("A message-pump is already running");
        }
    }

    /// Creates a socket.
    public unsafe Socket Socket(AddressFamily addressFamily, SocketType socketType, ProtocolType protocol)
    {
        Unsafe.SkipInit(out Socket socket);
        LibDemikernel.socket(&socket, addressFamily, socketType, protocol).AssertSuccess(nameof(LibDemikernel.socket));
        return socket;
    }

    /// Binds a socket to an endpoint.
    public unsafe void Bind(Socket socket, EndPoint endPoint)
    {
        var addr = endPoint.Serialize();
        var len = addr.Size;
        if (len > 128) throw new InvalidOperationException($"Endpoint address too large: {len} bytes");
        byte* saddr = stackalloc byte[len];
        for (int i = 0; i < len; i++)
        {
            saddr[i] = addr[i];
        }
        LibDemikernel.bind(socket, saddr, len).AssertSuccess(nameof(LibDemikernel.bind));
    }

    /// Enables a socket to accept incoming connections.
    public unsafe void Listen(Socket socket, int backlog)
    {
        LibDemikernel.listen(socket, backlog).AssertSuccess(nameof(LibDemikernel.listen));
    }

    public unsafe void Accept(T state, Socket socket)
    {
        Unsafe.SkipInit(out QueueToken qt);
        LibDemikernel.accept(&qt, socket).AssertSuccess(nameof(LibDemikernel.accept));

        GrowIfNeeded();
        _liveOperations[_liveOperationCount] = qt;
        _liveStates[_liveOperationCount] = state;
        _liveOperationCount++;
    }

    public unsafe void Pop(T state, Socket socket)
    {
        Unsafe.SkipInit(out QueueToken qt);
        LibDemikernel.pop(&qt, socket).AssertSuccess(nameof(LibDemikernel.pop));

        GrowIfNeeded();
        _liveOperations[_liveOperationCount] = qt;
        _liveStates[_liveOperationCount] = state;
        _liveOperationCount++;
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


    private unsafe void Execute()
    {
        System.Console.WriteLine("background thread running");
        var timeout = TimeSpec.Create(TimeSpan.FromMilliseconds(1)); // entirely arbitrary
        bool haveLock = false;
        try
        {
            Monitor.Enter(LibDemikernel.GlobalLock, ref haveLock);
            while (true)
            {
                // allow other threads to access the global lock for "sgalloc" and "close" (TODO: can we nuke this?)
                Monitor.Wait(LibDemikernel.GlobalLock, 0);

                if (_liveOperationCount == 0)
                {
                    continue; // go back and add them
                }

                // so, definitely something to do
                Unsafe.SkipInit(out QueueResult qr);
                int offset = 0;
                ApiResult result;
                fixed (QueueToken* qt = _liveOperations)
                {
                    result = LibDemikernel.wait_any(&qr, &offset, qt, _liveOperationCount, null);
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

                var readSocket = qr.Socket;
                T origState = state;
                switch (qr.Opcode)
                {
                    case Opcode.Accept:
                        System.Console.WriteLine(qr.Socket + " " + qr.AcceptResult.Socket);
                        this.OnAccept(state, qr.AcceptResult.Socket);
                        break;
                    case Opcode.Connect:
                        this.OnConnect(state, qr.Socket);
                        break;
                    case Opcode.Pop:
                        this.OnPop(state, qr.Socket, qr.ScatterGatherArray);
                        LibDemikernel.sgafree(&qr.ScatterGatherArray).AssertSuccess(nameof(LibDemikernel.sgafree));
                        break;
                    case Opcode.Push:
                        this.OnPush(state, qr.Socket);
                        break;
                }
            }
        }
        catch (Exception ex)
        {
            Debug.WriteLine(ex.Message);
        }
        finally
        {
            if (haveLock)
            {
                Monitor.Exit(LibDemikernel.GlobalLock);
            }
        }

    }
}
