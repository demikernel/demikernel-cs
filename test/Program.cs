class Program
{

    System.Collections.Generic.List<Demikernel.Interop.Socket> clients = new();

    Demikernel.Demikernel<Program> demi;

    public Program(string[] args)
    {
        this.demi = new Demikernel.Demikernel<Program>(this.OnAccept, this.OnConnect, this.OnPush, this.OnPop, args);
    }

    static void Main(string[] args)
    {
        var server = new Program(args);

        // TODO: read bind address from args.
        string address = "127.0.0.1";
        string port = "6379";
        System.Net.IPAddress ip = address == null ? System.Net.IPAddress.Any : System.Net.IPAddress.Parse(address);

        System.Net.IPEndPoint endPoint = new System.Net.IPEndPoint(ip, int.Parse(port));

        // Create a TCP socket.
        Demikernel.Interop.Socket socket = server.demi.Socket(ip.AddressFamily, System.Net.Sockets.SocketType.Stream, System.Net.Sockets.ProtocolType.Tcp);

        // Bind TCP socket to a local address.
        server.demi.Bind(socket, endPoint);

        // Enable incoming connections.
        server.demi.Listen(socket, 16);

        // Accept incoming connections.
        server.demi.Accept(server, socket);

        System.Console.WriteLine("Hello, World!");

        while (true) ;
    }

    private void OnAccept(Program server, Demikernel.Interop.Socket socket)
    {
        System.Console.WriteLine("OnAccept(): socket=" + socket);

        server.clients.Add(socket);

        // Pop a message from the socket.
        server.demi.Pop(server, socket);
    }

    private void OnConnect(Program server, Demikernel.Interop.Socket socket)
    {
        // todo.
        System.Console.WriteLine("OnConnect(): " + socket);
        while (true) ;
    }

    private void OnPush(Program server, Demikernel.Interop.Socket socket)
    {
        // todo.
        System.Console.WriteLine("OnPush(): " + socket);
        while (true) ;
    }

    private void OnPop(
        Program server,
        Demikernel.Interop.Socket socket,
        Demikernel.Interop.ScatterGatherArray sga)
    {
        // todo.
        System.Console.WriteLine("OnPop(): " + socket + " " + sga.SegmentCount);


        // Pop another message from the socket.
        server.demi.Pop(server, socket);
    }
}
