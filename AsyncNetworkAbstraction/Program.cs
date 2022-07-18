using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Orleans.Networking.Transport;
using Orleans.Serialization.Buffers.Adaptors;
using System.Buffers;
using System.Buffers.Binary;
using System.Buffers.Text;
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Text;

namespace AsyncNetworkAbstraction
{
    internal class Program
    {
        public ILogger? Logger { get; private set; }

        static async Task Main(string[] args)
        {
            Console.WriteLine("Hello, World!");
            var prog = new Program();
            await prog.RunAsync(args);
        }

        public async Task RunAsync(string[] args)
        {
            var host = new HostBuilder().ConfigureServices(services => services.AddLogging(logging => logging.AddConsole())).Build();
            Logger = host.Services.GetRequiredService<ILogger<Program>>();

            var endpoint = new IPEndPoint(IPAddress.Loopback, 8885);
            var server = RunServer(endpoint);
            var client = RunClient(endpoint);
            await server;
            await client;
        }


        public sealed class MessageWriteRequest : WriteRequest
        {
            TaskCompletionSource _completion = new();
            ReadOnlyMemory<byte> _buffer;
            public MessageWriteRequest(string message)
            {
                var count = Encoding.UTF8.GetByteCount(message);
                var bytes = new byte[count + sizeof(int)];
                BinaryPrimitives.WriteInt32LittleEndian(bytes, (int)count);
                var written = Encoding.UTF8.GetBytes(message, bytes.AsSpan(sizeof(int)));
                Debug.Assert(written == count);

                _buffer = bytes;
            }

            public override ReadOnlyMemory<byte> Buffer => _buffer;

            public override ReadOnlySequence<byte> Buffers => new(_buffer);

            public Task Completed => _completion.Task;

            public override void OnCompleted()
            {
                _completion.TrySetResult();
            }

            public override void OnError(Exception error)
            {
                _completion.TrySetException(error);
            }
        }

        public sealed class MessageReadRequest2 : ReadRequest, IDisposable
        {
            TaskCompletionSource<string> _completion = new();
            private PooledArrayBufferWriter _buffer = new();
            private int _messageLength;

            public Task<string> Completed => _completion.Task;
            public override Memory<byte> Buffer => _buffer.GetMemory();

            public int FramedLength => sizeof(int) + _messageLength;
            public int UnconsumedLength => _buffer.Length - FramedLength;

            public void Dispose() => _buffer.Dispose();

            public override void OnError(Exception error)
            {
                _completion.SetException(error);
            }

            public override bool OnProgress(int bytesRead)
            {
                _buffer.Advance(bytesRead);
                if (_buffer.Length < sizeof(int))
                {
                    return false;
                }

                if (_messageLength < 0)
                {
                    Span<byte> lengthBytes = stackalloc byte[sizeof(int)];
                    _buffer.CopyTo(lengthBytes);
                    _messageLength = BinaryPrimitives.ReadInt32LittleEndian(lengthBytes);
                }

                if (_buffer.Length < FramedLength)
                {
                    return false;
                }

                var payloadSpan = _buffer.ToArray().AsSpan(sizeof(int));
                var result = Encoding.UTF8.GetString(payloadSpan);
                _completion.SetResult(result);
                return true;
            }
        }

        public sealed class MessageReadRequest : ReadRequest
        {
            TaskCompletionSource<string> _completion = new();
            private byte[] _buffer = new byte[4096];
            private int _length = -1;
            private int _totalRead;

            public Task<string> Completed => _completion.Task;
            public override Memory<byte> Buffer => _buffer.AsMemory(_totalRead);

            public override void OnError(Exception error)
            {
                _completion.SetException(error);
            }

            public override bool OnProgress(int bytesRead)
            {
                _totalRead += bytesRead;
                if (_totalRead < sizeof(int))
                {
                    return false;
                }

                if (_length < 0)
                {
                    _length = BinaryPrimitives.ReadInt32LittleEndian(_buffer.AsSpan(0, sizeof(int)));
                }

                if (_totalRead < _length)
                {
                    return false;
                }

                var result = Encoding.UTF8.GetString(_buffer.AsSpan(sizeof(int), _length));
                _completion.SetResult(result);
                return true;
            }
        }

        public async Task RunClient(EndPoint endpoint)
        {
            var server = await ConnectAsync(endpoint, CancellationToken.None);

            var count = 0;
            while (true)
            {
                var message = $"Hello, World! {count}";
                var writeRequest = new MessageWriteRequest(message);
                using var readRequest = new MessageReadRequest2();
                var canWrite = server.WriteAsync(writeRequest);
                Debug.Assert(canWrite);
                var canRead = server.ReadAsync(readRequest);
                Debug.Assert(canRead);
                await writeRequest.Completed;
                var result = await readRequest.Completed;
                Logger!.LogInformation("Client read result: {Result}", result);
                ++count;
            }
        }

        public async Task RunServer(EndPoint endpoint)
        {
            var socket = StartServer(endpoint);
            while (true)
            {
                var client = await AcceptAsync(socket);
                if (client is null) break;
                _ = Task.Run(() => ServeClient(client));
            }
        }

        public async Task ServeClient(NetworkTransport client)
        {
            while (true)
            {
                using var readRequest = new MessageReadRequest2();
                var canRead = client.ReadAsync(readRequest);
                Debug.Assert(canRead);
                var message = await readRequest.Completed;
                Logger!.LogInformation("Server read request: {Message}", message);
                var writeRequest = new MessageWriteRequest($"Echo: {message}");
                var canWrite = client.WriteAsync(writeRequest);
                Debug.Assert(canWrite);
                await writeRequest.Completed;
            }
        }

        public Socket StartServer(EndPoint endpoint)
        {
            var listenSocket = new Socket(endpoint.AddressFamily, SocketType.Stream, ProtocolType.Tcp)
            {
                LingerState = new LingerOption(true, 0),
                NoDelay = true
            };

            listenSocket.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.ReuseAddress, true);
            listenSocket.EnableFastPath();

            // Kestrel expects IPv6Any to bind to both IPv6 and IPv4
            if (endpoint is IPEndPoint ip && ip.Address == IPAddress.IPv6Any)
            {
                listenSocket.DualMode = true;
            }

            try
            {
                listenSocket.Bind(endpoint);
            }
            catch (SocketException e) when (e.SocketErrorCode == SocketError.AddressAlreadyInUse)
            {
                throw new AddressInUseException(e.Message, e);
            }

            //EndPoint = listenSocket.LocalEndPoint;

            listenSocket.Listen(512);

            return listenSocket;
        }

        public async ValueTask<NetworkTransport?> AcceptAsync(Socket listenSocket, CancellationToken cancellationToken = default)
        {
            while (true)
            {
                try
                {
                    var acceptSocket = await listenSocket.AcceptAsync();
                    acceptSocket.NoDelay = true;

                    var connection = new TcpNetworkTransport(acceptSocket, Logger!);

                    connection.Start();

                    return connection;
                }
                catch (ObjectDisposedException)
                {
                    // A call was made to UnbindAsync/DisposeAsync just return null which signals we're done
                    return null;
                }
                catch (SocketException e) when (e.SocketErrorCode == SocketError.OperationAborted)
                {
                    // A call was made to UnbindAsync/DisposeAsync just return null which signals we're done
                    return null;
                }
                catch (SocketException exception)
                {
                    _ = exception;
                    // The connection got reset while it was in the backlog, so we try again.
                    //_trace.ConnectionReset(connectionId: "(null)");
                }
            }
        }

        public async ValueTask<NetworkTransport> ConnectAsync(EndPoint endpoint, CancellationToken cancellationToken)
        {
            var socket = new Socket(endpoint.AddressFamily, SocketType.Stream, ProtocolType.Tcp)
            {
                LingerState = new LingerOption(true, 0),
                NoDelay = true
            };

            //socket.EnableFastPath();
            using var completion = new SingleUseSocketAsyncEventArgs
            {
                RemoteEndPoint = endpoint
            };

            if (socket.ConnectAsync(completion))
            {
                using (cancellationToken.Register(s => Socket.CancelConnectAsync((SingleUseSocketAsyncEventArgs)s!), completion))
                {
                    await completion.Task;
                }
            }

            if (completion.SocketError != SocketError.Success)
            {
                if (completion.SocketError == SocketError.OperationAborted)
                    cancellationToken.ThrowIfCancellationRequested();
                throw new SocketConnectionException($"Unable to connect to {endpoint}. Error: {completion.SocketError}");
            }

            var connection = new TcpNetworkTransport(socket, Logger!);
            connection.Start();
            return connection;
        }

        private sealed class SingleUseSocketAsyncEventArgs : SocketAsyncEventArgs
        {
            private readonly TaskCompletionSource completion = new();

            public Task Task => completion.Task;

            protected override void OnCompleted(SocketAsyncEventArgs _) => this.completion.SetResult();
        }
    }
}
