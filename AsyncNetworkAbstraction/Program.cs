using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Orleans.Networking.Transport;
using Orleans.Serialization.Buffers.Adaptors;
using System.ComponentModel.DataAnnotations;
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
            //await prog.MessageParsing();
        }

        public async Task MessageParsing()
        {
            var readRequest = new MessageReadRequest();
            var writeRequest = new MessageWriteRequest();
            var pipe = new PooledBuffer();
            while (true)
            {
                for (var i = 0; i < 100; i++)
                {
                    writeRequest.Set(Guid.NewGuid().ToString("N"));
                    var buffers = writeRequest.Buffers;
                    foreach (var seg in buffers)
                    {
                        pipe.Write(seg.Span);
                    }
                    writeRequest.OnCompleted();
                    await writeRequest.Completed;
                    writeRequest.Reset();
                }

                readRequest.SetBuffer(pipe);
                while (readRequest.TryParseMessage())
                {
                    await readRequest.Completed;

                    var msg = ParseMessage(readRequest);
                    Console.WriteLine(msg);

                    if (readRequest.UnconsumedLength > 0)
                    {
                        var excess = new PooledBuffer();
                        var unconsumed = readRequest.Unconsumed;
                        unconsumed.CopyTo(ref excess);
                        readRequest.Reset();
                        readRequest.SetBuffer(excess);
                    }
                    else
                    {
                        readRequest.Reset();
                    }
                }

                pipe.Reset();
            }

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

        public async Task RunClient(EndPoint endpoint)
        {
            var server = await ConnectAsync(endpoint, CancellationToken.None);

            var processSendsTask = ProcessSends(server);
            var processReceivesTask = ProcessReceives(server);

            await processSendsTask;
            await processReceivesTask;
        }

        private async Task ProcessSends(NetworkTransport server)
        {
            await Task.Yield();
            var count = 0;
            using var writeRequest = new MessageWriteRequest();
            while (true)
            {
                var message = Guid.NewGuid().ToString("N");
                writeRequest.Set(message);
                var canWrite = server.WriteAsync(writeRequest);
                Debug.Assert(canWrite);
                await writeRequest.Completed;
                //Logger!.LogInformation("Wrote Message #{Count}", count);
                if (count % 10 == 0) await Task.Delay(10);
                await Task.Yield();
                writeRequest.Reset();
                ++count;
            }
        }

        private async Task ProcessReceives(NetworkTransport server)
        {
            var count = 0;
            var readRequest = new MessageReadRequest();
            var excessBuffer = new PooledBuffer();
            while (true)
            {
                if (!readRequest.Completed.IsCompleted)
                {
                    var canRead = server.ReadAsync(readRequest);
                    Debug.Assert(canRead);
                }

                await readRequest.Completed;
                var result = ParseMessage(readRequest);
                Logger!.LogInformation("Client read result: \"{Message}\" (excess bytes: {Excess})", result, readRequest.UnconsumedLength);

                if (readRequest.UnconsumedLength > 0)
                {
                    excessBuffer = new();
                    readRequest.Unconsumed.CopyTo(ref excessBuffer);
                    var previous = readRequest;
                    readRequest.Reset();
                    readRequest.SetBuffer(excessBuffer);
                    var needsMoreData = !readRequest.TryParseMessage();
                    if (!needsMoreData)
                    {
                        Logger!.LogInformation("Next message is already ready (\"{Message}\"), previous req was {previous}", ParseMessage(readRequest), previous);
                    }
                }
                else
                {
                    Console.WriteLine($"Resetting {readRequest}");
                    readRequest.Reset();
                    Logger!.LogInformation("Next message is not ready");
                }

                ++count;
            }
        }

        private string ParseMessage(MessageReadRequest request)
        {
            var slice = request.Payload;
            var payloadSpan = slice.ToArray().AsSpan();
            return Encoding.UTF8.GetString(payloadSpan);
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
            var readRequest = new MessageReadRequest();
            using var writeRequest = new MessageWriteRequest();
            while (true)
            {
                if (!readRequest.Completed.IsCompleted)
                {
                    var canRead = client.ReadAsync(readRequest);
                    Debug.Assert(canRead);
                }

                await readRequest.Completed;
                var message = ParseMessage(readRequest);

                Logger!.LogInformation("Server read request: \"{Message}\" ({Excess} bytes excess)", message, readRequest.UnconsumedLength);
                if (readRequest.UnconsumedLength > 0)
                {
                //var entireBuffer = Encoding.UTF8.GetString(readRequest.Unconsumed.ToArray());
                    var excessBuffer = new PooledBuffer();
                    readRequest.Unconsumed.CopyTo(ref excessBuffer);
                    var previous = readRequest;
                    readRequest.Reset();
                    readRequest.SetBuffer(excessBuffer);

                    //excessBuffer.Dispose();
                    var needsMoreData = !readRequest.TryParseMessage();
                    if (!needsMoreData)
                    {
                        Logger!.LogInformation("Next message is already ready (\"{Message}\"), {Previous}", ParseMessage(readRequest), previous);
                    }
                }
                else
                {
                    readRequest.Reset();
                    Logger!.LogInformation("Next message is not ready");
                }

                writeRequest.Set($"Echo: {message}");
                Logger!.LogInformation("Server responding with: {Message}", message);
                var canWrite = client.WriteAsync(writeRequest);
                Debug.Assert(canWrite);
                await writeRequest.Completed;
                writeRequest.Reset();
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
