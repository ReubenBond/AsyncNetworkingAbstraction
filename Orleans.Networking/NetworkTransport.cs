#nullable enable

using Orleans;
using System.Buffers;

namespace Orleans.Networking;

public abstract class ReadRequest
{
    public abstract Memory<byte> Buffer { get; }
    public abstract bool OnProgress(int bytesRead);
    public abstract void OnError(Exception error);
}

public abstract class WriteRequest
{
    public bool IsSingleBuffer { get; set; }
    public abstract ReadOnlyMemory<byte> Buffer { get; }
    public abstract ReadOnlySequence<byte> Buffers { get; }
    public abstract void OnCompleted();
    public abstract void OnError(Exception error);
}

public abstract class NetworkTransport : IAsyncDisposable
{
    public abstract bool ReadAsync(ReadRequest request);
    public abstract bool WriteAsync(WriteRequest request);
    public abstract ValueTask CloseAsync(Exception? closeException);
    public abstract ValueTask DisposeAsync();
    public abstract CancellationToken Closed { get; }
}
