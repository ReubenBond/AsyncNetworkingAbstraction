using Orleans.Networking;
using Orleans.Networking.Buffers;
using System.Buffers;
using System.Buffers.Binary;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks.Sources;

namespace AsyncNetworkAbstraction;

public sealed class MessageWriteRequest : WriteRequest, IValueTaskSource, IDisposable
{
    private ManualResetValueTaskSourceCore<int> _completion = new();
    private PooledBuffer _buffer = new();

    public MessageWriteRequest()
    {
        _buffer = new PooledBuffer();
    }

    public void Set(string message)
    {
        var payloadSize = Encoding.UTF8.GetByteCount(message);
        var totalSize = sizeof(int) + payloadSize;
        var span = _buffer.GetSpan(totalSize);
        BinaryPrimitives.WriteInt32LittleEndian(span, payloadSize);
        var written = Encoding.UTF8.GetBytes(message, span[sizeof(int)..]);
        Debug.Assert(written == payloadSize);
        _buffer.Advance(totalSize);
    }

    public void Reset()
    {
        _completion.Reset();
        _buffer.Reset();
    }

    public void Dispose() => Reset();

    public override ReadOnlyMemory<byte> Buffer => throw new InvalidOperationException();

    public override ReadOnlySequence<byte> Buffers => _buffer.AsReadOnlySequence();

    public ValueTask Completed => new(this, _completion.Version);

    public override void OnCompleted()
    {
        _completion.SetResult(0);
    }

    public override void OnError(Exception error)
    {
        _completion.SetException(error);
    }

    void IValueTaskSource.OnCompleted(Action<object?> continuation, object? state, short token, ValueTaskSourceOnCompletedFlags flags) => _completion.OnCompleted(continuation, state, token, flags);
    void IValueTaskSource.GetResult(short token) => _completion.GetResult(token);
    ValueTaskSourceStatus IValueTaskSource.GetStatus(short token) => _completion.GetStatus(token);

    public override string ToString() => RuntimeHelpers.GetHashCode(this).ToString("X");
}
