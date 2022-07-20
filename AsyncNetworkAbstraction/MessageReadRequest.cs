using Orleans.Networking.Transport;
using Orleans.Serialization.Buffers.Adaptors;
using System.Buffers.Binary;
using System.Runtime.CompilerServices;
using System.Threading.Tasks.Sources;

namespace AsyncNetworkAbstraction
{
    public sealed class MessageReadRequest : ReadRequest, IValueTaskSource, IDisposable
        {
            private ManualResetValueTaskSourceCore<int> _completion = new();
            private PooledBuffer _buffer = new();
            private int _messageLength = 0;

            public ValueTask Completed => new(this, _completion.Version);
            public override Memory<byte> Buffer => _buffer.GetMemory();

            public int FramedLength => sizeof(int) + _messageLength;
            public int UnconsumedLength => _buffer.Length - FramedLength;

            public PooledBuffer.BufferSlice Payload => _buffer.Slice(sizeof(int), _messageLength);

            public PooledBuffer.BufferSlice Unconsumed => _buffer.Slice(FramedLength);

            public void SetBuffer(in PooledBuffer buffer)
            {
                _buffer = buffer;
            
                    Span<byte> lengthBytes = stackalloc byte[sizeof(int)];
                    _buffer.CopyTo(lengthBytes);
                    var len = BinaryPrimitives.ReadInt32LittleEndian(lengthBytes);
            }

            public void Reset()
            {
                _messageLength = 0;
                _completion.Reset();
                _buffer.Reset();
            }

            public override void OnError(Exception error)
            {
                _completion.SetException(error);
            }

            public bool TryParseMessage()
            {
                if (_buffer.Length < sizeof(int))
                {
                    return false;
                }

                if (_messageLength == 0)
                {
                    Span<byte> lengthBytes = stackalloc byte[sizeof(int)];
                    _buffer.CopyTo(lengthBytes);
                    _messageLength = BinaryPrimitives.ReadInt32LittleEndian(lengthBytes);
                    Console.WriteLine($"{this} Length: {_messageLength}");
                }

                if (_buffer.Length < FramedLength)
                {
                    return false;
                }

                _completion.SetResult(0);
                return true;
            }

            public override bool OnProgress(int bytesRead)
            {
                _buffer.Advance(bytesRead);
                return TryParseMessage();
            }

            public void Dispose() => Reset();
            void IValueTaskSource.OnCompleted(Action<object?> continuation, object? state, short token, ValueTaskSourceOnCompletedFlags flags) => _completion.OnCompleted(continuation, state, token, flags);
            void IValueTaskSource.GetResult(short token) => _completion.GetResult(token);
            ValueTaskSourceStatus IValueTaskSource.GetStatus(short token) => _completion.GetStatus(token);

        public override string ToString() => RuntimeHelpers.GetHashCode(this).ToString("X");
    }
}
