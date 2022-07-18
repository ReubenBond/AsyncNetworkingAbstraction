using System.Buffers;
using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Orleans.Serialization.Buffers.Adaptors;

/// <summary>
/// A <see cref="IBufferWriter{T}"/> implementation implemented using pooled arrays which is specialized for creating <see cref="ReadOnlySequence{T}"/> instances.
/// </summary>
[StructLayout(LayoutKind.Auto)]
public struct PooledArrayBufferWriter : IBufferWriter<byte>, IDisposable
{
    private SequenceSegment? _first;
    private SequenceSegment? _last;
    private SequenceSegment? _current;
    private int _totalLength;
    private int _currentPosition;

    /// <summary>
    /// Initializes a new instance of the <see cref="PooledArrayBufferWriter"/> struct.
    /// </summary>
    public PooledArrayBufferWriter()
    {
        _first = _last = null;
        _current = null;
        _totalLength = 0;
        _currentPosition = 0;
    }

    /// <summary>Gets the total length which has been written.</summary>
    public readonly int Length => _totalLength + _currentPosition;

    /// <summary>
    /// Returns the data which has been written as an array.
    /// </summary>
    /// <returns>The data which has been written.</returns>
    public readonly byte[] ToArray()
    {
        var result = new byte[Length];
        var resultSpan = result.AsSpan();
        var current = _first;
        while (current != null)
        {
            var span = current.CommittedMemory.Span;
            span.CopyTo(resultSpan);
            resultSpan = resultSpan[span.Length..];
            current = current.Next as SequenceSegment;
        }

        if (_current is not null && _currentPosition > 0)
        {
            _current.Array.AsSpan(0, _currentPosition).CopyTo(resultSpan);
        }

        return result;
    }

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Advance(int bytes)
    {
        if (_current is null || _currentPosition > _current.Array.Length)
        {
            ThrowInvalidOperation();
        }

        _currentPosition += bytes;

        [DoesNotReturn]
        [MethodImpl(MethodImplOptions.NoInlining)]
        static void ThrowInvalidOperation() => throw new InvalidOperationException("Attempted to advance past the end of a buffer.");
    }

    /// <inheritdoc/>
    public void Dispose()
    {
        var current = _first;
        while (current != null)
        {
            var previous = current;
            current = previous.Next as SequenceSegment;
            previous.Return();
        }

        _current?.Return();

        _first = _last = null;
        _current = null;
        _currentPosition = 0;
        _totalLength = 0;
    }

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Memory<byte> GetMemory(int sizeHint = 0)
    {
        if (_current is null || sizeHint >= _current.Array.Length - _currentPosition)
        {
            return GetMemorySlow(sizeHint);
        }

        return _current.AsMemory(_currentPosition);
    }

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Span<byte> GetSpan(int sizeHint)
    {
        if (_current is null || sizeHint >= _current.Array.Length - _currentPosition)
        {
            return GetSpanSlow(sizeHint);
        }

        return _current.Array.AsSpan(_currentPosition);
    }

    /// <summary>Copies the contents of this writer to a span.</summary>
    public readonly void CopyTo(Span<byte> output)
    {
        var current = _first;
        while (current != null)
        {
            var segment = current.CommittedMemory.Span;
            var slice = segment[..Math.Min(segment.Length, output.Length)];
            slice.CopyTo(output);
            output = output[slice.Length..];
            current = current.Next as SequenceSegment;
        }

        if (_currentPosition > 0 && _current is not null)
        {
            var span = _current.Array.AsSpan(0, Math.Min(output.Length, _currentPosition));
            span.CopyTo(output);
        }
    }

    /// <summary>Copies the contents of this writer to another writer.</summary>
    public readonly void CopyTo<TBufferWriter>(ref TBufferWriter writer) where TBufferWriter : IBufferWriter<byte>
    {
        var current = _first;
        while (current != null)
        {
            var span = current.CommittedMemory.Span;
            writer.Write(span);
            current = current.Next as SequenceSegment;
        }

        if (_currentPosition > 0 && _current is not null)
        {
            writer.Write(_current.Array.AsSpan(0, _currentPosition));
        }
    }

    /// <summary>
    /// Returns a new <see cref="ReadOnlySequence{T}"/> which must not be accessed after disposing this instance.
    /// </summary>
    public ReadOnlySequence<byte> AsReadOnlySequence()
    {
        if (Length == 0)
        {
            return ReadOnlySequence<byte>.Empty;
        }

        Commit();
        if (_first == _last)
        {
            return new ReadOnlySequence<byte>(_first!.CommittedMemory);
        }

        return new ReadOnlySequence<byte>(_first, 0, _last, _last.CommittedMemory.Length);
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private Span<byte> GetSpanSlow(int sizeHint) => Grow(sizeHint).Array;

    [MethodImpl(MethodImplOptions.NoInlining)]
    private Memory<byte> GetMemorySlow(int sizeHint) => Grow(sizeHint).AsMemory(0);

    private SequenceSegment Grow(int sizeHint)
    {
        Commit();
        var newBuffer = SequenceSegmentPool.Shared.Rent(sizeHint);
        return _current = newBuffer;
    }

    private void Commit()
    {
        if (_currentPosition == 0 || _current is null)
        {
            return;
        }

        _current.Commit(_totalLength, _currentPosition);
        _totalLength += _currentPosition;
        if (_first is null)
        {
            _first = _current;
            _last = _current;
        }
        else
        {
            _last.SetNext(_current);
            _last = _current;
        }

        _current = null;
        _currentPosition = 0;
    }

    public readonly ref struct Slice
    {
        private readonly ref PooledArrayBufferWriter _writer;
        public Slice(ref PooledArrayBufferWriter writer, int start, int length)
        {
            _writer = writer;
            Offset = start;
            Length = length;
        }

        internal int Offset { get; init; }
        internal int Length { get; init; }

        /// <summary>Copies the contents of this writer to a span.</summary>
        public readonly void CopyTo(Span<byte> output)
        {
            var endPosition = Offset + Length;
            var position = 0;
            var current = _writer._first;
            while (current != null)
            {
                var segment = current.CommittedMemory.Span;

                // Find the starting segment and the offset to copy from.
                int offset;
                if (position < Offset)
                {
                    if (position + segment.Length <= Offset)
                    {
                        // Start is in a subsequent segment
                        position += segment.Length;
                        current = current.Next as SequenceSegment;
                        continue;
                    }
                    else
                    {
                        // Start is in this segment
                        offset = Offset - position;
                    }
                }
                else
                {
                    offset = 0;
                }

                // Copy the relevant data from the current segment into the output.
                var slice = segment[offset..Math.Min(segment.Length - offset, endPosition - (position + offset))];
                slice.CopyTo(output);
                output = output[slice.Length..];
                position += slice.Length;
                current = current.Next as SequenceSegment;
            }

            if (_writer._currentPosition > 0 && _writer._current is not null)
            {
                var offset = Math.Max(Offset - position, 0);
                var slice = _writer._current.Array.AsSpan(offset, Math.Min(_writer._currentPosition, endPosition - (position + offset)));
                slice.CopyTo(output);
            }
        }

        /// <summary>Copies the contents of this writer to a span.</summary>
        public readonly void CopyTo<TBufferWriter>(ref TBufferWriter output) where TBufferWriter : IBufferWriter<byte>
        {
            foreach (var span in this)
            {
                output.Write(span);
            }
        }

        public SliceEnumerator GetEnumerator() => new(this);

        public ref struct SliceEnumerator
        {
            private static readonly SequenceSegment InitialSegmentSentinel = SequenceSegment.Empty;

            private readonly Slice _slice;
            private int _position;
            private SequenceSegment? _segment;

            public SliceEnumerator(Slice slice)
            {
                _slice = slice;
                _segment = InitialSegmentSentinel;
                Current = Span<byte>.Empty;
            }

            internal int Offset => _slice.Offset;
            internal int Length => _slice.Length;
            public ReadOnlySpan<byte> Current { get; private set; }

            public bool MoveNext()
            {
                if (ReferenceEquals(_segment, InitialSegmentSentinel))
                {
                    _segment = _slice._writer._first;
                }

                var endPosition = Offset + Length;
                while (_segment != null)
                {
                    var segment = _segment.CommittedMemory.Span;

                    // Find the starting segment and the offset to copy from.
                    int offset;
                    if (_position < Offset)
                    {
                        if (_position + segment.Length <= Offset)
                        {
                            // Start is in a subsequent segment
                            _position += segment.Length;
                            _segment = _segment.Next as SequenceSegment;
                            continue;
                        }
                        else
                        {
                            // Start is in this segment
                            offset = Offset - _position;
                        }
                    }
                    else
                    {
                        offset = 0;
                    }

                    Current = segment[offset..Math.Min(segment.Length - offset, endPosition - (_position + offset))];
                    _position += Current.Length;
                    _segment = _segment.Next as SequenceSegment;
                    return true;
                }

                if (_slice._writer._currentPosition > 0 && _slice._writer._current is not null)
                {
                    var offset = Math.Max(Offset - _position, 0);
                    Current = _slice._writer._current.Array.AsSpan(offset, Math.Min(_slice._writer._currentPosition, endPosition - (_position + offset)));
                    return true;
                }

                return false;
            }
        }
    }

    private sealed class SequenceSegmentPool
    {
        public static SequenceSegmentPool Shared { get; } = new();
        public const int MinimumBlockSize = 4 * 1024;
        private readonly ConcurrentQueue<SequenceSegment> _blocks = new();
        private readonly ConcurrentQueue<SequenceSegment> _largeBlocks = new();

        private SequenceSegmentPool() { }

        public SequenceSegment Rent(int size = -1)
        {
            SequenceSegment block;
            if (size <= MinimumBlockSize)
            {
                if (!_blocks.TryDequeue(out block))
                {
                    block = new SequenceSegment(size);
                }
            }
            else if (_largeBlocks.TryDequeue(out block))
            {
                block.InitializeLargeSegment(size);
                return block;
            }

            return block ?? new SequenceSegment(size);
        }

        internal void Return(SequenceSegment block)
        {
            if (block.IsStandardSize)
            {
                _blocks.Enqueue(block);
            }
            else
            {
                _largeBlocks.Enqueue(block);
            }
        }
    }

    private sealed class SequenceSegment : ReadOnlySequenceSegment<byte>
    {
        internal static SequenceSegment Empty => new();

        private SequenceSegment()
        {
            Array = System.Array.Empty<byte>();
        }

        internal SequenceSegment(int length)
        {
            InitializeSegment(length);
        }

        public void InitializeLargeSegment(int length)
        {
            InitializeSegment((int)BitOperations.RoundUpToPowerOf2((uint)length));
        }

        [MemberNotNull(nameof(Array))]
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void InitializeSegment(int length)
        {
            if (length <= SequenceSegmentPool.MinimumBlockSize)
            {
                var pinnedArray = GC.AllocateUninitializedArray<byte>(SequenceSegmentPool.MinimumBlockSize, pinned: true);
                Array = pinnedArray;
            }
            else
            {
                Array = ArrayPool<byte>.Shared.Rent(length);
            }
        }

        public byte[] Array { get; private set; }

        public ReadOnlyMemory<byte> CommittedMemory => base.Memory;

        public bool IsStandardSize => Array.Length == SequenceSegmentPool.MinimumBlockSize;

        public Memory<byte> AsMemory(int offset)
        {
            if (IsStandardSize)
            {
                return MemoryMarshal.CreateFromPinnedArray(Array, offset, Array.Length);
            }

            return Array.AsMemory(offset);
        }

        public Memory<byte> AsMemory(int offset, int length)
        {
            if (IsStandardSize)
            {
                return MemoryMarshal.CreateFromPinnedArray(Array, offset, length);
            }

            return Array.AsMemory(offset, length);
        }

        public void Commit(long runningIndex, int length)
        {
            RunningIndex = runningIndex;
            base.Memory = AsMemory(0, length);
        }

        public void SetNext(SequenceSegment next) => Next = next;

        public void Return()
        {
            RunningIndex = default;
            Next = default;
            base.Memory = default;

            if (IsStandardSize)
            {
                SequenceSegmentPool.Shared.Return(this);
            }
            else if (Array.Length > 0)
            {
                ArrayPool<byte>.Shared.Return(Array);
                Array = System.Array.Empty<byte>();
            }
        }
    }
}
