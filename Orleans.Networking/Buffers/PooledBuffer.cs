﻿using System.Buffers;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Orleans.Networking.Buffers;

namespace Orleans.Networking.Buffers;

/// <summary>
/// A <see cref="IBufferWriter{T}"/> implementation implemented using pooled arrays which is specialized for creating <see cref="ReadOnlySequence{T}"/> instances.
/// </summary>
[StructLayout(LayoutKind.Auto)]
public struct PooledBuffer : IBufferWriter<byte>, IDisposable
{
    private SequenceSegment? _first;
    private SequenceSegment? _last;
    private SequenceSegment? _writeHead;
    private int _totalLength;
    private int _currentPosition;

    /// <summary>
    /// Initializes a new instance of the <see cref="PooledBuffer"/> struct.
    /// </summary>
    public PooledBuffer()
    {
        _first = _last = null;
        _writeHead = null;
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

        if (_writeHead is not null && _currentPosition > 0)
        {
            _writeHead.Array.AsSpan(0, _currentPosition).CopyTo(resultSpan);
        }

        return result;
    }

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Advance(int bytes)
    {
        if (_writeHead is null || _currentPosition > _writeHead.Array.Length)
        {
            ThrowInvalidOperation();
        }

        _currentPosition += bytes;

        [DoesNotReturn]
        [MethodImpl(MethodImplOptions.NoInlining)]
        static void ThrowInvalidOperation() => throw new InvalidOperationException("Attempted to advance past the end of a buffer.");
    }

    public void Reset()
    {
        var current = _first;
        while (current != null)
        {
            var previous = current;
            current = previous.Next as SequenceSegment;
            previous.Return();
        }

        _writeHead?.Return();

        _first = _last = _writeHead = null;
        _currentPosition = _totalLength = 0;
    }

    /// <inheritdoc/>
    public void Dispose() => Reset();

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Memory<byte> GetMemory(int sizeHint = 0)
    {
        if (_writeHead is null || sizeHint >= _writeHead.Array.Length - _currentPosition)
        {
            return GetMemorySlow(sizeHint);
        }

        return _writeHead.AsMemory(_currentPosition);
    }

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Span<byte> GetSpan(int sizeHint = 0)
    {
        if (_writeHead is null || sizeHint >= _writeHead.Array.Length - _currentPosition)
        {
            return GetSpanSlow(sizeHint);
        }

        return _writeHead.Array.AsSpan(_currentPosition);
    }

    /// <summary>Copies the contents of this writer to a span.</summary>
    public readonly void CopyTo(Span<byte> output)
    {
        var current = _first;
        while (output.Length > 0 && current != null)
        {
            var segment = current.CommittedMemory.Span;
            var slice = segment[..Math.Min(segment.Length, output.Length)];
            slice.CopyTo(output);
            output = output[slice.Length..];
            current = current.Next as SequenceSegment;
        }

        if (output.Length > 0 && _currentPosition > 0 && _writeHead is not null)
        {
            var span = _writeHead.Array.AsSpan(0, Math.Min(output.Length, _currentPosition));
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

        if (_currentPosition > 0 && _writeHead is not null)
        {
            writer.Write(_writeHead.Array.AsSpan(0, _currentPosition));
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

        return new ReadOnlySequence<byte>(_first!, 0, _last!, _last!.CommittedMemory.Length);
    }

    public BufferSlice Slice() => new(this, 0, Length);

    public BufferSlice Slice(int offset) => new(this, offset, Length - offset);

    public BufferSlice Slice(int offset, int length) => new(this, offset, length);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Write(ReadOnlySequence<byte> input)
    {
        foreach (var segment in input)
        {
            Write(segment.Span);
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Write(ReadOnlySpan<byte> value)
    {
        Span<byte> destination = GetSpan();

        // Fast path, try copying to the available memory directly
        if (value.Length <= destination.Length)
        {
            value.CopyTo(destination);
            Advance(value.Length);
        }
        else
        {
            WriteMultiSegment(value, destination);
        }
    }

    private void WriteMultiSegment(in ReadOnlySpan<byte> source, Span<byte> destination)
    {
        ReadOnlySpan<byte> input = source;
        while (true)
        {
            int writeSize = Math.Min(destination.Length, input.Length);
            input.Slice(0, writeSize).CopyTo(destination);
            Advance(writeSize);
            input = input.Slice(writeSize);
            if (input.Length > 0)
            {
                destination = GetSpan();

                continue;
            }

            return;
        }
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private Span<byte> GetSpanSlow(int sizeHint) => Grow(sizeHint).Array;

    [MethodImpl(MethodImplOptions.NoInlining)]
    private Memory<byte> GetMemorySlow(int sizeHint) => Grow(sizeHint).AsMemory(0);

    private SequenceSegment Grow(int sizeHint)
    {
        Commit();
        var newBuffer = SequenceSegmentPool.Shared.Rent(sizeHint);
        return _writeHead = newBuffer;
    }

    private void Commit()
    {
        if (_currentPosition == 0 || _writeHead is null)
        {
            return;
        }

        _writeHead.Commit(_totalLength, _currentPosition);
        _totalLength += _currentPosition;
        if (_first is null)
        {
            _first = _writeHead;
            _last = _writeHead;
        }
        else
        {
            Debug.Assert(_last is not null);
            _last.SetNext(_writeHead);
            _last = _writeHead;
        }

        _writeHead = null;
        _currentPosition = 0;
    }

    public BufferSlice.MemorySequence AsMemorySequence() => Slice().AsMemorySequence();

    public BufferSlice.SpanSequence AsSpanSequence() => Slice().AsSpanSequence();

    public readonly struct BufferSlice
    {
        private readonly PooledBuffer _writer;
        private readonly int _offset;

        public BufferSlice(in PooledBuffer writer, int start, int length)
        {
            _writer = writer;
            _offset = start;
            Length = length;
        }

        public readonly int Length { get; init; }

        /// <summary>Copies the contents of this writer to a span.</summary>
        public readonly int CopyTo(Span<byte> output)
        {
            int copied = 0;
            foreach (var span in this)
            {
                span.CopyTo(output);
                output = output[span.Length..];
                copied += span.Length;
            }

            return copied;
        }

        /// <summary>Copies the contents of this writer to a pooled buffer.</summary>
        public readonly void CopyTo(ref PooledBuffer output)
        {
            foreach (var span in this)
            {
                output.Write(span);
            }
        }

        /// <summary>Copies the contents of this writer to a buffer writer.</summary>
        public readonly void CopyTo<TBufferWriter>(ref TBufferWriter output) where TBufferWriter : struct, IBufferWriter<byte>
        {
            foreach (var span in this)
            {
                output.Write(span);
            }
        }

        /// <summary>
        /// Returns the data which has been written as an array.
        /// </summary>
        /// <returns>The data which has been written.</returns>
        public readonly byte[] ToArray()
        {
            var result = new byte[Length];
            Span<byte> resultSpan = result;
            foreach (var span in this)
            {
                span.CopyTo(resultSpan);
                resultSpan = resultSpan[span.Length..];
            }

            return result;
        }

        public readonly MemorySequence AsMemorySequence() => new(in this);

        public readonly SpanSequence AsSpanSequence() => new(in this);

        /*
        public ReadOnlySequence<byte> AsReadOnlySequence()
        {
            if (Length == 0)
            {
                return ReadOnlySequence<byte>.Empty;
            }

            _writer.Commit();
            SequenceSegment? start = _writer._first;
            FindSegment(_offset, ref start);
            SequenceSegment? end = start;
            FindSegment(_offset + Length, ref end);

            Debug.Assert(start is not null);
            Debug.Assert(end is not null);

            var startOffset = _offset - start.RunningIndex;
            if (start == end && startOffset == 0)
            {
                if (startOffset == 0)
                {
                    return new ReadOnlySequence<byte>(start!.CommittedMemory);
                }
                
                return new ReadOnlySequence<byte>(start.CommittedMemory[(int)startOffset..]);
            }

            var endOffset = _offset + Length - end.RunningIndex;
            return new ReadOnlySequence<byte>(start, 0, end, (int)endOffset);

            static void FindSegment(int offset, ref SequenceSegment? current)
            {
                while (current is not null && current.RunningIndex + current.CommittedMemory.Length < offset)
                {
                    current = current.Next as SequenceSegment;
                }
            }
        }
        */

        public readonly SpanEnumerator GetEnumerator() => new(in this);

        public readonly ref struct MemorySequence
        {
            private readonly ref BufferSlice _slice;
            public MemorySequence(in BufferSlice slice) => _slice = slice;
            public MemoryEnumerator GetEnumerator() => new(in _slice);
        }

        public readonly ref struct SpanSequence
        {
            private readonly ref BufferSlice _slice;
            public SpanSequence(in BufferSlice slice) => _slice = slice;
            public SpanEnumerator GetEnumerator() => new(in _slice);
        }

        public ref struct SpanEnumerator
        {
            private static readonly SequenceSegment InitialSegmentSentinel = new();
            private static readonly SequenceSegment FinalSegmentSentinel = new();
            private readonly BufferSlice _slice;
            private int _position;
            private SequenceSegment? _segment;

            public SpanEnumerator(in BufferSlice slice)
            {
                _slice = slice;
                _segment = InitialSegmentSentinel;
                Current = Span<byte>.Empty;
            }

            internal readonly PooledBuffer Writer => _slice._writer;
            internal readonly int Offset => _slice._offset;
            internal readonly int Length => _slice.Length;

            public ReadOnlySpan<byte> Current { get; private set; }

            public bool MoveNext()
            {
                if (ReferenceEquals(_segment, InitialSegmentSentinel))
                {
                    _segment = _slice._writer._first;
                }

                var endPosition = Offset + Length;
                while (_segment != null && _segment != FinalSegmentSentinel)
                {
                    var segment = _segment.CommittedMemory.Span;

                    // Find the starting segment and the offset to copy from.
                    int segmentOffset;
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
                            segmentOffset = Offset - _position;
                        }
                    }
                    else
                    {
                        segmentOffset = 0;
                    }

                    Current = segment[segmentOffset..Math.Min(segment.Length - segmentOffset, endPosition - (_position + segmentOffset))];
                    _position += Current.Length;
                    _segment = _segment.Next as SequenceSegment;
                    return true;
                }

                if (_segment != FinalSegmentSentinel && Writer._currentPosition > 0 && Writer._writeHead is { } head && _position < endPosition)
                {
                    var offset = Math.Max(Offset - _position, 0);
                    Current = head.Array.AsSpan(offset, Math.Min(Writer._currentPosition, endPosition - (_position + offset)));
                    _segment = FinalSegmentSentinel;
                    return true;
                }

                return false;
            }
        }

        public ref struct MemoryEnumerator
        {
            private static readonly SequenceSegment InitialSegmentSentinel = new();
            private static readonly SequenceSegment FinalSegmentSentinel = new();
            private readonly BufferSlice _slice;
            private int _position;
            private SequenceSegment? _segment;

            public MemoryEnumerator(in BufferSlice slice)
            {
                _slice = slice;
                _segment = InitialSegmentSentinel;
                Current = Memory<byte>.Empty;
            }

            internal readonly PooledBuffer Writer => _slice._writer;
            internal readonly int Offset => _slice._offset;
            internal readonly int Length => _slice.Length;

            public ReadOnlyMemory<byte> Current { get; private set; }

            public bool MoveNext()
            {
                if (ReferenceEquals(_segment, InitialSegmentSentinel))
                {
                    _segment = _slice._writer._first;
                }

                var endPosition = Offset + Length;
                while (_segment != null)
                {
                    var segment = _segment.CommittedMemory;

                    // Find the starting segment and the offset to copy from.
                    int segmentOffset;
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
                            segmentOffset = Offset - _position;
                        }
                    }
                    else
                    {
                        segmentOffset = 0;
                    }

                    Current = segment[segmentOffset..Math.Min(segment.Length - segmentOffset, endPosition - (_position + segmentOffset))];
                    _position += Current.Length;
                    _segment = _segment.Next as SequenceSegment;
                    return true;
                }

                if (_segment != FinalSegmentSentinel && Writer._currentPosition > 0 && Writer._writeHead is { } head)
                {
                    var offset = Math.Max(Offset - _position, 0);
                    Current = head.Array.AsMemory(offset, Math.Min(Writer._currentPosition, endPosition - (_position + offset)));
                    _segment = FinalSegmentSentinel;
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
            SequenceSegment? block;
            if (size <= MinimumBlockSize)
            {
                if (!_blocks.TryDequeue(out block))
                {
                    block = new SequenceSegment(size);
                }
            }
            else if (_largeBlocks.TryDequeue(out block))
            {
                block.ResizeLargeSegment(size);
                return block;
            }

            return block ?? new SequenceSegment(size);
        }

        internal void Return(SequenceSegment block)
        {
            Debug.Assert(block.IsValid);
            if (block.IsMinimumSize)
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
        internal SequenceSegment()
        {
            Array = System.Array.Empty<byte>();
        }

        internal SequenceSegment(int length)
        {
            InitializeArray(length);
        }

        public void ResizeLargeSegment(int length)
        {
            Debug.Assert(length > SequenceSegmentPool.MinimumBlockSize);
            InitializeArray(length);
        }

        [MemberNotNull(nameof(Array))]
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void InitializeArray(int length)
        {
            if (length <= SequenceSegmentPool.MinimumBlockSize)
            {
                Debug.Assert(Array is null);
                var pinnedArray = GC.AllocateUninitializedArray<byte>(SequenceSegmentPool.MinimumBlockSize, pinned: true);
                Array = pinnedArray;
            }
            else
            {
                // Round up to a power of two.
                length = (int)BitOperations.RoundUpToPowerOf2((uint)length);

                if (Array is not null)
                {
                    // The segment has an appropriate size already.
                    if (Array.Length == length)
                    {
                        return;
                    }

                    // The segment is being resized.
                    ArrayPool<byte>.Shared.Return(Array);
                }

                Array = ArrayPool<byte>.Shared.Rent(length);
            }
        }

        public byte[] Array { get; private set; }

        public ReadOnlyMemory<byte> CommittedMemory => Memory;

        public bool IsValid => Array is { Length: > 0 };
        public bool IsMinimumSize => Array.Length == SequenceSegmentPool.MinimumBlockSize;

        public Memory<byte> AsMemory(int offset) => AsMemory(offset, Array.Length - offset);

        public Memory<byte> AsMemory(int offset, int length)
        {
            if (IsMinimumSize)
            {
                return MemoryMarshal.CreateFromPinnedArray(Array, offset, length);
            }

            return Array.AsMemory(offset, length);
        }

        public void Commit(long runningIndex, int length)
        {
            RunningIndex = runningIndex;
            Memory = AsMemory(0, length);
        }

        public void SetNext(SequenceSegment next) => Next = next;

        public void Return()
        {
            RunningIndex = default;
            Next = default;
            Memory = default;

            SequenceSegmentPool.Shared.Return(this);
        }
    }
}
