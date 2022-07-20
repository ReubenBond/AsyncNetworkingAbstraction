#nullable enable

using Microsoft.Extensions.Logging;
using Orleans.Networking.Utilities;
using System.Diagnostics.CodeAnalysis;

namespace Orleans.Networking.Streams;

public abstract class StreamNetworkTransport : NetworkTransport
{
    private readonly ILogger _logger;
    private readonly SingleWaiterInlineSignal _writerSignal = new();
    private readonly SingleWaiterInlineSignal _readerSignal = new();
    private readonly Queue<WriteRequest> _pendingWrites = new();
    private readonly Queue<ReadRequest> _pendingReads = new();
    private Task? _runTask;
    private readonly CancellationTokenSource _connectionClosingCts = new();
    private readonly CancellationTokenSource _connectionClosedCts = new();
    private readonly object _writesLock = new();
    private readonly object _readsLock = new();
    private Exception? _shutdownReason;

    protected StreamNetworkTransport(ILogger logger)
    {
        _logger = logger;
    }

    protected abstract Stream Stream { get; }

    public virtual void Start()
    {
        _runTask = RunAsync();
    }

    public override CancellationToken Closed => _connectionClosedCts.Token;

    public override async ValueTask CloseAsync(Exception? closeException)
    {
        _shutdownReason ??= closeException;
        _connectionClosingCts.Cancel();

        var completion = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        _connectionClosedCts.Token.Register(OnClosed, completion, useSynchronizationContext: false);
        await completion.Task;

        static void OnClosed(object? state)
        {
            if (state is not TaskCompletionSource completion) throw new ArgumentException($"State must be a {nameof(TaskCompletionSource)}", nameof(state));
            completion.TrySetResult();
        }
    }

    public override async ValueTask DisposeAsync()
    {
        await CloseAsync(null);
    }

    public override bool ReadAsync(ReadRequest request)
    {
        if (_connectionClosingCts.IsCancellationRequested)
        {
            return false;
        }

        lock (_readsLock)
        {
            _pendingReads.Enqueue(request);
        }

        _readerSignal.Signal();
        return true;
    }

    public override bool WriteAsync(WriteRequest request)
    {
        if (_connectionClosingCts.IsCancellationRequested)
        {
            return false;
        }

        lock (_writesLock)
        {
            _pendingWrites.Enqueue(request);
        }

        _writerSignal.Signal();
        return true;
    }

    private async Task RunAsync()
    {
        try
        {
            await RunAsyncCore();
        }
        finally
        {
            await DisposeAsync();
            _connectionClosedCts.Cancel();
        }
    }

    protected virtual async Task RunAsyncCore()
    {
        try
        {
            var readsTask = ProcessReads();
            var writesTask = ProcessWrites();
            await readsTask;
            await writesTask;
        }
        catch (Exception exception)
        {
            _shutdownReason ??= exception;
        }
    }

    private async Task ProcessReads()
    {
        await Task.Yield();
        Exception? error = default;
        ReadRequest? operation = default;
        try
        {
            while (!_connectionClosingCts.IsCancellationRequested)
            {
                while (TryDequeue(out operation))
                {
                    while (true)
                    {
                        var bytesRead = await Stream.ReadAsync(operation.Buffer);
                        if (bytesRead == 0)
                        {
                            error = new EndOfStreamException();
                            break;
                        }

                        if (operation.OnProgress(bytesRead))
                        {
                            break;
                        }
                    }

                    if (error is not null)
                    {
                        // Bubble the error up
                        break;
                    }
                }

                if (error is not null)
                {
                    // Bubble the error up
                    break;
                }

                await _readerSignal.WaitAsync();
            }
        }
        catch (Exception exception)
        {
            error ??= exception;
        }
        finally
        {
            _shutdownReason ??= error;
            if ((error ?? _shutdownReason) is { } reason)
            {
                operation?.OnError(reason);
            }

            if (error is not null)
            {
                _logger.LogError(0, error, $"Unexpected exception in {nameof(StreamNetworkTransport)}.{nameof(ProcessReads)}.");
            }

            _connectionClosingCts.Cancel();
        }

        bool TryDequeue([NotNullWhen(true)] out ReadRequest? operation)
        {
            lock (_readsLock)
            {
                return _pendingReads.TryDequeue(out operation);
            }
        }
    }

    private async Task ProcessWrites()
    {
        await Task.Yield();
        Exception? error = default;
        WriteRequest? operation = default;
        try
        {
            while (!_connectionClosingCts.IsCancellationRequested)
            {
                while (TryDequeue(out operation))
                {
                    if (operation.IsSingleBuffer)
                    {
                        await Stream.WriteAsync(operation.Buffer);
                    }
                    else
                    {
                        foreach (var buffer in operation.Buffers)
                        {
                            await Stream.WriteAsync(buffer);
                        }
                    }

                    operation.OnCompleted();
                }

                await _writerSignal.WaitAsync();
            }
        }
        catch (Exception exception)
        {
            error ??= exception;
        }
        finally
        {
            _shutdownReason ??= error;
            if ((error ?? _shutdownReason) is { } reason)
            {
                operation?.OnError(reason);
            }

            if (error is not null)
            {
                _logger.LogError(0, error, $"Unexpected exception in {nameof(StreamNetworkTransport)}.{nameof(ProcessWrites)}.");
            }

            _connectionClosingCts.Cancel();
        }

        bool TryDequeue([NotNullWhen(true)] out WriteRequest? operation)
        {
            lock (_writesLock)
            {
                return _pendingWrites.TryDequeue(out operation);
            }
        }
    }
}
