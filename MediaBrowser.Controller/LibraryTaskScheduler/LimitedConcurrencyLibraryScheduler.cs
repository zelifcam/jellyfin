using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using MediaBrowser.Controller.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace MediaBrowser.Controller.LibraryTaskScheduler;

/// <summary>
/// Provides Parallel action interface to process tasks with a set concurrency level.
/// </summary>
public sealed class LimitedConcurrencyLibraryScheduler : ILimitedConcurrencyLibraryScheduler, IAsyncDisposable
{
    private const int CleanupGracePeriod = 60;
    private readonly IHostApplicationLifetime _hostApplicationLifetime;
    private readonly ILogger<LimitedConcurrencyLibraryScheduler> _logger;
    private readonly IServerConfigurationManager _serverConfigurationManager;
    private readonly Dictionary<CancellationTokenSource, Task> _taskRunners = new();

    private static readonly AsyncLocal<CancellationTokenSource> _deadlockDetector = new();

    /// <summary>
    /// Gets used to lock all operations on the Tasks queue and creating workers.
    /// </summary>
    private readonly Lock _taskLock = new();

    private readonly Channel<TaskQueueItem> _tasks = Channel.CreateUnbounded<TaskQueueItem>();

    private volatile int _workCounter;
    private Task? _cleanupTask;
    private bool _disposed;

    /// <summary>
    /// Initializes a new instance of the <see cref="LimitedConcurrencyLibraryScheduler"/> class.
    /// </summary>
    /// <param name="hostApplicationLifetime">The hosting lifetime.</param>
    /// <param name="logger">The logger.</param>
    /// <param name="serverConfigurationManager">The server configuration manager.</param>
    public LimitedConcurrencyLibraryScheduler(
        IHostApplicationLifetime hostApplicationLifetime,
        ILogger<LimitedConcurrencyLibraryScheduler> logger,
        IServerConfigurationManager serverConfigurationManager)
    {
        _hostApplicationLifetime = hostApplicationLifetime;
        _logger = logger;
        _serverConfigurationManager = serverConfigurationManager;
    }

    private void ScheduleTaskCleanup()
    {
        lock (_taskLock)
        {
            if (_cleanupTask is not null)
            {
                _logger.LogDebug("Cleanup task already scheduled.");
                // cleanup task is already running.
                return;
            }

            _cleanupTask = RunCleanupTask();
        }

        async Task RunCleanupTask()
        {
            _logger.LogDebug("Schedule cleanup task in {CleanupGracePerioid} sec.", CleanupGracePeriod);
            await Task.Delay(TimeSpan.FromSeconds(CleanupGracePeriod)).ConfigureAwait(false);
            if (_disposed)
            {
                _logger.LogDebug("Abort cleaning up, already disposed.");
                return;
            }

            lock (_taskLock)
            {
                if (_tasks.Reader.Count > 0 || _workCounter > 0)
                {
                    _logger.LogDebug("Delay cleanup task, operations still running.");
                    // tasks are still there so its still in use. Reschedule cleanup task.
                    // we cannot just exit here and rely on the other invoker because there is a considerable timeframe where it could have already ended.
                    _cleanupTask = RunCleanupTask();
                    return;
                }
            }

            _logger.LogDebug("Cleanup runners.");
            foreach (var item in _taskRunners.ToArray())
            {
                await item.Key.CancelAsync().ConfigureAwait(false);
                _taskRunners.Remove(item.Key);
            }
        }
    }

    private bool ShouldForceSequentialOperation()
    {
        // if the user either set the setting to 1 or it's unset and we have fewer than 4 cores it's better to run sequentially.
        var fanoutSetting = _serverConfigurationManager.Configuration.LibraryScanFanoutConcurrency;
        return fanoutSetting == 1 || (fanoutSetting <= 0 && Environment.ProcessorCount <= 3);
    }

    private int CalculateScanConcurrencyLimit()
    {
        // when this is invoked, we already checked ShouldForceSequentialOperation for the sequential check.
        var fanoutConcurrency = _serverConfigurationManager.Configuration.LibraryScanFanoutConcurrency;
        if (fanoutConcurrency <= 0)
        {
            // in case the user did not set a limit manually, we can assume he has 3 or more cores as already checked by ShouldForceSequentialOperation.
            return Environment.ProcessorCount - 3;
        }

        return fanoutConcurrency;
    }

    private void Worker()
    {
        lock (_taskLock)
        {
            var operationFanout = Math.Max(0, CalculateScanConcurrencyLimit() - _taskRunners.Count);
            _logger.LogDebug("Spawn {NumberRunners} new runners.", operationFanout);
            for (int i = 0; i < operationFanout; i++)
            {
                var stopToken = new CancellationTokenSource();
                var combinedSource = CancellationTokenSource.CreateLinkedTokenSource(stopToken.Token, _hostApplicationLifetime.ApplicationStopping);
                _taskRunners.Add(
                    combinedSource,
                    Task.Factory.StartNew(
                        ItemWorker,
                        (combinedSource, stopToken),
                        combinedSource.Token,
                        TaskCreationOptions.PreferFairness,
                        TaskScheduler.Default));
            }
        }
    }

    private async Task ItemWorker(object? obj)
    {
        var stopToken = ((CancellationTokenSource TaskStop, CancellationTokenSource GlobalStop))obj!;
        _deadlockDetector.Value = stopToken.TaskStop;
        try
        {
            while (!stopToken.GlobalStop.Token.IsCancellationRequested)
            {
                var item = await _tasks.Reader.ReadAsync(stopToken.GlobalStop.Token).ConfigureAwait(false);
                try
                {
                    var newWorkerLimit = Interlocked.Increment(ref _workCounter) > 0;
                    Debug.Assert(newWorkerLimit, "_workCounter > 0");
                    _logger.LogDebug("Process new item '{Data}'.", item.Data);
                    await ProcessItem(item).ConfigureAwait(false);
                }
                finally
                {
                    var newWorkerLimit = Interlocked.Decrement(ref _workCounter) >= 0;
                    Debug.Assert(newWorkerLimit, "_workCounter > 0");
                }
            }
        }
        catch (OperationCanceledException) when (stopToken.TaskStop.IsCancellationRequested)
        {
            // thats how you do it, interupt the waiter thread. There is nothing to do here when it was on purpose.
        }
        finally
        {
            _logger.LogDebug("Cleanup Runner'.");
            _deadlockDetector.Value = default!;
            _taskRunners.Remove(stopToken.TaskStop);
            stopToken.GlobalStop.Dispose();
            stopToken.TaskStop.Dispose();
        }
    }

    private async Task ProcessItem(TaskQueueItem item)
    {
        try
        {
            if (item.CancellationToken.IsCancellationRequested)
            {
                // if item is cancelled, just skip it
                return;
            }

            await item.Worker(item.Data).ConfigureAwait(true);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error while performing a library operation");
        }
        finally
        {
            item.Progress.Report(100);
            item.Done.SetResult();
        }
    }

    /// <inheritdoc/>
    public async Task Enqueue<T>(T[] data, Func<T, IProgress<double>, Task> worker, IProgress<double> progress, CancellationToken cancellationToken)
    {
        if (_disposed)
        {
            return;
        }

        if (data.Length == 0 || cancellationToken.IsCancellationRequested)
        {
            progress.Report(100);
            return;
        }

        _logger.LogDebug("Enqueue new Workset of {NoItems} items.", data.Length);

        TaskQueueItem[] workItems = null!;

        void UpdateProgress()
        {
            progress.Report(workItems.Select(e => e.ProgressValue).Average());
        }

        workItems = data.Select(item =>
        {
            TaskQueueItem queueItem = null!;
            return queueItem = new TaskQueueItem()
            {
                Data = item!,
                Progress = new Progress<double>(innerPercent =>
                    {
                        // round the percent and only update progress if it changed to prevent excessive UpdateProgress calls
                        var innerPercentRounded = Math.Round(innerPercent);
                        if (queueItem.ProgressValue != innerPercentRounded)
                        {
                            queueItem.ProgressValue = innerPercentRounded;
                            UpdateProgress();
                        }
                    }),
                Worker = (val) => worker((T)val, queueItem.Progress),
                CancellationToken = cancellationToken
            };
        }).ToArray();

        if (ShouldForceSequentialOperation())
        {
            _logger.LogDebug("Process sequentially (forced). items={Count}", workItems.Length);
            try
            {
                foreach (var item in workItems)
                {
                    await ProcessItem(item).ConfigureAwait(false);
                }
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                // operation is cancelled. Do nothing.
            }

            _logger.LogDebug("Process sequentially done.");
            return;
        }

        if (_deadlockDetector.Value is not null)
        {
            // Nested invocation from within a runner — process sequentially inline.
            // Do NOT write to the channel: that risks cross-runner item stealing and
            // circular waits between in-place loops.
            _logger.LogDebug("Process sequentially (nested). items={Count}", workItems.Length);
            try
            {
                for (var idx = 0; idx < workItems.Length; idx++)
                {
                    var item = workItems[idx];
                    await ProcessItem(item).ConfigureAwait(false);
                }
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                // operation is cancelled. Do nothing.
            }

            _logger.LogDebug("Process sequentially done.");
        }
        else
        {
            // Top-level parallel path.
            _logger.LogDebug("Process parallel. items={Count}", workItems.Length);
            for (var i = 0; i < workItems.Length; i++)
            {
                await _tasks.Writer.WriteAsync(workItems[i], CancellationToken.None).ConfigureAwait(false);
            }

            Worker();

            // When items outnumber runners, a bare Task.WhenAll deadlocks because
            // excess items sit in the channel with no free runner to pick them up.
            // Instead, actively drain the channel while waiting. Set the deadlock
            // detector so any nested Enqueue from items we process goes sequential.
            var drainSentinel = new CancellationTokenSource();
            _deadlockDetector.Value = drainSentinel;
            _logger.LogDebug("Wait for {NoWorkers} to complete.", workItems.Length);
            try
            {
                while (workItems.Any(e => !e.Done.Task.IsCompleted))
                {
                    if (cancellationToken.IsCancellationRequested)
                    {
                        break;
                    }

                    if (_tasks.Reader.TryRead(out var channelItem))
                    {
                        _logger.LogDebug("Drain processing '{Data}'.", channelItem.Data);
                        await ProcessItem(channelItem).ConfigureAwait(false);
                    }
                    else
                    {
                        // Channel empty — all items are with runners or already done.
                        // Wait for any to complete rather than spinning.
                        var pending = workItems
                            .Where(e => !e.Done.Task.IsCompleted)
                            .Select(e => e.Done.Task)
                            .ToArray();
                        if (pending.Length > 0)
                        {
                            await Task.WhenAny(pending).ConfigureAwait(false);
                        }
                    }
                }
            }
            finally
            {
                // Restore — the calling thread is not a runner.
                _deadlockDetector.Value = null!;
                drainSentinel.Dispose();
            }

            _logger.LogDebug("{NoWorkers} completed.", workItems.Length);
            ScheduleTaskCleanup();
        }
    }

    /// <inheritdoc/>
    public async ValueTask DisposeAsync()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;
        _tasks.Writer.Complete();
        foreach (var item in _taskRunners)
        {
            await item.Key.CancelAsync().ConfigureAwait(false);
        }

        if (_cleanupTask is not null)
        {
            await _cleanupTask.ConfigureAwait(false);
            _cleanupTask?.Dispose();
        }
    }

    private class TaskQueueItem
    {
        public required object Data { get; init; }

        public double ProgressValue { get; set; }

        public required Func<object, Task> Worker { get; init; }

        public required IProgress<double> Progress { get; init; }

        public TaskCompletionSource Done { get; } = new();

        public CancellationToken CancellationToken { get; init; }
    }
}
