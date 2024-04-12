/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.general;

import static io.airbyte.metrics.lib.ApmTraceConstants.WORKER_OPERATION_NAME;

import datadog.trace.api.Trace;
import io.airbyte.commons.concurrency.BoundedConcurrentLinkedQueue;
import io.airbyte.commons.concurrency.ClosableLinkedBlockingQueue;
import io.airbyte.commons.concurrency.ClosableQueue;
import io.airbyte.commons.io.LineGobbler;
import io.airbyte.commons.timer.Stopwatch;
import io.airbyte.config.PerformanceMetrics;
import io.airbyte.config.ReplicationOutput;
import io.airbyte.metrics.lib.ApmTraceUtils;
import io.airbyte.metrics.lib.MetricAttribute;
import io.airbyte.metrics.lib.MetricClient;
import io.airbyte.metrics.lib.MetricClientFactory;
import io.airbyte.metrics.lib.MetricTags;
import io.airbyte.metrics.lib.OssMetricsRegistry;
import io.airbyte.persistence.job.models.ReplicationInput;
import io.airbyte.protocol.models.AirbyteMessage;
import io.airbyte.protocol.models.AirbyteMessage.Type;
import io.airbyte.protocol.models.AirbyteTraceMessage;
import io.airbyte.workers.RecordSchemaValidator;
import io.airbyte.workers.context.ReplicationContext;
import io.airbyte.workers.context.ReplicationFeatureFlags;
import io.airbyte.workers.exception.WorkerException;
import io.airbyte.workers.helper.StreamStatusCompletionTracker;
import io.airbyte.workers.internal.AirbyteDestination;
import io.airbyte.workers.internal.AirbyteSource;
import io.airbyte.workers.internal.DestinationTimeoutMonitor;
import io.airbyte.workers.internal.HeartbeatTimeoutChaperone;
import io.airbyte.workers.internal.exception.DestinationException;
import io.airbyte.workers.internal.exception.SourceException;
import io.airbyte.workers.internal.syncpersistence.SyncPersistence;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

/**
 * Implementation of a ReplicationWorker using buffers.
 * <p>
 * There is one thread per IO/Transform and buffers in between the different steps to apply
 * backpressure.
 */
public class BufferedReplicationWorker implements ReplicationWorker {

  private static final Logger LOGGER = LoggerFactory.getLogger(BufferedReplicationWorker.class);

  private final String jobId;
  private final Integer attempt;
  private final AirbyteSource source;
  private final AirbyteDestination destination;
  private final ReplicationWorkerHelper replicationWorkerHelper;
  private final ReplicationFeatureFlagReader replicationFeatureFlagReader;
  private final RecordSchemaValidator recordSchemaValidator;
  private final SyncPersistence syncPersistence;
  private final HeartbeatTimeoutChaperone srcHeartbeatTimeoutChaperone;
  private final ClosableQueue<AirbyteMessage> messagesFromSourceQueue;
  private final ClosableQueue<AirbyteMessage> messagesForDestinationQueue;
  private final ExecutorService executors;
  private final DestinationTimeoutMonitor destinationTimeoutMonitor;

  private volatile boolean isReadFromDestRunning;
  private volatile boolean writeToDestFailed;

  private final Stopwatch readFromSourceStopwatch;
  private final Stopwatch processFromSourceStopwatch;
  private final Stopwatch writeToDestStopwatch;
  private final Stopwatch readFromDestStopwatch;
  private final Stopwatch processFromDestStopwatch;
  private final StreamStatusCompletionTracker streamStatusCompletionTracker;

  private static final int sourceMaxBufferSize = 1000;
  private static final int destinationMaxBufferSize = 1000;
  private static final int observabilityMetricsPeriodInSeconds = 1;
  private static final int executorShutdownGracePeriodInSeconds = 10;

  public BufferedReplicationWorker(final String jobId,
                                   final int attempt,
                                   final AirbyteSource source,
                                   final AirbyteDestination destination,
                                   final SyncPersistence syncPersistence,
                                   final RecordSchemaValidator recordSchemaValidator,
                                   final HeartbeatTimeoutChaperone srcHeartbeatTimeoutChaperone,
                                   final ReplicationFeatureFlagReader replicationFeatureFlagReader,
                                   final ReplicationWorkerHelper replicationWorkerHelper,
                                   final DestinationTimeoutMonitor destinationTimeoutMonitor,
                                   final BufferedReplicationWorkerType bufferedReplicationWorkerType,
                                   final StreamStatusCompletionTracker streamStatusCompletionTracker) {
    this(jobId, attempt, source, destination, syncPersistence, recordSchemaValidator, srcHeartbeatTimeoutChaperone, replicationFeatureFlagReader,
        replicationWorkerHelper, destinationTimeoutMonitor, bufferedReplicationWorkerType, OptionalInt.empty(), streamStatusCompletionTracker);
  }

  public BufferedReplicationWorker(final String jobId,
                                   final int attempt,
                                   final AirbyteSource source,
                                   final AirbyteDestination destination,
                                   final SyncPersistence syncPersistence,
                                   final RecordSchemaValidator recordSchemaValidator,
                                   final HeartbeatTimeoutChaperone srcHeartbeatTimeoutChaperone,
                                   final ReplicationFeatureFlagReader replicationFeatureFlagReader,
                                   final ReplicationWorkerHelper replicationWorkerHelper,
                                   final DestinationTimeoutMonitor destinationTimeoutMonitor,
                                   final BufferedReplicationWorkerType bufferedReplicationWorkerType,
                                   final OptionalInt pollTimeOutDurationForQueue,
                                   final StreamStatusCompletionTracker streamStatusCompletionTracker) {
    this.jobId = jobId;
    this.attempt = attempt;
    this.source = source;
    this.destination = destination;
    this.replicationWorkerHelper = replicationWorkerHelper;
    this.destinationTimeoutMonitor = destinationTimeoutMonitor;
    this.replicationFeatureFlagReader = replicationFeatureFlagReader;
    this.recordSchemaValidator = recordSchemaValidator;
    this.syncPersistence = syncPersistence;
    this.srcHeartbeatTimeoutChaperone = srcHeartbeatTimeoutChaperone;
    this.messagesFromSourceQueue =
        bufferedReplicationWorkerType == BufferedReplicationWorkerType.BUFFERED ? new BoundedConcurrentLinkedQueue<>(sourceMaxBufferSize)
            : new ClosableLinkedBlockingQueue<>(sourceMaxBufferSize, pollTimeOutDurationForQueue);
    this.messagesForDestinationQueue =
        bufferedReplicationWorkerType == BufferedReplicationWorkerType.BUFFERED ? new BoundedConcurrentLinkedQueue<>(destinationMaxBufferSize)
            : new ClosableLinkedBlockingQueue<>(destinationMaxBufferSize, pollTimeOutDurationForQueue);
    // readFromSource + processMessage + writeToDestination + readFromDestination +
    // source heartbeat + dest timeout monitor + workload heartbeat = 7 threads
    this.executors = Executors.newFixedThreadPool(7);
    this.isReadFromDestRunning = true;
    this.writeToDestFailed = false;

    this.readFromSourceStopwatch = new Stopwatch();
    this.processFromSourceStopwatch = new Stopwatch();
    this.writeToDestStopwatch = new Stopwatch();
    this.readFromDestStopwatch = new Stopwatch();
    this.processFromDestStopwatch = new Stopwatch();
    this.streamStatusCompletionTracker = streamStatusCompletionTracker;
  }

  @Trace(operationName = WORKER_OPERATION_NAME)
  @Override
  public ReplicationOutput run(final ReplicationInput replicationInput, final Path jobRoot) throws WorkerException {
    final Map<String, String> mdc = MDC.getCopyOfContextMap();
    LOGGER.info("start sync worker. job id: {} attempt id: {}", jobId, attempt);
    LineGobbler.startSection("REPLICATION");

    try {
      final ReplicationContext replicationContext = getReplicationContext(replicationInput);
      final ReplicationFeatureFlags flags = replicationFeatureFlagReader.readReplicationFeatureFlags();
      replicationWorkerHelper.initialize(replicationContext, flags, jobRoot, replicationInput.getCatalog());

      final CloseableWithTimeout destinationWithCloseTimeout = new CloseableWithTimeout(destination, mdc, flags);
      // note: resources are closed in the opposite order in which they are declared. thus source will be
      // closed first (which is what we want).
      try (recordSchemaValidator; syncPersistence; srcHeartbeatTimeoutChaperone; source; destinationTimeoutMonitor; destinationWithCloseTimeout) {
        CompletableFuture.allOf(
            runAsync(() -> replicationWorkerHelper.startDestination(destination, replicationInput, jobRoot), mdc),
            runAsync(() -> replicationWorkerHelper.startSource(source, replicationInput, jobRoot), mdc)).join();

        replicationWorkerHelper.markReplicationRunning();

        if (replicationWorkerHelper.isWorkerV2TestEnabled()) {
          CompletableFuture.runAsync(
              replicationWorkerHelper.getWorkloadStatusHeartbeat(mdc),
              executors);
        }

        CompletableFuture.allOf(
            runAsyncWithHeartbeatCheck(this::readFromSource, mdc),
            runAsync(this::processMessage, mdc),
            flags.isDestinationTimeoutEnabled() ? runAsyncWithTimeout(this::writeToDestination, mdc) : runAsync(this::writeToDestination, mdc),
            runAsync(this::readFromDestination, mdc)).join();

      } catch (final CompletionException e) {

        // Exceptions for each runnable are already handled, those exceptions are coming from the joins and
        // are safe to ignore at this point
        ApmTraceUtils.addExceptionToTrace(e);
      } catch (final Exception e) {
        ApmTraceUtils.addExceptionToTrace(e);
        replicationWorkerHelper.trackFailure(e);
        replicationWorkerHelper.markFailed();
      } finally {
        executors.shutdownNow();

        try {
          // Best effort to mark as complete when the Worker is actually done.
          executors.awaitTermination(executorShutdownGracePeriodInSeconds, TimeUnit.SECONDS);
          if (!executors.isTerminated()) {
            final MetricClient metricClient = MetricClientFactory.getMetricClient();
            metricClient.count(OssMetricsRegistry.REPLICATION_WORKER_EXECUTOR_SHUTDOWN_ERROR, 1,
                new MetricAttribute(MetricTags.IMPLEMENTATION, "buffered"));
          }
        } catch (final InterruptedException e) {
          // Preserve the interrupt status
          Thread.currentThread().interrupt();
        }
      }

      if (!replicationWorkerHelper.getCancelled()) {
        // We don't call endOfReplication on cancelled because it should be called from the cancel method.
        replicationWorkerHelper.endOfReplication();
      }

      final var perfMetrics = new PerformanceMetrics()
          .withAdditionalProperty("readFromSource", readFromSourceStopwatch)
          .withAdditionalProperty("processFromSource", processFromSourceStopwatch)
          .withAdditionalProperty("writeToDest", writeToDestStopwatch)
          .withAdditionalProperty("readFromDest", readFromDestStopwatch)
          .withAdditionalProperty("processFromDest", processFromDestStopwatch);
      return replicationWorkerHelper.getReplicationOutput(perfMetrics);
    } catch (final Exception e) {
      ApmTraceUtils.addExceptionToTrace(e);
      throw new WorkerException("Sync failed", e);
    }

  }

  private CompletableFuture<?> runAsync(final Runnable runnable, final Map<String, String> mdc) {
    return CompletableFuture.runAsync(() -> {
      MDC.setContextMap(mdc);
      runnable.run();
    }, executors).whenComplete(this::trackFailures);
  }

  private CompletableFuture<?> runAsyncWithHeartbeatCheck(final Runnable runnable, final Map<String, String> mdc) {
    final CompletableFuture<Void> runnableFuture = CompletableFuture.runAsync(() -> {
      MDC.setContextMap(mdc);
      runnable.run();
    }, executors);
    final CompletableFuture<Void> heartbeatFuture = CompletableFuture.runAsync(() -> {
      MDC.setContextMap(mdc);
      try {
        srcHeartbeatTimeoutChaperone.runWithHeartbeatThread(runnableFuture);
      } catch (final HeartbeatTimeoutChaperone.HeartbeatTimeoutException e) {
        // TODO need to close to queue for the other threads to stop
        messagesFromSourceQueue.close();
        throw e;
      } catch (final CompletionException | ExecutionException e) {
        // We only want to report source heartbeat failures, the rest is noise
        ApmTraceUtils.addExceptionToTrace(e);
      }
    }, executors);
    return CompletableFuture.allOf(heartbeatFuture, runnableFuture).whenComplete(this::trackFailures);
  }

  private CompletableFuture<?> runAsyncWithTimeout(final Runnable runnable, final Map<String, String> mdc) {
    final CompletableFuture<Void> runnableFuture = CompletableFuture.runAsync(() -> {
      MDC.setContextMap(mdc);
      runnable.run();
    }, executors);
    final CompletableFuture<Void> timeoutFuture = CompletableFuture.runAsync(() -> {
      MDC.setContextMap(mdc);
      try {
        destinationTimeoutMonitor.runWithTimeoutThread(runnableFuture);
      } catch (final DestinationTimeoutMonitor.TimeoutException e) {
        messagesFromSourceQueue.close();
        throw e;
      } catch (final CompletionException | ExecutionException e) {
        ApmTraceUtils.addExceptionToTrace(e);
      }
    }, executors);
    return CompletableFuture.allOf(timeoutFuture, runnableFuture).whenComplete(this::trackFailures);
  }

  @SuppressWarnings("PMD.UnusedFormalParameter")
  private <V> void trackFailures(final V value, final Throwable t) {
    if (t != null) {
      ApmTraceUtils.addExceptionToTrace(t);
      replicationWorkerHelper.trackFailure(t.getCause());
      replicationWorkerHelper.markFailed();
    }
  }

  private ReplicationContext getReplicationContext(final ReplicationInput replicationInput) {

    final UUID sourceDefinitionId = replicationWorkerHelper.getSourceDefinitionIdForSourceId(replicationInput.getSourceId());
    final UUID destinationDefinitionId = replicationWorkerHelper.getDestinationDefinitionIdForDestinationId(replicationInput.getDestinationId());
    return new ReplicationContext(replicationInput.getIsReset(), replicationInput.getConnectionId(), replicationInput.getSourceId(),
        replicationInput.getDestinationId(), Long.parseLong(jobId),
        attempt, replicationInput.getWorkspaceId(), replicationInput.getSourceLauncherConfig().getDockerImage(),
        replicationInput.getDestinationLauncherConfig().getDockerImage(), sourceDefinitionId, destinationDefinitionId);
  }

  @Override
  public void cancel() {
    boolean wasInterrupted = false;

    replicationWorkerHelper.markCancelled();

    LOGGER.info("Cancelling replication worker...");
    executors.shutdownNow();
    try {
      executors.awaitTermination(executorShutdownGracePeriodInSeconds, TimeUnit.SECONDS);
    } catch (final InterruptedException e) {
      wasInterrupted = true;
      ApmTraceUtils.addExceptionToTrace(e);
      LOGGER.error("Unable to cancel due to interruption.", e);
    }

    LOGGER.info("Cancelling destination...");
    try {
      destination.cancel();
    } catch (final Exception e) {
      ApmTraceUtils.addExceptionToTrace(e);
      LOGGER.info("Error cancelling destination: ", e);
    }

    LOGGER.info("Cancelling source...");
    try {
      source.cancel();
    } catch (final Exception e) {
      ApmTraceUtils.addExceptionToTrace(e);
      LOGGER.info("Error cancelling source: ", e);
    }

    replicationWorkerHelper.endOfReplication();

    if (wasInterrupted) {
      // Preserve the interrupt flag if we were interrupted
      Thread.currentThread().interrupt();
    }
  }

  /**
   * Checks if a source is finished while timing how long the check took. This is needed because the
   * current Source implementation will read to determine whether the source is finished. As a result,
   * to track the time spent in a source, we need to track both isFinished and attemptRead.
   */
  private boolean sourceIsFinished() {
    try (final var t = readFromSourceStopwatch.start()) {
      return source.isFinished();
    }
  }

  private void readFromSource() {
    // Capture the result of the last source.isFinished read for reporting.
    // We cannot call isFinished in the finally clause as it may throw an error.
    boolean sourceIsFinished = false;
    try {
      LOGGER.info("readFromSource: start");

      while (!replicationWorkerHelper.getShouldAbort() && !(sourceIsFinished = sourceIsFinished()) && !messagesFromSourceQueue.isClosed()) {
        final Optional<AirbyteMessage> messageOptional = source.attemptRead();
        if (messageOptional.isPresent()) {
          final AirbyteMessage message = messageOptional.get();
          if (message.getType() == Type.TRACE && message.getTrace().getType() == AirbyteTraceMessage.Type.STREAM_STATUS) {
            streamStatusCompletionTracker.track(message.getTrace().getStreamStatus());
          }

          while (!replicationWorkerHelper.getShouldAbort() && !messagesFromSourceQueue.add(message)
              && !messagesFromSourceQueue.isClosed()) {
            Thread.sleep(100);
          }
        }
      }

      if (replicationWorkerHelper.isWorkerV2TestEnabled() && replicationWorkerHelper.getShouldAbort()) {
        source.cancel();
      }

      if (source.getExitValue() == 0) {
        replicationWorkerHelper.endOfSource();
      } else {
        throw new SourceException("Source process exited with non-zero exit code " + source.getExitValue());
      }
    } catch (final SourceException e) {
      LOGGER.info("readFromSource: source exception", e);
      throw e;
    } catch (final InterruptedException e) {
      LOGGER.info("readFromSource: interrupted", e);
      // Getting interrupted during sleep, rethrowing to fail fast
      throw new RuntimeException(e);
    } catch (final Exception e) {
      LOGGER.info("readFromSource: exception caught", e);
      throw new SourceException("Source process read attempt failed", e);
    } finally {
      LOGGER.info("readFromSource: done. (source.isFinished:{}, fromSource.isClosed:{})", sourceIsFinished, messagesFromSourceQueue.isClosed());
      messagesFromSourceQueue.close();
    }

  }

  private void processMessage() {
    try {
      LOGGER.info("processMessage: start");

      while (!replicationWorkerHelper.getShouldAbort() && !messagesFromSourceQueue.isDone() && !messagesForDestinationQueue.isClosed()) {
        final AirbyteMessage message;
        message = messagesFromSourceQueue.poll();
        if (message == null) {
          continue;
        }

        final Optional<AirbyteMessage> processedMessageOpt;
        try (final var t = processFromSourceStopwatch.start()) {
          processedMessageOpt = replicationWorkerHelper.processMessageFromSource(message);
        }
        if (processedMessageOpt.isPresent()) {
          final AirbyteMessage m = processedMessageOpt.get();
          // TODO this check should move to the processMessageFromSource
          if (m.getType() == Type.RECORD || m.getType() == Type.STATE) {
            while (!messagesForDestinationQueue.add(m) && !messagesForDestinationQueue.isClosed()) {
              Thread.sleep(100);
            }
          }
        }
      }

    } catch (final InterruptedException e) {
      // Getting interrupted during sleep, rethrowing to fail fast
      LOGGER.info("processMessage: interrupted", e);
      throw new RuntimeException(e);
    } catch (final Exception e) {
      LOGGER.info("processMessage: exception caught", e);
      throw e;
    } finally {
      LOGGER.info("processMessage: done. (fromSource.isDone:{}, forDest.isClosed:{})",
          messagesFromSourceQueue.isDone(), messagesForDestinationQueue.isClosed());
      messagesFromSourceQueue.close();
      messagesForDestinationQueue.close();
    }
  }

  private void writeToDestination() {
    try {
      LOGGER.info("writeToDestination: start");
      try {
        while (!replicationWorkerHelper.getShouldAbort() && !messagesForDestinationQueue.isDone() && isReadFromDestRunning) {
          final AirbyteMessage message;
          message = messagesForDestinationQueue.poll();
          if (message == null) {
            continue;
          }

          try (final var t = writeToDestStopwatch.start()) {
            destination.accept(message);
          }
        }

        final List<AirbyteMessage> statusMessageToSend = replicationWorkerHelper.getStreamStatusToSend(source.getExitValue());
        for (AirbyteMessage airbyteMessage : statusMessageToSend) {
          destination.accept(airbyteMessage);
        }
      } finally {
        destination.notifyEndOfInput();
      }

    } catch (final Exception e) {
      writeToDestFailed = true;
      LOGGER.info("writeToDestination: exception caught", e);
      throw new DestinationException("Destination process message delivery failed", e);
    } finally {
      LOGGER.info("writeToDestination: done. (forDest.isDone:{}, isDestRunning:{})", messagesForDestinationQueue.isDone(), isReadFromDestRunning);
      messagesForDestinationQueue.close();
    }
  }

  private void readFromDestination() {
    // Capture the result of the last destination.isFinished read for reporting.
    // We cannot call isFinished in the finally clause as it may throw an error.
    boolean destinationIsFinished = false;

    LOGGER.info("readFromDestination: start");
    try {
      while (!replicationWorkerHelper.getShouldAbort() && !writeToDestFailed && !(destinationIsFinished = destinationIsFinished())) {
        final Optional<AirbyteMessage> messageOptional;
        try (final var t = readFromDestStopwatch.start()) {
          messageOptional = destination.attemptRead();
        } catch (final Exception e) {
          throw new DestinationException("Destination process read attempt failed", e);
        }
        if (messageOptional.isPresent()) {
          try (final var t = processFromDestStopwatch.start()) {
            replicationWorkerHelper.processMessageFromDestination(messageOptional.get());
          }
        }
      }
      if (destination.getExitValue() != 0) {
        throw new DestinationException("Destination process exited with non-zero exit code " + destination.getExitValue());
      } else {
        replicationWorkerHelper.endOfDestination();
      }

    } catch (final Exception e) {
      LOGGER.info("readFromDestination: exception caught", e);
      throw e;
    } finally {
      LOGGER.info("readFromDestination: done. (writeToDestFailed:{}, dest.isFinished:{})", writeToDestFailed, destinationIsFinished);
      isReadFromDestRunning = false;
    }
  }

  /**
   * Checks if a destination is finished while timing how long the check took. This is needed because
   * the current Destination implementation will read to determine whether the destination is
   * finished. As a result, to track the time spent in a destination, we need to track both isFinished
   * and attemptRead.
   */
  private boolean destinationIsFinished() {
    try (final var t = readFromDestStopwatch.start()) {
      return destination.isFinished();
    }
  }

  private class CloseableWithTimeout implements AutoCloseable {

    AutoCloseable autoCloseable;
    private final Map<String, String> mdc;
    private final ReplicationFeatureFlags flags;

    public CloseableWithTimeout(final AutoCloseable autoCloseable, final Map<String, String> mdc, final ReplicationFeatureFlags flags) {
      this.autoCloseable = autoCloseable;
      this.mdc = mdc;
      this.flags = flags;
    }

    @Override
    public void close() throws Exception {
      if (flags.isDestinationTimeoutEnabled()) {
        runAsyncWithTimeout(() -> {
          try {
            autoCloseable.close();
          } catch (final Exception e) {
            throw new RuntimeException(e);
          }
        }, mdc).join();
      } else {
        autoCloseable.close();
      }
    }

  }

}
