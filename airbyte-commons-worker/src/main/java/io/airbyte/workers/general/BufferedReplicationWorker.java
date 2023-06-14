/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.general;

import io.airbyte.commons.concurrency.BoundedConcurrentLinkedQueue;
import io.airbyte.commons.converters.ConnectorConfigUpdater;
import io.airbyte.commons.converters.ThreadedTimeTracker;
import io.airbyte.commons.io.LineGobbler;
import io.airbyte.config.ReplicationOutput;
import io.airbyte.config.StandardSyncInput;
import io.airbyte.metrics.lib.ApmTraceUtils;
import io.airbyte.metrics.lib.MetricClient;
import io.airbyte.metrics.lib.MetricClientFactory;
import io.airbyte.metrics.lib.OssMetricsRegistry;
import io.airbyte.protocol.models.AirbyteMessage;
import io.airbyte.protocol.models.AirbyteMessage.Type;
import io.airbyte.workers.RecordSchemaValidator;
import io.airbyte.workers.context.ReplicationContext;
import io.airbyte.workers.context.ReplicationFeatureFlags;
import io.airbyte.workers.exception.WorkerException;
import io.airbyte.workers.helper.AirbyteMessageDataExtractor;
import io.airbyte.workers.internal.AirbyteDestination;
import io.airbyte.workers.internal.AirbyteMapper;
import io.airbyte.workers.internal.AirbyteSource;
import io.airbyte.workers.internal.FieldSelector;
import io.airbyte.workers.internal.HeartbeatTimeoutChaperone;
import io.airbyte.workers.internal.book_keeping.MessageTracker;
import io.airbyte.workers.internal.book_keeping.events.ReplicationAirbyteMessageEventPublishingHelper;
import io.airbyte.workers.internal.exception.DestinationException;
import io.airbyte.workers.internal.exception.SourceException;
import io.airbyte.workers.internal.sync_persistence.SyncPersistence;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
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
  private final BoundedConcurrentLinkedQueue<AirbyteMessage> messagesFromSourceQueue;
  private final BoundedConcurrentLinkedQueue<AirbyteMessage> messagesForDestinationQueue;
  private final ExecutorService executors;
  private final ScheduledExecutorService scheduledExecutors;

  private final AtomicLong destMessagesRead;
  private final AtomicLong destMessagesSent;
  private final AtomicLong sourceMessagesRead;

  private volatile boolean isReadFromDestRunning;
  private volatile boolean cancelled;

  private static final int sourceMaxBufferSize = 1000;
  private static final int destinationMaxBufferSize = 1000;
  private static final int observabilityMetricsPeriodInSeconds = 1;

  public BufferedReplicationWorker(final String jobId,
                                   final int attempt,
                                   final AirbyteSource source,
                                   final AirbyteMapper mapper,
                                   final AirbyteDestination destination,
                                   final MessageTracker messageTracker,
                                   final SyncPersistence syncPersistence,
                                   final RecordSchemaValidator recordSchemaValidator,
                                   final FieldSelector fieldSelector,
                                   final ConnectorConfigUpdater connectorConfigUpdater,
                                   final HeartbeatTimeoutChaperone srcHeartbeatTimeoutChaperone,
                                   final ReplicationFeatureFlagReader replicationFeatureFlagReader,
                                   final AirbyteMessageDataExtractor airbyteMessageDataExtractor,
                                   final ReplicationAirbyteMessageEventPublishingHelper replicationAirbyteMessageEventPublishingHelper) {
    this.jobId = jobId;
    this.attempt = attempt;
    this.source = source;
    this.destination = destination;
    this.replicationWorkerHelper = new ReplicationWorkerHelper(airbyteMessageDataExtractor, fieldSelector, mapper, messageTracker, syncPersistence,
        connectorConfigUpdater, replicationAirbyteMessageEventPublishingHelper, new ThreadedTimeTracker());
    this.replicationFeatureFlagReader = replicationFeatureFlagReader;
    this.recordSchemaValidator = recordSchemaValidator;
    this.syncPersistence = syncPersistence;
    this.srcHeartbeatTimeoutChaperone = srcHeartbeatTimeoutChaperone;
    this.messagesFromSourceQueue = new BoundedConcurrentLinkedQueue<>(sourceMaxBufferSize);
    this.messagesForDestinationQueue = new BoundedConcurrentLinkedQueue<>(destinationMaxBufferSize);
    this.executors = Executors.newFixedThreadPool(4);
    this.scheduledExecutors = Executors.newSingleThreadScheduledExecutor();
    this.isReadFromDestRunning = true;
    this.cancelled = false;

    this.destMessagesRead = new AtomicLong();
    this.destMessagesSent = new AtomicLong();
    this.sourceMessagesRead = new AtomicLong();
  }

  @Override
  public ReplicationOutput run(final StandardSyncInput syncInput, final Path jobRoot) throws WorkerException {
    final Map<String, String> mdc = MDC.getCopyOfContextMap();
    LOGGER.info("start sync worker. job id: {} attempt id: {}", jobId, attempt);
    LineGobbler.startSection("REPLICATION");

    try {
      final ReplicationContext replicationContext = getReplicationContext(syncInput);
      final ReplicationFeatureFlags flags = replicationFeatureFlagReader.readReplicationFeatureFlags(replicationContext, syncInput);
      replicationWorkerHelper.initialize(replicationContext, flags);

      // note: resources are closed in the opposite order in which they are declared. thus source will be
      // closed first (which is what we want).
      try (recordSchemaValidator; syncPersistence; srcHeartbeatTimeoutChaperone; destination; source) {
        scheduledExecutors.scheduleAtFixedRate(this::reportObservabilityMetrics, 0, observabilityMetricsPeriodInSeconds, TimeUnit.SECONDS);

        CompletableFuture.allOf(
            runAsync(() -> replicationWorkerHelper.startDestination(destination, syncInput, jobRoot), mdc),
            runAsync(() -> replicationWorkerHelper.startSource(source, syncInput, jobRoot), mdc)).join();

        CompletableFuture.allOf(
            runAsyncWithHeartbeatCheck(this::readFromSource, mdc),
            runAsync(this::processMessage, mdc),
            runAsync(this::writeToDestination, mdc),
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
        scheduledExecutors.shutdownNow();
      }

      replicationWorkerHelper.endOfReplication();
      return replicationWorkerHelper.getReplicationOutput();
    } catch (final Exception e) {
      ApmTraceUtils.addExceptionToTrace(e);
      throw new WorkerException("Sync failed", e);
    }

  }

  private void reportObservabilityMetrics() {
    final MetricClient metricClient = MetricClientFactory.getMetricClient();
    metricClient.gauge(OssMetricsRegistry.WORKER_DESTINATION_BUFFER_SIZE, messagesForDestinationQueue.size());
    metricClient.gauge(OssMetricsRegistry.WORKER_SOURCE_BUFFER_SIZE, messagesFromSourceQueue.size());
    metricClient.count(OssMetricsRegistry.WORKER_DESTINATION_MESSAGE_READ, destMessagesRead.getAndSet(0));
    metricClient.count(OssMetricsRegistry.WORKER_DESTINATION_MESSAGE_SENT, destMessagesSent.getAndSet(0));
    metricClient.count(OssMetricsRegistry.WORKER_SOURCE_MESSAGE_READ, sourceMessagesRead.getAndSet(0));
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
    });
    return CompletableFuture.allOf(heartbeatFuture, runnableFuture).whenComplete(this::trackFailures);
  }

  @SuppressWarnings("PMD.UnusedFormalParameter")
  private <V> void trackFailures(final V value, final Throwable t) {
    if (t != null) {
      ApmTraceUtils.addExceptionToTrace(t);
      replicationWorkerHelper.trackFailure(t.getCause());
      replicationWorkerHelper.markFailed();
    }
  }

  private ReplicationContext getReplicationContext(final StandardSyncInput syncInput) {
    return new ReplicationContext(syncInput.getIsReset(), syncInput.getConnectionId(), syncInput.getSourceId(),
        syncInput.getDestinationId(), Long.parseLong(jobId),
        attempt, syncInput.getWorkspaceId());
  }

  @Override
  public void cancel() {
    cancelled = true;
    replicationWorkerHelper.markCancelled();

    LOGGER.info("Cancelling replication worker...");
    try {
      executors.awaitTermination(10, TimeUnit.SECONDS);
    } catch (final InterruptedException e) {
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
  }

  private void readFromSource() {
    // Capture the result of the last source.isFinished read for reporting.
    // We cannot call isFinished in the finally clause as it may throw an error.
    boolean sourceIsFinished = false;
    try {
      LOGGER.info("readFromSource: start");

      while (!cancelled && !(sourceIsFinished = source.isFinished()) && !messagesFromSourceQueue.isClosed()) {
        final Optional<AirbyteMessage> messageOptional = source.attemptRead();
        if (messageOptional.isPresent()) {
          sourceMessagesRead.incrementAndGet();
          while (!messagesFromSourceQueue.add(messageOptional.get()) && !messagesFromSourceQueue.isClosed()) {
            Thread.sleep(100);
          }
        }
      }

      if (source.getExitValue() == 0) {
        replicationWorkerHelper.endOfSource();
      } else {
        throw new SourceException("Source process exited with non-zero exit code " + source.getExitValue());
      }
    } catch (final SourceException e) {
      LOGGER.info("readFromSource: source exception", e);
      throw e;
    } catch (InterruptedException e) {
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

      while (!messagesFromSourceQueue.isDone() && !messagesForDestinationQueue.isClosed()) {
        final AirbyteMessage message;
        message = messagesFromSourceQueue.poll();
        if (message == null) {
          continue;
        }

        final Optional<AirbyteMessage> processedMessageOpt = replicationWorkerHelper.processMessageFromSource(message);
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

    } catch (InterruptedException e) {
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
        while (!messagesForDestinationQueue.isDone() && isReadFromDestRunning) {
          final AirbyteMessage message;
          message = messagesForDestinationQueue.poll();
          if (message == null) {
            continue;
          }

          destination.accept(message);
          destMessagesSent.incrementAndGet();
        }
      } finally {
        destination.notifyEndOfInput();
      }

    } catch (final Exception e) {
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
      while (!(destinationIsFinished = destination.isFinished())) {
        final Optional<AirbyteMessage> messageOptional;
        try {
          messageOptional = destination.attemptRead();
        } catch (final Exception e) {
          throw new DestinationException("Destination process read attempt failed", e);
        }
        if (messageOptional.isPresent()) {
          destMessagesRead.incrementAndGet();
          replicationWorkerHelper.processMessageFromDestination(messageOptional.get());
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
      LOGGER.info("readFromDestination: done. (dest.isFinished:{})", destinationIsFinished);
      isReadFromDestRunning = false;
    }
  }

}
