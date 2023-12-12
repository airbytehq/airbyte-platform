/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.general;

import static io.airbyte.metrics.lib.ApmTraceConstants.WORKER_OPERATION_NAME;

import datadog.trace.api.Trace;
import io.airbyte.commons.io.LineGobbler;
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
import io.airbyte.workers.RecordSchemaValidator;
import io.airbyte.workers.context.ReplicationContext;
import io.airbyte.workers.context.ReplicationFeatureFlags;
import io.airbyte.workers.exception.WorkerException;
import io.airbyte.workers.internal.AirbyteDestination;
import io.airbyte.workers.internal.AirbyteSource;
import io.airbyte.workers.internal.DestinationTimeoutMonitor;
import io.airbyte.workers.internal.HeartbeatTimeoutChaperone;
import io.airbyte.workers.internal.exception.DestinationException;
import io.airbyte.workers.internal.exception.SourceException;
import io.airbyte.workers.internal.syncpersistence.SyncPersistence;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

/**
 * This worker is the "data shovel" of ETL. It is responsible for moving data from the Source
 * container to the Destination container. It manages the full lifecycle of this process. This
 * includes:
 * <ul>
 * <li>Starting the Source and Destination containers</li>
 * <li>Passing data from Source to Destination</li>
 * <li>Executing any configured map-only operations (Mappers) in between the Source and
 * Destination</li>
 * <li>Collecting metadata about the data that is passing from Source to Destination</li>
 * <li>Listening for state messages emitted from the Destination to keep track of what data has been
 * replicated.</li>
 * <li>Handling shutdown of the Source and Destination</li>
 * <li>Handling failure cases and returning state for partially completed replications (so that the
 * next replication can pick up where it left off instead of starting from the beginning)</li>
 * </ul>
 */
@SuppressWarnings("PMD.AvoidPrintStackTrace")
public class DefaultReplicationWorker implements ReplicationWorker {

  private static final Logger LOGGER = LoggerFactory.getLogger(DefaultReplicationWorker.class);

  private final String jobId;
  private final int attempt;
  private final DestinationTimeoutMonitor destinationTimeoutMonitor;
  private final ReplicationWorkerHelper replicationWorkerHelper;
  private final AirbyteSource source;
  private final AirbyteDestination destination;
  private final SyncPersistence syncPersistence;
  private final ExecutorService executors;
  private final AtomicBoolean hasFailed;
  private final RecordSchemaValidator recordSchemaValidator;
  private final HeartbeatTimeoutChaperone srcHeartbeatTimeoutChaperone;
  private final ReplicationFeatureFlagReader replicationFeatureFlagReader;

  private static final int executorShutdownGracePeriodInSeconds = 10;

  public DefaultReplicationWorker(final String jobId,
                                  final int attempt,
                                  final AirbyteSource source,
                                  final AirbyteDestination destination,
                                  final SyncPersistence syncPersistence,
                                  final RecordSchemaValidator recordSchemaValidator,
                                  final HeartbeatTimeoutChaperone srcHeartbeatTimeoutChaperone,
                                  final ReplicationFeatureFlagReader replicationFeatureFlagReader,
                                  final ReplicationWorkerHelper replicationWorkerHelper,
                                  final DestinationTimeoutMonitor destinationTimeoutMonitor) {
    this.jobId = jobId;
    this.attempt = attempt;
    this.destinationTimeoutMonitor = destinationTimeoutMonitor;
    this.replicationWorkerHelper = replicationWorkerHelper;
    this.source = source;
    this.destination = destination;
    this.syncPersistence = syncPersistence;
    // readFromSrcAndWriteToDstRunnable + readFromDstThread
    // + source heartbeat + dest timeout monitor + workload timeout = 5
    this.executors = Executors.newFixedThreadPool(5);
    this.recordSchemaValidator = recordSchemaValidator;
    this.srcHeartbeatTimeoutChaperone = srcHeartbeatTimeoutChaperone;
    this.replicationFeatureFlagReader = replicationFeatureFlagReader;

    this.hasFailed = new AtomicBoolean(false);
  }

  /**
   * Run executes two threads. The first pipes data from STDOUT of the source to STDIN of the
   * destination. The second listen on STDOUT of the destination. The goal of this second thread is to
   * detect when the destination emits state messages. Only state messages emitted by the destination
   * should be treated as state that is safe to return from run. In the case when the destination
   * emits no state, we fall back on whatever state is pass in as an argument to this method.
   *
   * @param replicationInput all configuration for running replication
   * @param jobRoot file root that worker is allowed to use
   * @return output of the replication attempt (including state)
   * @throws WorkerException exception from worker
   */
  @Trace(operationName = WORKER_OPERATION_NAME)
  @Override
  public final ReplicationOutput run(final ReplicationInput replicationInput, final Path jobRoot) throws WorkerException {
    LOGGER.info("start sync worker. job id: {} attempt id: {}", jobId, attempt);

    LineGobbler.startSection("REPLICATION");

    try {
      LOGGER.info("configured sync modes: {}", replicationInput.getCatalog().getStreams()
          .stream()
          .collect(Collectors.toMap(s -> s.getStream().getNamespace() + "." + s.getStream().getName(),
              s -> String.format("%s - %s", s.getSyncMode(), s.getDestinationSyncMode()))));

      final ReplicationContext replicationContext =
          new ReplicationContext(replicationInput.getIsReset(), replicationInput.getConnectionId(), replicationInput.getSourceId(),
              replicationInput.getDestinationId(), Long.parseLong(jobId),
              attempt, replicationInput.getWorkspaceId());

      final ReplicationFeatureFlags flags = replicationFeatureFlagReader.readReplicationFeatureFlags();
      replicationWorkerHelper.initialize(replicationContext, flags, jobRoot);

      replicate(jobRoot, replicationInput, flags);

      return replicationWorkerHelper.getReplicationOutput();
    } catch (final Exception e) {
      ApmTraceUtils.addExceptionToTrace(e);
      throw new WorkerException("Sync failed", e);
    }

  }

  private void replicate(final Path jobRoot,
                         final ReplicationInput replicationInput,
                         final ReplicationFeatureFlags flags) {
    final Map<String, String> mdc = MDC.getCopyOfContextMap();

    final CloseableWithTimeout destinationWithCloseTimeout = new CloseableWithTimeout(destination, mdc, flags);
    // note: resources are closed in the opposite order in which they are declared. thus source will be
    // closed first (which is what we want).
    try (recordSchemaValidator; syncPersistence; srcHeartbeatTimeoutChaperone; source; destinationTimeoutMonitor; destinationWithCloseTimeout) {
      replicationWorkerHelper.startDestination(destination, replicationInput, jobRoot);
      replicationWorkerHelper.startSource(source, replicationInput, jobRoot);

      replicationWorkerHelper.markReplicationRunning();

      if (replicationWorkerHelper.isWorkerV2TestEnabled()) {
        CompletableFuture.runAsync(
            replicationWorkerHelper.getWorkloadStatusHeartbeat(),
            executors);
      }

      // note: `whenComplete` is used instead of `exceptionally` so that the original exception is still
      // thrown
      final CompletableFuture<?> readFromDstThread = CompletableFuture.runAsync(
          readFromDstRunnable(destination, replicationWorkerHelper, mdc),
          executors)
          .whenComplete((msg, ex) -> {
            if (ex != null) {
              ApmTraceUtils.addExceptionToTrace(ex);
              replicationWorkerHelper.trackFailure(ex.getCause());
            }
          });

      final CompletableFuture<Void> readSrcAndWriteDstThread = CompletableFuture.runAsync(readFromSrcAndWriteToDstRunnable(
          source,
          destination,
          replicationWorkerHelper,
          mdc), executors)
          .whenComplete((msg, ex) -> {
            if (ex != null) {
              ApmTraceUtils.addExceptionToTrace(ex);
              replicationWorkerHelper.trackFailure(ex.getCause());
            }
          });

      if (flags.isDestinationTimeoutEnabled()) {
        attachHeartbeatCheck(readSrcAndWriteDstThread, srcHeartbeatTimeoutChaperone, mdc);
        attachDestinationTimeout(readSrcAndWriteDstThread, mdc);
      } else {
        try {
          srcHeartbeatTimeoutChaperone.runWithHeartbeatThread(readSrcAndWriteDstThread);
        } catch (final HeartbeatTimeoutChaperone.HeartbeatTimeoutException ex) {
          ApmTraceUtils.addExceptionToTrace(ex);
          replicationWorkerHelper.trackFailure(ex);
        }
      }

      LOGGER.info("Waiting for source and destination threads to complete.");
      // CompletableFuture#allOf waits until all futures finish before returning, even if one throws an
      // exception. So in order to handle exceptions from a future immediately without needing to wait for
      // the other future to finish, we first call CompletableFuture#anyOf.
      CompletableFuture.anyOf(readSrcAndWriteDstThread, readFromDstThread).get();
      LOGGER.info("One of source or destination thread complete. Waiting on the other.");
      CompletableFuture.allOf(readSrcAndWriteDstThread, readFromDstThread).get();
      LOGGER.info("Source and destination threads complete.");

      if (!replicationWorkerHelper.getCancelled()) {
        // We don't call endOfReplication on cancelled because it should be called from the cancel method.
        replicationWorkerHelper.endOfReplication();
      }
    } catch (final Exception e) {
      hasFailed.set(true);
      replicationWorkerHelper.markFailed();
      ApmTraceUtils.addExceptionToTrace(e);
      LOGGER.error("Sync worker failed.", e);
    } finally {
      executors.shutdownNow();

      try {
        // Best effort to mark as complete when the Worker is actually done.
        if (!executors.awaitTermination(executorShutdownGracePeriodInSeconds, TimeUnit.SECONDS)) {
          final MetricClient metricClient = MetricClientFactory.getMetricClient();
          metricClient.count(OssMetricsRegistry.REPLICATION_WORKER_EXECUTOR_SHUTDOWN_ERROR, 1,
              new MetricAttribute(MetricTags.IMPLEMENTATION, "default"));
        }
      } catch (final InterruptedException e) {
        // Preserve the interrupt status
        Thread.currentThread().interrupt();
      }
    }
  }

  private void attachHeartbeatCheck(
                                    final CompletableFuture<Void> completableFuture,
                                    final HeartbeatTimeoutChaperone heartbeatTimeoutChaperone,
                                    final Map<String, String> mdc) {
    // new thread since heartbeatTimeoutChaperone.runWithHeartbeatThread is blocking
    CompletableFuture.runAsync(() -> {
      MDC.setContextMap(mdc);
      try {
        heartbeatTimeoutChaperone.runWithHeartbeatThread(completableFuture);
      } catch (final HeartbeatTimeoutChaperone.HeartbeatTimeoutException e) {
        ApmTraceUtils.addExceptionToTrace(e);
        replicationWorkerHelper.trackFailure(e);
      } catch (final ExecutionException e) {
        throw new RuntimeException(e);
      }
    }, executors);
  }

  private void attachDestinationTimeout(final CompletableFuture<Void> completableFuture, final Map<String, String> mdc) {
    CompletableFuture.runAsync(() -> {
      MDC.setContextMap(mdc);
      try {
        destinationTimeoutMonitor.runWithTimeoutThread(completableFuture);
      } catch (final DestinationTimeoutMonitor.TimeoutException e) {
        ApmTraceUtils.addExceptionToTrace(e);
        replicationWorkerHelper.trackFailure(e);
      } catch (final ExecutionException e) {
        throw new RuntimeException(e);
      }
    }, executors);
  }

  private CompletableFuture<?> runAsyncWithTimeout(final Runnable runnable, final Map<String, String> mdc) {
    final CompletableFuture<Void> runnableFuture = CompletableFuture.runAsync(() -> {
      MDC.setContextMap(mdc);
      runnable.run();
    }, executors);

    attachDestinationTimeout(runnableFuture, mdc);

    return runnableFuture;
  }

  @SuppressWarnings("PMD.AvoidInstanceofChecksInCatchClause")
  private static Runnable readFromDstRunnable(final AirbyteDestination destination,
                                              final ReplicationWorkerHelper replicationWorkerHelper,
                                              final Map<String, String> mdc) {
    return () -> {
      MDC.setContextMap(mdc);
      LOGGER.info("Destination output thread started.");
      try {
        while (!replicationWorkerHelper.getShouldAbort() && !destination.isFinished()) {
          final Optional<AirbyteMessage> messageOptional;
          try {
            messageOptional = destination.attemptRead();
          } catch (final Exception e) {
            throw new DestinationException("Destination process read attempt failed", e);
          }
          if (messageOptional.isPresent()) {
            replicationWorkerHelper.processMessageFromDestination(messageOptional.get());
          }
        }
        if (!replicationWorkerHelper.getShouldAbort() && destination.getExitValue() != 0) {
          throw new DestinationException("Destination process exited with non-zero exit code " + destination.getExitValue());
        } else {
          replicationWorkerHelper.endOfDestination();
        }
      } catch (final Exception e) {
        if (!replicationWorkerHelper.getCancelled()) {
          // Although this thread is closed first, it races with the destination's closure and can attempt one
          // final read after the destination is closed before it's terminated.
          // This read will fail and throw an exception. Because of this, throw exceptions only if the worker
          // was not cancelled.

          if (e instanceof DestinationException) {
            // Surface Destination exceptions directly so that they can be classified properly by the worker
            throw e;
          } else {
            throw new RuntimeException(e);
          }
        }
      }
    };
  }

  @SuppressWarnings("PMD.AvoidInstanceofChecksInCatchClause")
  private static Runnable readFromSrcAndWriteToDstRunnable(final AirbyteSource source,
                                                           final AirbyteDestination destination,
                                                           final ReplicationWorkerHelper replicationWorkerHelper,
                                                           final Map<String, String> mdc) {
    return () -> {
      MDC.setContextMap(mdc);
      LOGGER.info("Replication thread started.");

      try {
        while (!replicationWorkerHelper.getShouldAbort() && !source.isFinished()) {
          final Optional<AirbyteMessage> messageOptional;
          try {
            messageOptional = source.attemptRead();
          } catch (final Exception e) {
            throw new SourceException("Source process read attempt failed", e);
          }

          if (messageOptional.isPresent()) {
            final AirbyteMessage airbyteMessage = messageOptional.get();
            final Optional<AirbyteMessage> processedAirbyteMessage =
                replicationWorkerHelper.processMessageFromSource(airbyteMessage);

            if (processedAirbyteMessage.isPresent()) {
              final AirbyteMessage message = processedAirbyteMessage.get();
              try {
                if (message.getType() == Type.RECORD || message.getType() == Type.STATE) {
                  destination.accept(message);
                }
              } catch (final Exception e) {
                throw new DestinationException("Destination process message delivery failed", e);
              }
            }
          } else {
            LOGGER.info("Source has no more messages, closing connection.");
            try {
              source.close();
            } catch (final Exception e) {
              throw new SourceException("Source didn't exit properly - check the logs!", e);
            }
          }
        }
        if (replicationWorkerHelper.isWorkerV2TestEnabled() && replicationWorkerHelper.getShouldAbort()) {
          source.cancel();
        }
        replicationWorkerHelper.endOfSource();

        try {
          destination.notifyEndOfInput();
        } catch (final Exception e) {
          throw new DestinationException("Destination process end of stream notification failed", e);
        }
        if (!replicationWorkerHelper.getShouldAbort() && source.getExitValue() != 0) {
          throw new SourceException("Source process exited with non-zero exit code " + source.getExitValue());
        }
      } catch (final Exception e) {
        // If we exit this function with an exception, we should assume failure and stop the other thread.
        replicationWorkerHelper.abort();

        if (!replicationWorkerHelper.getCancelled()) {
          // Although this thread is closed first, it races with the source's closure and can attempt one
          // final read after the source is closed before it's terminated.
          // This read will fail and throw an exception. Because of this, throw exceptions only if the worker
          // was not cancelled.

          if (e instanceof SourceException) {
            // Surface Source and Destination exceptions directly so that they can be classified properly by the
            // worker
            throw (SourceException) e;
          } else if (e instanceof DestinationException) {
            // Surface Source and Destination exceptions directly so that they can be classified properly by the
            // worker
            throw (DestinationException) e;
          } else {
            throw new RuntimeException(e);
          }
        }
      }
    };
  }

  @Override
  public void cancel() {
    boolean wasInterrupted = false;

    // Resources are closed in the opposite order they are declared.
    LOGGER.info("Cancelling replication worker...");
    executors.shutdownNow();
    try {
      executors.awaitTermination(executorShutdownGracePeriodInSeconds, TimeUnit.SECONDS);
    } catch (final InterruptedException e) {
      ApmTraceUtils.addExceptionToTrace(e);
      LOGGER.error("Unable to cancel due to interruption.", e);
      wasInterrupted = true;
    }
    replicationWorkerHelper.markCancelled();

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
