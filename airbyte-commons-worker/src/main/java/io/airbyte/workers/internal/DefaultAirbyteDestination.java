/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.internal;

import static io.airbyte.metrics.lib.OssMetricsRegistry.WORKER_DESTINATION_ACCEPT_ELAPSED_MILLISECS;
import static io.airbyte.metrics.lib.OssMetricsRegistry.WORKER_DESTINATION_NOTIFY_END_OF_INPUT_ELAPSED_MILLISECS;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import io.airbyte.commons.constants.WorkerConstants;
import io.airbyte.commons.io.IOs;
import io.airbyte.commons.io.LineGobbler;
import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.logging.LoggingHelper.Color;
import io.airbyte.commons.logging.MdcScope;
import io.airbyte.commons.logging.MdcScope.Builder;
import io.airbyte.commons.protocol.DefaultProtocolSerializer;
import io.airbyte.commons.protocol.ProtocolSerializer;
import io.airbyte.config.WorkerDestinationConfig;
import io.airbyte.metrics.lib.MetricAttribute;
import io.airbyte.metrics.lib.MetricClient;
import io.airbyte.metrics.lib.MetricsRegistry;
import io.airbyte.protocol.models.AirbyteMessage;
import io.airbyte.protocol.models.AirbyteMessage.Type;
import io.airbyte.workers.WorkerUtils;
import io.airbyte.workers.exception.WorkerException;
import io.airbyte.workers.process.IntegrationLauncher;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default implementation of {@link AirbyteDestination}.
 */
public class DefaultAirbyteDestination implements AirbyteDestination {

  private static final Logger LOGGER = LoggerFactory.getLogger(DefaultAirbyteDestination.class);
  public static final MdcScope.Builder CONTAINER_LOG_MDC_BUILDER = new Builder()
      .setLogPrefix("destination")
      .setPrefixColor(Color.YELLOW_BACKGROUND);
  static final Set<Integer> IGNORED_EXIT_CODES = Set.of(
      0, // Normal exit
      143 // SIGTERM
  );

  private static final int EXECUTOR_SHUTDOWN_GRACE_PERIOD_SECONDS = 10;
  public static final int EMIT_ELAPSED_TIME_METRIC_INTERVAL_SECONDS = 10 * 60;

  private final IntegrationLauncher integrationLauncher;
  private final AirbyteStreamFactory streamFactory;
  private final AirbyteMessageBufferedWriterFactory messageWriterFactory;
  private final ProtocolSerializer protocolSerializer;
  private final boolean elapsedTimeTrackingEnabled;
  private final ScheduledExecutorService scheduledExecutor;
  private final MetricClient metricClient;
  private final MetricAttribute[] metricAttrs;
  private final int emitElapsedTimeMetricIntervalSeconds;

  private final AtomicBoolean inputHasEnded = new AtomicBoolean(false);

  private Process destinationProcess = null;
  private AirbyteMessageBufferedWriter writer = null;
  private Iterator<AirbyteMessage> messageIterator = null;
  private Integer exitValue = null;

  @VisibleForTesting
  public DefaultAirbyteDestination(final IntegrationLauncher integrationLauncher) {
    this(integrationLauncher,
        VersionedAirbyteStreamFactory.noMigrationVersionedAirbyteStreamFactory(LOGGER, CONTAINER_LOG_MDC_BUILDER, Optional.empty(),
            Runtime.getRuntime().maxMemory(), false),
        new DefaultAirbyteMessageBufferedWriterFactory(),
        new DefaultProtocolSerializer(), false, null, null);
  }

  @VisibleForTesting
  DefaultAirbyteDestination(final IntegrationLauncher integrationLauncher,
                            final AirbyteStreamFactory streamFactory,
                            final AirbyteMessageBufferedWriterFactory messageWriterFactory,
                            final ProtocolSerializer protocolSerializer,
                            final boolean elapsedTimeTrackingEnabled,
                            final MetricClient metricClient,
                            final MetricAttribute[] metricAttrs,
                            final int emitElapsedTimeMetricIntervalSeconds) {
    this.integrationLauncher = integrationLauncher;
    this.streamFactory = streamFactory;
    this.messageWriterFactory = messageWriterFactory;
    this.protocolSerializer = protocolSerializer;
    this.elapsedTimeTrackingEnabled = elapsedTimeTrackingEnabled;
    this.metricClient = metricClient;
    this.metricAttrs = metricAttrs;
    this.emitElapsedTimeMetricIntervalSeconds = emitElapsedTimeMetricIntervalSeconds;
    this.scheduledExecutor = elapsedTimeTrackingEnabled ? Executors.newSingleThreadScheduledExecutor() : null;
  }

  public DefaultAirbyteDestination(final IntegrationLauncher integrationLauncher,
                                   final AirbyteStreamFactory streamFactory,
                                   final AirbyteMessageBufferedWriterFactory messageWriterFactory,
                                   final ProtocolSerializer protocolSerializer,
                                   final boolean elapsedTimeTrackingEnabled,
                                   final MetricClient metricClient,
                                   final MetricAttribute[] metricAttrs) {
    this(
        integrationLauncher,
        streamFactory,
        messageWriterFactory,
        protocolSerializer,
        elapsedTimeTrackingEnabled,
        metricClient,
        metricAttrs,
        EMIT_ELAPSED_TIME_METRIC_INTERVAL_SECONDS);
  }

  @Override
  public void start(final WorkerDestinationConfig destinationConfig, final Path jobRoot) throws IOException, WorkerException {
    Preconditions.checkState(destinationProcess == null);

    LOGGER.info("Running destination...");
    destinationProcess = integrationLauncher.write(
        jobRoot,
        WorkerConstants.DESTINATION_CONFIG_JSON_FILENAME,
        Jsons.serialize(destinationConfig.getDestinationConnectionConfiguration()),
        WorkerConstants.DESTINATION_CATALOG_JSON_FILENAME,
        protocolSerializer.serialize(destinationConfig.getCatalog()));
    // stdout logs are logged elsewhere since stdout also contains data
    LineGobbler.gobble(destinationProcess.getErrorStream(), LOGGER::error, "airbyte-destination", CONTAINER_LOG_MDC_BUILDER);

    writer = messageWriterFactory.createWriter(new BufferedWriter(new OutputStreamWriter(destinationProcess.getOutputStream(), Charsets.UTF_8)));

    final List<Type> acceptedMessageTypes = List.of(Type.STATE, Type.TRACE, Type.CONTROL);
    messageIterator = streamFactory.create(IOs.newBufferedReader(destinationProcess.getInputStream()))
        .filter(message -> acceptedMessageTypes.contains(message.getType()))
        .iterator();
  }

  @Override
  public void accept(final AirbyteMessage message) throws IOException {
    if (elapsedTimeTrackingEnabled) {
      final ScheduledFuture<?> scheduledFuture = emitAcceptElapsedTimeMetricOnInterval();
      acceptWithoutTrackingElapsedTime(message);
      scheduledFuture.cancel(true);
    } else {
      acceptWithoutTrackingElapsedTime(message);
    }
  }

  @VisibleForTesting
  void acceptWithoutTrackingElapsedTime(final AirbyteMessage message) throws IOException {
    Preconditions.checkState(destinationProcess != null && !inputHasEnded.get());

    writer.write(message);
  }

  @Override
  public void notifyEndOfInput() throws IOException {
    if (elapsedTimeTrackingEnabled) {
      final ScheduledFuture<?> scheduledFuture = emitNotifyEndOfInputElapsedTimeMetricOnInterval();
      notifyEndOfInputWithoutTrackingElapsedTime();
      scheduledFuture.cancel(true);
    } else {
      notifyEndOfInputWithoutTrackingElapsedTime();
    }
  }

  @VisibleForTesting
  void notifyEndOfInputWithoutTrackingElapsedTime() throws IOException {
    Preconditions.checkState(destinationProcess != null && !inputHasEnded.get());

    writer.flush();
    writer.close();
    inputHasEnded.set(true);
  }

  @Override
  public void close() throws Exception {
    if (destinationProcess == null) {
      LOGGER.debug("Destination process already exited");
      return;
    }

    if (!inputHasEnded.get()) {
      notifyEndOfInput();
    }

    if (elapsedTimeTrackingEnabled) {
      scheduledExecutor.shutdownNow();

      try {
        scheduledExecutor.awaitTermination(EXECUTOR_SHUTDOWN_GRACE_PERIOD_SECONDS, TimeUnit.SECONDS);
        if (!scheduledExecutor.isTerminated()) {
          LOGGER.error("Failed to shutdown scheduled executor");
        }
      } catch (final InterruptedException e) {
        // Preserve the interrupt status
        Thread.currentThread().interrupt();
      }
    }

    LOGGER.debug("Closing destination process");
    WorkerUtils.gentleClose(destinationProcess, 1, TimeUnit.MINUTES);
    if (destinationProcess.isAlive() || !IGNORED_EXIT_CODES.contains(getExitValue())) {
      final String message =
          destinationProcess.isAlive() ? "Destination has not terminated " : "Destination process exit with code " + getExitValue();
      throw new WorkerException(message + ". This warning is normal if the job was cancelled.");
    }
  }

  @Override
  public void cancel() throws Exception {
    LOGGER.info("Attempting to cancel destination process...");

    if (destinationProcess == null) {
      LOGGER.info("Destination process no longer exists, cancellation is a no-op.");
    } else {
      LOGGER.info("Destination process exists, cancelling...");
      WorkerUtils.cancelProcess(destinationProcess);
      LOGGER.info("Cancelled destination process!");
    }
  }

  @Override
  public boolean isFinished() {
    Preconditions.checkState(destinationProcess != null);
    /*
     * As this check is done on every message read, it is important for this operation to be efficient.
     * Short circuit early to avoid checking the underlying process. Note: hasNext is blocking.
     */
    return !messageIterator.hasNext() && !destinationProcess.isAlive();
  }

  @Override
  public int getExitValue() {
    Preconditions.checkState(destinationProcess != null, "Destination process is null, cannot retrieve exit value.");
    Preconditions.checkState(!destinationProcess.isAlive(), "Destination process is still alive, cannot retrieve exit value.");

    if (exitValue == null) {
      exitValue = destinationProcess.exitValue();
    }

    return exitValue;
  }

  @Override
  public Optional<AirbyteMessage> attemptRead() {
    Preconditions.checkState(destinationProcess != null);

    return Optional.ofNullable(messageIterator.hasNext() ? messageIterator.next() : null);
  }

  public ScheduledFuture<?> emitNotifyEndOfInputElapsedTimeMetricOnInterval() {
    return emitMetricOnInterval(WORKER_DESTINATION_NOTIFY_END_OF_INPUT_ELAPSED_MILLISECS);
  }

  public ScheduledFuture<?> emitAcceptElapsedTimeMetricOnInterval() {
    return emitMetricOnInterval(WORKER_DESTINATION_ACCEPT_ELAPSED_MILLISECS);
  }

  private ScheduledFuture<?> emitMetricOnInterval(final MetricsRegistry metric) {
    final long startTime = System.currentTimeMillis();

    return scheduledExecutor.scheduleAtFixedRate(() -> metricClient.gauge(
        metric,
        System.currentTimeMillis() - startTime,
        metricAttrs), emitElapsedTimeMetricIntervalSeconds, emitElapsedTimeMetricIntervalSeconds, TimeUnit.SECONDS);
  }

}
