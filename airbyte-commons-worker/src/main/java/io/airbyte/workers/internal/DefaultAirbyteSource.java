/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.internal;

import static io.airbyte.commons.constants.WorkerConstants.KubeConstants.POD_READY_TIMEOUT;
import static io.airbyte.metrics.lib.ApmTraceConstants.WORKER_OPERATION_NAME;
import static io.airbyte.protocol.models.AirbyteMessage.Type.RECORD;
import static io.airbyte.protocol.models.AirbyteMessage.Type.STATE;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import datadog.trace.api.Trace;
import dev.failsafe.Failsafe;
import dev.failsafe.RetryPolicy;
import io.airbyte.commons.constants.WorkerConstants;
import io.airbyte.commons.io.IOs;
import io.airbyte.commons.io.LineGobbler;
import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.logging.LoggingHelper;
import io.airbyte.commons.logging.LoggingHelper.Color;
import io.airbyte.commons.logging.MdcScope;
import io.airbyte.commons.logging.MdcScope.Builder;
import io.airbyte.commons.protocol.ProtocolSerializer;
import io.airbyte.config.WorkerSourceConfig;
import io.airbyte.metrics.lib.MetricClient;
import io.airbyte.protocol.models.AirbyteMessage;
import io.airbyte.protocol.models.AirbyteMessage.Type;
import io.airbyte.workers.WorkerUtils;
import io.airbyte.workers.exception.WorkerException;
import io.airbyte.workers.process.IntegrationLauncher;
import java.nio.file.Path;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default implementation of {@link AirbyteSource}.
 */
public class DefaultAirbyteSource implements AirbyteSource {

  private static final Logger LOGGER = LoggerFactory.getLogger(DefaultAirbyteSource.class);

  private static final Duration GRACEFUL_SHUTDOWN_DURATION = Duration.of(1, ChronoUnit.MINUTES);
  static final Set<Integer> IGNORED_EXIT_CODES = Set.of(
      0, // Normal exit
      143 // SIGTERM
  );

  public static final MdcScope.Builder CONTAINER_LOG_MDC_BUILDER = new Builder()
      .setLogPrefix(LoggingHelper.SOURCE_LOGGER_PREFIX)
      .setPrefixColor(Color.BLUE_BACKGROUND);

  private final IntegrationLauncher integrationLauncher;
  private final AirbyteStreamFactory streamFactory;
  private final ProtocolSerializer protocolSerializer;
  private final HeartbeatMonitor heartbeatMonitor;
  private final MessageMetricsTracker messageMetricsTracker;

  private Process sourceProcess = null;
  private Iterator<AirbyteMessage> messageIterator = null;
  private Integer exitValue = null;
  private final boolean featureFlagLogConnectorMsgs;

  public DefaultAirbyteSource(final IntegrationLauncher integrationLauncher,
                              final AirbyteStreamFactory streamFactory,
                              final HeartbeatMonitor heartbeatMonitor,
                              final ProtocolSerializer protocolSerializer,
                              final boolean logConnectorMsgs,
                              final MetricClient metricClient) {
    this.integrationLauncher = integrationLauncher;
    this.streamFactory = streamFactory;
    this.protocolSerializer = protocolSerializer;
    this.heartbeatMonitor = heartbeatMonitor;
    this.featureFlagLogConnectorMsgs = logConnectorMsgs;
    this.messageMetricsTracker = new MessageMetricsTracker(metricClient);
  }

  @Override
  public void start(final WorkerSourceConfig sourceConfig, final Path jobRoot, final UUID connectionId) throws Exception {
    Preconditions.checkState(sourceProcess == null);

    if (connectionId != null) {
      messageMetricsTracker.trackConnectionId(connectionId);
    }

    sourceProcess = integrationLauncher.read(jobRoot,
        WorkerConstants.SOURCE_CONFIG_JSON_FILENAME,
        Jsons.serialize(sourceConfig.getSourceConnectionConfiguration()),
        WorkerConstants.SOURCE_CATALOG_JSON_FILENAME,
        protocolSerializer.serialize(sourceConfig.getCatalog(), false),
        sourceConfig.getState() == null ? null : WorkerConstants.INPUT_STATE_JSON_FILENAME,
        // TODO We should be passing a typed state here and use the protocolSerializer
        sourceConfig.getState() == null ? null : Jsons.serialize(sourceConfig.getState().getState()));
    // stdout logs are logged elsewhere since stdout also contains data
    LineGobbler.gobble(sourceProcess.getErrorStream(), LOGGER::error, "airbyte-source", CONTAINER_LOG_MDC_BUILDER);

    logInitialStateAsJSON(sourceConfig);

    final List<Type> acceptedMessageTypes = List.of(Type.RECORD, STATE, Type.TRACE, Type.CONTROL);
    Failsafe.with(RetryPolicy.builder().withBackoff(Duration.ofSeconds(10), POD_READY_TIMEOUT).build()).run(() -> {
      messageIterator = streamFactory.create(IOs.newBufferedReader(sourceProcess.getInputStream()))
          .peek(message -> {
            if (shouldBeat(message.getType())) {
              heartbeatMonitor.beat();
            }
          })
          .filter(message -> acceptedMessageTypes.contains(message.getType()))
          .iterator();
    });

  }

  @VisibleForTesting
  static boolean shouldBeat(final AirbyteMessage.Type airbyteMessageType) {
    return airbyteMessageType == STATE || airbyteMessageType == RECORD;
  }

  @Override
  public boolean isFinished() {
    Preconditions.checkState(sourceProcess != null);

    /*
     * As this check is done on every message read, it is important for this operation to be efficient.
     * Short circuit early to avoid checking the underlying process. note: hasNext is blocking.
     */
    return !messageIterator.hasNext() && !sourceProcess.isAlive();
  }

  @Override
  public int getExitValue() throws IllegalStateException {
    Preconditions.checkState(sourceProcess != null, "Source process is null, cannot retrieve exit value.");
    Preconditions.checkState(!sourceProcess.isAlive(), "Source process is still alive, cannot retrieve exit value.");

    if (exitValue == null) {
      exitValue = sourceProcess.exitValue();
    }

    return exitValue;
  }

  @Override
  public Optional<AirbyteMessage> attemptRead() {
    Preconditions.checkState(sourceProcess != null);

    final Optional<AirbyteMessage> m = Optional.ofNullable(messageIterator.hasNext() ? messageIterator.next() : null);
    if (m.isPresent()) {
      messageMetricsTracker.trackSourceRead(m.get().getType());
    }
    return m;
  }

  @Override
  public void close() throws Exception {
    emitSourceMessageReadCountMetric();

    if (sourceProcess == null) {
      LOGGER.debug("Source process already exited");
      return;
    }

    LOGGER.debug("Closing source process");
    WorkerUtils.gentleClose(
        sourceProcess,
        GRACEFUL_SHUTDOWN_DURATION.toMillis(),
        TimeUnit.MILLISECONDS);

    if (sourceProcess.isAlive() || !IGNORED_EXIT_CODES.contains(getExitValue())) {
      final String message = sourceProcess.isAlive() ? "Source has not terminated " : "Source process exit with code " + getExitValue();
      throw new WorkerException(message + ". This warning is normal if the job was cancelled.");
    }
  }

  @Trace(operationName = WORKER_OPERATION_NAME)
  @Override
  public void cancel() throws Exception {
    emitSourceMessageReadCountMetric();

    LOGGER.info("Attempting to cancel source process...");

    if (sourceProcess == null) {
      LOGGER.info("Source process no longer exists, cancellation is a no-op.");
    } else {
      LOGGER.info("Source process exists, cancelling...");
      WorkerUtils.cancelProcess(sourceProcess);
      LOGGER.info("Cancelled source process!");
    }
  }

  private void emitSourceMessageReadCountMetric() {
    messageMetricsTracker.flushSourceReadCountMetric();
  }

  private void logInitialStateAsJSON(final WorkerSourceConfig sourceConfig) {
    if (!featureFlagLogConnectorMsgs) {
      return;
    }

    if (sourceConfig.getState() == null) {
      LOGGER.info("source starting state | empty");
      return;
    }

    LOGGER.info("source starting state | " + Jsons.serialize(sourceConfig.getState().getState()));
  }

}
