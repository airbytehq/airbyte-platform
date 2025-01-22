/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers;

import com.fasterxml.jackson.databind.JsonNode;
import io.airbyte.commons.io.IOs;
import io.airbyte.commons.json.Jsons;
import io.airbyte.config.AirbyteStream;
import io.airbyte.config.ConfiguredAirbyteCatalog;
import io.airbyte.config.WorkerDestinationConfig;
import io.airbyte.config.WorkerSourceConfig;
import io.airbyte.persistence.job.models.ReplicationInput;
import io.airbyte.protocol.models.AirbyteControlConnectorConfigMessage;
import io.airbyte.protocol.models.AirbyteControlMessage;
import io.airbyte.protocol.models.AirbyteMessage;
import io.airbyte.protocol.models.AirbyteMessage.Type;
import io.airbyte.protocol.models.AirbyteStreamNameNamespacePair;
import io.airbyte.protocol.models.Config;
import io.airbyte.workers.internal.AirbyteStreamFactory;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Worker static utils.
 */
// TODO:(Issue-4824): Figure out how to log Docker process information.
public class WorkerUtils {

  private static final Logger LOGGER = LoggerFactory.getLogger(WorkerUtils.class);

  /**
   * Waits until process ends or until time elapses.
   *
   * @param process process to wait for
   * @param timeout timeout magnitude
   * @param timeUnit timeout unit
   */
  public static void gentleClose(final Process process, final long timeout, final TimeUnit timeUnit) {

    if (process == null) {
      return;
    }

    if (process.info() != null) {
      process.info().commandLine().ifPresent(commandLine -> LOGGER.debug("Gently closing process {}", commandLine));
    }

    try {
      if (process.isAlive()) {
        process.waitFor(timeout, timeUnit);
      }
    } catch (final InterruptedException e) {
      LOGGER.error("Exception while while waiting for process to finish", e);
    }

    if (process.isAlive()) {
      closeProcess(process, Duration.of(1, ChronoUnit.MINUTES));
    }
  }

  /**
   * Signal process to close. Once time has elapsed end it forcefully.
   *
   * @param process process to close
   * @param lastChanceDuration timeout
   */
  public static void closeProcess(final Process process, final Duration lastChanceDuration) {
    if (process == null) {
      return;
    }
    try {
      process.destroy();
      process.waitFor(lastChanceDuration.toMillis(), TimeUnit.MILLISECONDS);
      if (process.isAlive()) {
        LOGGER.warn("Process is still alive after calling destroy. Attempting to destroy forcibly...");
        process.destroyForcibly();
      }
    } catch (final InterruptedException e) {
      LOGGER.error("Exception when closing process.", e);
    }
  }

  /**
   * Wait for process indefinitely.
   *
   * @param process process to wait for
   */
  public static void wait(final Process process) {
    try {
      process.waitFor();
    } catch (final InterruptedException e) {
      LOGGER.error("Exception while while waiting for process to finish", e);
    }
  }

  /**
   * Translates a StandardSyncInput into a WorkerSourceConfig. WorkerSourceConfig is a subset of
   * StandardSyncInput.
   */
  public static WorkerSourceConfig syncToWorkerSourceConfig(final ReplicationInput replicationInput) {
    return new WorkerSourceConfig()
        .withSourceId(replicationInput.getSourceId())
        .withSourceConnectionConfiguration(replicationInput.getSourceConfiguration())
        .withCatalog(replicationInput.getCatalog())
        .withState(replicationInput.getState());
  }

  /**
   * Translates a StandardSyncInput into a WorkerDestinationConfig. WorkerDestinationConfig is a
   * subset of StandardSyncInput.{}
   */
  public static WorkerDestinationConfig syncToWorkerDestinationConfig(final ReplicationInput replicationInput) {
    return new WorkerDestinationConfig()
        .withConnectionId(replicationInput.getConnectionId())
        .withDestinationId(replicationInput.getDestinationId())
        .withDestinationConnectionConfiguration(replicationInput.getDestinationConfiguration())
        .withCatalog(replicationInput.getCatalog())
        .withState(replicationInput.getState());
  }

  /**
   * Get most recent control message from map of message type to messages.
   *
   * @param typeToMsgs message type to messages
   * @return control message, if present.
   */
  public static Optional<AirbyteControlConnectorConfigMessage> getMostRecentConfigControlMessage(final Map<Type, List<AirbyteMessage>> typeToMsgs) {
    return typeToMsgs.getOrDefault(Type.CONTROL, new ArrayList<>()).stream()
        .map(AirbyteMessage::getControl)
        .filter(control -> control.getType() == AirbyteControlMessage.Type.CONNECTOR_CONFIG)
        .map(AirbyteControlMessage::getConnectorConfig)
        .reduce((first, second) -> second);
  }

  /**
   * Determine if a control message has updated the config for the connector.
   *
   * @param initialConfigJson original config json
   * @param configMessage control message that is changing the config message
   * @return true, if control message is actually changing the original config. otherwise, false.
   */
  public static Boolean getDidControlMessageChangeConfig(final JsonNode initialConfigJson, final AirbyteControlConnectorConfigMessage configMessage) {
    final Config newConfig = configMessage.getConfig();
    final JsonNode newConfigJson = Jsons.jsonNode(newConfig);
    return !initialConfigJson.equals(newConfigJson);
  }

  /**
   * Group messages from process by type.
   *
   * @param process process emitting messages.
   * @param streamFactory stream factory producing message.
   * @param timeOut time out.
   * @return map of message type to messages
   * @throws IOException exception while reading
   */
  public static Map<Type, List<AirbyteMessage>> getMessagesByType(final Process process, final AirbyteStreamFactory streamFactory, final int timeOut)
      throws IOException {
    final Map<Type, List<AirbyteMessage>> messagesByType;
    try (final InputStream stdout = process.getInputStream()) {
      messagesByType = streamFactory.create(IOs.newBufferedReader(stdout))
          .collect(Collectors.groupingBy(AirbyteMessage::getType));

      WorkerUtils.gentleClose(process, timeOut, TimeUnit.MINUTES);
      return messagesByType;
    }
  }

  /**
   * Map stream names to their respective schemas.
   *
   * @param catalog sync input
   * @return map of stream names to their schemas
   */
  public static Map<AirbyteStreamNameNamespacePair, JsonNode> mapStreamNamesToSchemas(final ConfiguredAirbyteCatalog catalog) {
    return catalog.getStreams().stream().collect(
        Collectors.toMap(
            k -> airbyteStreamNameNamespacePairFromAirbyteStream(k.getStream()),
            v -> v.getStream().getJsonSchema()));

  }

  private static AirbyteStreamNameNamespacePair airbyteStreamNameNamespacePairFromAirbyteStream(final AirbyteStream stream) {
    return new AirbyteStreamNameNamespacePair(stream.getName(), stream.getNamespace());
  }

}
