/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.commons.io.IOs
import io.airbyte.commons.json.Jsons
import io.airbyte.config.AirbyteStream
import io.airbyte.config.ConfiguredAirbyteCatalog
import io.airbyte.config.ConfiguredAirbyteStream
import io.airbyte.config.WorkerDestinationConfig
import io.airbyte.config.WorkerSourceConfig
import io.airbyte.persistence.job.models.ReplicationInput
import io.airbyte.protocol.models.v0.AirbyteControlConnectorConfigMessage
import io.airbyte.protocol.models.v0.AirbyteControlMessage
import io.airbyte.protocol.models.v0.AirbyteMessage
import io.airbyte.protocol.models.v0.AirbyteStreamNameNamespacePair
import io.airbyte.workers.internal.AirbyteStreamFactory
import io.airbyte.workers.internal.MessageOrigin
import io.github.oshai.kotlinlogging.KotlinLogging
import java.io.IOException
import java.time.Duration
import java.time.temporal.ChronoUnit
import java.util.Optional
import java.util.concurrent.TimeUnit
import java.util.stream.Collectors

// TODO:(Issue-4824): Figure out how to log Docker process information.

/**
 * Worker static utils.
 */
object WorkerUtils {
  private val log = KotlinLogging.logger {}

  /**
   * Waits until process ends or until time elapses.
   *
   * @param process process to wait for
   * @param timeout timeout magnitude
   * @param timeUnit timeout unit
   */
  fun gentleClose(
    process: Process?,
    timeout: Long,
    timeUnit: TimeUnit?,
  ) {
    if (process == null) {
      return
    }

    if (process.info() != null) {
      process.info().commandLine().ifPresent { commandLine: String? ->
        log.debug(
          "Gently closing process {}",
          commandLine,
        )
      }
    }

    try {
      if (process.isAlive) {
        process.waitFor(timeout, timeUnit)
      }
    } catch (e: InterruptedException) {
      log.error(e) { "Exception while while waiting for process to finish" }
    }

    if (process.isAlive) {
      closeProcess(process, Duration.of(1, ChronoUnit.MINUTES))
    }
  }

  /**
   * Signal process to close. Once time has elapsed end it forcefully.
   *
   * @param process process to close
   * @param lastChanceDuration timeout
   */
  fun closeProcess(
    process: Process?,
    lastChanceDuration: Duration,
  ) {
    if (process == null) {
      return
    }
    try {
      process.destroy()
      process.waitFor(lastChanceDuration.toMillis(), TimeUnit.MILLISECONDS)
      if (process.isAlive) {
        log.warn { "Process is still alive after calling destroy. Attempting to destroy forcibly..." }
        process.destroyForcibly()
      }
    } catch (e: InterruptedException) {
      log.error(e) { "Exception when closing process." }
    }
  }

  /**
   * Wait for process indefinitely.
   *
   * @param process process to wait for
   */
  fun wait(process: Process) {
    try {
      process.waitFor()
    } catch (e: InterruptedException) {
      log.error(e) { "Exception while while waiting for process to finish" }
    }
  }

  /**
   * Translates a StandardSyncInput into a WorkerSourceConfig. WorkerSourceConfig is a subset of
   * StandardSyncInput.
   */
  fun syncToWorkerSourceConfig(replicationInput: ReplicationInput): WorkerSourceConfig =
    WorkerSourceConfig()
      .withSourceId(replicationInput.sourceId)
      .withSourceConnectionConfiguration(replicationInput.sourceConfiguration)
      .withCatalog(replicationInput.catalog)
      .withState(replicationInput.state)

  /**
   * Translates a StandardSyncInput into a WorkerDestinationConfig. WorkerDestinationConfig is a
   * subset of StandardSyncInput.{}
   */
  fun syncToWorkerDestinationConfig(replicationInput: ReplicationInput): WorkerDestinationConfig =
    WorkerDestinationConfig()
      .withConnectionId(replicationInput.connectionId)
      .withDestinationId(replicationInput.destinationId)
      .withDestinationConnectionConfiguration(replicationInput.destinationConfiguration)
      .withCatalog(replicationInput.catalog)
      .withState(replicationInput.state)

  /**
   * Get most recent control message from map of message type to messages.
   *
   * @param typeToMsgs message type to messages
   * @return control message, if present.
   */
  fun getMostRecentConfigControlMessage(typeToMsgs: Map<AirbyteMessage.Type, List<AirbyteMessage>>): Optional<AirbyteControlConnectorConfigMessage> =
    typeToMsgs
      .getOrDefault(AirbyteMessage.Type.CONTROL, ArrayList())
      .stream()
      .map { obj: AirbyteMessage -> obj.control }
      .filter { control: AirbyteControlMessage -> control.type == AirbyteControlMessage.Type.CONNECTOR_CONFIG }
      .map { obj: AirbyteControlMessage -> obj.connectorConfig }
      .reduce { first: AirbyteControlConnectorConfigMessage?, second: AirbyteControlConnectorConfigMessage? -> second }

  /**
   * Determine if a control message has updated the config for the connector.
   *
   * @param initialConfigJson original config json
   * @param configMessage control message that is changing the config message
   * @return true, if control message is actually changing the original config. otherwise, false.
   */
  fun getDidControlMessageChangeConfig(
    initialConfigJson: JsonNode,
    configMessage: AirbyteControlConnectorConfigMessage,
  ): Boolean {
    val newConfig = configMessage.config
    val newConfigJson = Jsons.jsonNode(newConfig)
    return initialConfigJson != newConfigJson
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
  @Throws(IOException::class)
  fun getMessagesByType(
    process: Process,
    streamFactory: AirbyteStreamFactory,
    timeOut: Int,
  ): Map<AirbyteMessage.Type, List<AirbyteMessage>> {
    val messagesByType: Map<AirbyteMessage.Type, List<AirbyteMessage>>
    process.inputStream.use { stdout ->
      messagesByType =
        streamFactory
          .create(IOs.newBufferedReader(stdout), MessageOrigin.SOURCE)
          .collect(
            Collectors.groupingBy { obj: AirbyteMessage -> obj.type },
          )
      gentleClose(process, timeOut.toLong(), TimeUnit.MINUTES)
      return messagesByType
    }
  }

  /**
   * Map stream names to their respective schemas.
   *
   * @param catalog sync input
   * @return map of stream names to their schemas
   */
  fun mapStreamNamesToSchemas(catalog: ConfiguredAirbyteCatalog): MutableMap<AirbyteStreamNameNamespacePair, JsonNode?> =
    catalog.streams.stream().collect(
      Collectors.toMap(
        { k: ConfiguredAirbyteStream -> airbyteStreamNameNamespacePairFromAirbyteStream(k.stream) },
        { v: ConfiguredAirbyteStream -> v.stream.jsonSchema },
      ),
    ) as MutableMap<AirbyteStreamNameNamespacePair, JsonNode?>

  private fun airbyteStreamNameNamespacePairFromAirbyteStream(stream: AirbyteStream): AirbyteStreamNameNamespacePair =
    AirbyteStreamNameNamespacePair(stream.name, stream.namespace)
}
