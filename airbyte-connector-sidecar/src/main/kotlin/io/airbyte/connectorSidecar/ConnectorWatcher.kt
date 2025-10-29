/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.connectorSidecar

import io.airbyte.commons.annotation.InternalForTesting
import io.airbyte.commons.io.IOs
import io.airbyte.commons.io.LineGobbler
import io.airbyte.commons.logging.LogSource
import io.airbyte.commons.protocol.AirbyteMessageSerDeProvider
import io.airbyte.commons.protocol.AirbyteProtocolVersionedMigratorFactory
import io.airbyte.commons.version.AirbyteProtocolVersion
import io.airbyte.config.ActorType
import io.airbyte.config.ConnectorJobOutput
import io.airbyte.config.FailureReason
import io.airbyte.config.StandardCheckConnectionInput
import io.airbyte.config.StandardCheckConnectionOutput
import io.airbyte.config.StandardDiscoverCatalogInput
import io.airbyte.metrics.MetricClient
import io.airbyte.micronaut.runtime.AirbyteConnectorConfig
import io.airbyte.micronaut.runtime.AirbyteSidecarConfig
import io.airbyte.persistence.job.models.IntegrationLauncherConfig
import io.airbyte.workers.helper.GsonPksExtractor
import io.airbyte.workers.internal.AirbyteStreamFactory
import io.airbyte.workers.internal.MessageOrigin
import io.airbyte.workers.internal.VersionedAirbyteStreamFactory
import io.airbyte.workers.internal.VersionedAirbyteStreamFactory.InvalidLineFailureConfiguration
import io.airbyte.workers.models.SidecarInput
import io.airbyte.workers.pod.FileConstants
import io.airbyte.workers.workload.WorkloadOutputWriter
import io.airbyte.workload.api.client.WorkloadApiClient
import io.airbyte.workload.api.domain.WorkloadFailureRequest
import io.airbyte.workload.api.domain.WorkloadSuccessRequest
import io.github.oshai.kotlinlogging.KotlinLogging
import io.github.oshai.kotlinlogging.withLoggingContext
import jakarta.inject.Named
import jakarta.inject.Singleton
import java.io.InputStream
import java.nio.file.Files
import java.nio.file.Path
import java.time.Duration
import java.util.Optional
import kotlin.system.exitProcess
import kotlin.time.TimeSource
import kotlin.time.toJavaDuration

private val logger = KotlinLogging.logger {}

@Singleton
class ConnectorWatcher(
  @Named("output") val outputPath: Path,
  private val airbyteConnectorConfig: AirbyteConnectorConfig,
  private val airbyteSidecarConfig: AirbyteSidecarConfig,
  private val sidecarInput: SidecarInput,
  private val connectorMessageProcessor: ConnectorMessageProcessor,
  private val serDeProvider: AirbyteMessageSerDeProvider,
  private val airbyteProtocolVersionedMigratorFactory: AirbyteProtocolVersionedMigratorFactory,
  private val gsonPksExtractor: GsonPksExtractor,
  private val workloadApiClient: WorkloadApiClient,
  private val outputWriter: WorkloadOutputWriter,
  private val logContextFactory: SidecarLogContextFactory,
  private val heartbeatMonitor: HeartbeatMonitor,
  private val metricClient: MetricClient,
) {
  fun run() {
    withLoggingContext(logContextFactory.create(sidecarInput.logPath)) {
      LineGobbler.startSection(sidecarInput.operationType.toString())
      var heartbeatStarted = false
      try {
        heartbeatMonitor.startHeartbeatThread(sidecarInput)
        heartbeatStarted = true

        waitForConnectorOutput(sidecarInput)

        if (heartbeatMonitor.shouldAbort()) {
          logger.warn { "Heartbeat indicates that the workload is in a terminal state, exiting process" }
          exitInternalError()
        }
        val connectorOutput = processConnectorOutput(sidecarInput)
        saveConnectorOutput(sidecarInput.workloadId, connectorOutput)
        markWorkloadSuccess(sidecarInput.workloadId)
      } catch (e: Exception) {
        handleException(sidecarInput, e)
      } finally {
        if (heartbeatStarted) {
          heartbeatMonitor.stopHeartbeatThread()
        }
        LineGobbler.endSection(sidecarInput.operationType.toString())
        exitProperly()
      }
    }
  }

  private fun waitForConnectorOutput(input: SidecarInput) {
    val stopwatch = TimeSource.Monotonic
    while (!areNeededFilesPresent()) {
      Thread.sleep(100)
      if (heartbeatMonitor.shouldAbort()) {
        logger.warn { "Heartbeat indicates that the workload is in a terminal state, exiting process" }
        exitInternalError()
      }
      val isWithinSync = input.discoverCatalogInput?.manual?.not() ?: false
      if (hasFileTimeoutReached(stopwatch, isWithinSync)) {
        readOutputForLogs()

        val message =
          "Failed to find output files from connector within timeout of " +
            "${airbyteSidecarConfig.fileTimeoutMinutes} minute(s). Is the connector still running?"
        logger.warn { message }
        val failureReason =
          FailureReason()
            .withFailureOrigin(FailureReason.FailureOrigin.UNKNOWN)
            .withExternalMessage(message)
        failWorkload(input.workloadId, failureReason)
        exitFileNotFound()
      }
    }
  }

  /**
   * Reads the output file using the AirbyteStreamFactory.
   * This has the side effect of processing the log messages from the connector.
   */
  private fun readOutputForLogs() {
    val stream = getConnectorOutputStream()
    val streamFactory = getStreamFactory(sidecarInput.integrationLauncherConfig)
    withLoggingContext(logContextFactory.createConnectorContext(sidecarInput.logPath)) {
      streamFactory
        .create(
          bufferedReader = IOs.newBufferedReader(stream),
          origin = if (logContextFactory.inferLogSource() == LogSource.DESTINATION) MessageOrigin.DESTINATION else MessageOrigin.SOURCE,
        ).forEach {
          // We're just forcing the stream reader to read the messages.
          // The expected side effect is that the stream reader will log AirbyteLog Messages
        }
    }
  }

  private fun processConnectorOutput(input: SidecarInput): ConnectorJobOutput {
    logger.info { "Connector exited, processing output" }
    val outputStream = getConnectorOutputStream()
    val exitCode = readFile(FileConstants.EXIT_CODE_FILE).trim().toInt()
    logger.info { "Connector exited with exit code $exitCode" }
    val streamFactory = getStreamFactory(input.integrationLauncherConfig)

    return when (input.operationType) {
      SidecarInput.OperationType.CHECK ->
        connectorMessageProcessor.run(
          outputStream,
          streamFactory,
          ConnectorMessageProcessor.OperationInput(checkInput = input.checkConnectionInput),
          exitCode,
          input.operationType,
        )
      SidecarInput.OperationType.DISCOVER ->
        connectorMessageProcessor.run(
          outputStream,
          streamFactory,
          ConnectorMessageProcessor.OperationInput(discoveryInput = input.discoverCatalogInput),
          exitCode,
          input.operationType,
        )
      SidecarInput.OperationType.SPEC ->
        connectorMessageProcessor.run(
          outputStream,
          streamFactory,
          ConnectorMessageProcessor.OperationInput(),
          exitCode,
          input.operationType,
        )
    }
  }

  private fun getConnectorOutputStream(): InputStream {
    val outputFilePath = Path.of(FileConstants.JOB_OUTPUT_FILE)
    return if (Files.exists(outputFilePath)) {
      logger.info { "Output file ${FileConstants.JOB_OUTPUT_FILE} found" }
      Files.newInputStream(outputFilePath)
    } else {
      InputStream.nullInputStream()
    }
  }

  private fun saveConnectorOutput(
    workloadId: String,
    connectorOutput: ConnectorJobOutput,
  ) {
    logger.info { "Writing output of $workloadId to the doc store" }
    outputWriter.write(workloadId, connectorOutput)
  }

  private fun markWorkloadSuccess(workloadId: String) {
    logger.info { "Marking workload $workloadId as successful" }
    workloadApiClient.workloadSuccess(WorkloadSuccessRequest(workloadId))
  }

  fun handleException(
    input: SidecarInput,
    e: Exception,
  ) {
    try {
      logger.error(e) { "Error performing operation: ${e.javaClass.name}" }
      val connectorOutput =
        when (input.operationType) {
          SidecarInput.OperationType.CHECK -> getFailedOutput(input.checkConnectionInput, e)
          SidecarInput.OperationType.DISCOVER -> getFailedOutput(input.discoverCatalogInput, e)
          SidecarInput.OperationType.SPEC -> getFailedOutput(input.integrationLauncherConfig.dockerImage, e)
        }
      outputWriter.write(input.workloadId, connectorOutput)
      failWorkload(input.workloadId, connectorOutput.failureReason)
    } catch (e: Exception) {
      failWorkload(
        input.workloadId,
        FailureReason()
          .withFailureOrigin(FailureReason.FailureOrigin.AIRBYTE_PLATFORM)
          .withExternalMessage("Unable to persist the job Output, check the document store credentials.")
          .withInternalMessage(e.message)
          .withStacktrace(e.stackTraceToString()),
      )
    } finally {
      exitInternalError()
    }
  }

  @InternalForTesting
  fun readFile(fileName: String): String = Files.readString(Path.of(airbyteConnectorConfig.configDir, fileName))

  @InternalForTesting
  fun areNeededFilesPresent(): Boolean =
    Files.exists(outputPath) && Files.exists(Path.of(airbyteConnectorConfig.configDir, FileConstants.EXIT_CODE_FILE))

  @InternalForTesting
  fun getStreamFactory(integrationLauncherConfig: IntegrationLauncherConfig): AirbyteStreamFactory {
    val protocolVersion =
      integrationLauncherConfig.protocolVersion
        ?: AirbyteProtocolVersion.DEFAULT_AIRBYTE_PROTOCOL_VERSION
    return VersionedAirbyteStreamFactory<Any>(
      serDeProvider = serDeProvider,
      migratorFactory = airbyteProtocolVersionedMigratorFactory,
      protocolVersion = protocolVersion,
      connectionId = Optional.empty(),
      configuredAirbyteCatalog = Optional.empty(),
      invalidLineFailureConfiguration = InvalidLineFailureConfiguration(false),
      gsonPksExtractor = gsonPksExtractor,
      metricClient = metricClient,
    )
  }

  @InternalForTesting
  fun exitProperly() {
    logger.info { "Deliberately exiting process with code 0." }
    exitProcess(0)
  }

  @InternalForTesting
  fun exitInternalError() {
    logger.info { "Deliberately exiting process with code 1." }
    exitProcess(1)
  }

  @InternalForTesting
  fun exitFileNotFound() {
    logger.info { "Deliberately exiting process with code 2." }
    exitProcess(2)
  }

  fun hasFileTimeoutReached(
    stopwatch: TimeSource.Monotonic,
    withinSync: Boolean,
  ): Boolean {
    val timeoutMinutes = if (withinSync) airbyteSidecarConfig.fileTimeoutMinutesWithinSync else airbyteSidecarConfig.fileTimeoutMinutes
    return stopwatch.markNow().elapsedNow().toJavaDuration() > Duration.ofMinutes(timeoutMinutes.toLong())
  }

  @InternalForTesting
  fun getFailedOutput(
    input: StandardCheckConnectionInput?,
    e: Exception,
  ): ConnectorJobOutput {
    val failureOrigin =
      if (input?.actorType == ActorType.SOURCE) {
        FailureReason.FailureOrigin.SOURCE
      } else {
        FailureReason.FailureOrigin.DESTINATION
      }

    val failureReason =
      FailureReason()
        .withFailureOrigin(failureOrigin)
        .withExternalMessage("The check connection failed due to an internal error.")
        .withInternalMessage(e.message)
        .withStacktrace(e.stackTraceToString())

    val checkConnectionOutput =
      StandardCheckConnectionOutput()
        .withStatus(StandardCheckConnectionOutput.Status.FAILED)
        .withMessage("The check connection failed.")

    return ConnectorJobOutput()
      .withOutputType(ConnectorJobOutput.OutputType.CHECK_CONNECTION)
      .withCheckConnection(checkConnectionOutput)
      .withFailureReason(failureReason)
  }

  @InternalForTesting
  fun getFailedOutput(
    input: StandardDiscoverCatalogInput?,
    e: Exception,
  ): ConnectorJobOutput {
    val failureReason =
      FailureReason()
        .withFailureOrigin(FailureReason.FailureOrigin.SOURCE)
        .withExternalMessage("The discover catalog failed due to an internal error for source: ${input?.sourceId}")
        .withInternalMessage(e.message)
        .withStacktrace(e.stackTraceToString())

    return ConnectorJobOutput()
      .withOutputType(ConnectorJobOutput.OutputType.DISCOVER_CATALOG_ID)
      .withDiscoverCatalogId(null)
      .withFailureReason(failureReason)
  }

  @InternalForTesting
  fun getFailedOutput(
    dockerImage: String,
    e: Exception,
  ): ConnectorJobOutput {
    val failureReason =
      FailureReason()
        .withFailureOrigin(FailureReason.FailureOrigin.SOURCE)
        .withExternalMessage("The spec failed due to an internal error for connector: $dockerImage")
        .withInternalMessage(e.message)
        .withStacktrace(e.stackTraceToString())

    return ConnectorJobOutput()
      .withOutputType(ConnectorJobOutput.OutputType.SPEC)
      .withDiscoverCatalogId(null)
      .withFailureReason(failureReason)
  }

  private fun failWorkload(
    workloadId: String,
    failureReason: FailureReason?,
  ) {
    logger.info { "Failing workload $workloadId." }
    val request =
      if (failureReason != null) {
        WorkloadFailureRequest(
          workloadId,
          failureReason.failureOrigin.value(),
          failureReason.externalMessage,
        )
      } else {
        WorkloadFailureRequest(workloadId)
      }
    workloadApiClient.workloadFailure(request)
  }
}
