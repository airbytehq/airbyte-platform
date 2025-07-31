/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.connectorSidecar

import com.google.common.annotations.VisibleForTesting
import com.google.common.base.Stopwatch
import io.airbyte.commons.io.LineGobbler
import io.airbyte.commons.json.Jsons
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
import io.airbyte.persistence.job.models.IntegrationLauncherConfig
import io.airbyte.workers.helper.GsonPksExtractor
import io.airbyte.workers.internal.AirbyteStreamFactory
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
import io.micronaut.context.annotation.Value
import jakarta.inject.Named
import jakarta.inject.Singleton
import java.io.InputStream
import java.nio.file.Files
import java.nio.file.Path
import java.time.Duration
import java.util.Optional
import kotlin.system.exitProcess

private val logger = KotlinLogging.logger {}

@Singleton
class ConnectorWatcher(
  @Named("output") val outputPath: Path,
  @Named("configDir") val configDir: String,
  @Value("\${airbyte.sidecar.file-timeout-minutes}") val fileTimeoutMinutes: Int,
  @Value("\${airbyte.sidecar.file-timeout-minutes-within-sync}") val fileTimeoutMinutesWithinSync: Int,
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
    val sidecarInput = readSidecarInput()
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

  private fun readSidecarInput(): SidecarInput {
    val inputContent = readFile(FileConstants.SIDECAR_INPUT_FILE)
    return Jsons.deserialize(inputContent, SidecarInput::class.java)
  }

  private fun waitForConnectorOutput(input: SidecarInput) {
    val stopwatch = Stopwatch.createStarted()
    while (!areNeededFilesPresent()) {
      Thread.sleep(100)
      if (heartbeatMonitor.shouldAbort()) {
        logger.warn { "Heartbeat indicates that the workload is in a terminal state, exiting process" }
        exitInternalError()
      }
      val isWithinSync = input.discoverCatalogInput?.manual?.not() ?: false
      if (hasFileTimeoutReached(stopwatch, isWithinSync)) {
        val message = "Failed to find output files from connector within timeout of $fileTimeoutMinutes minute(s). Is the connector still running?"
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

  @VisibleForTesting
  fun readFile(fileName: String): String = Files.readString(Path.of(configDir, fileName))

  @VisibleForTesting
  fun areNeededFilesPresent(): Boolean = Files.exists(outputPath) && Files.exists(Path.of(configDir, FileConstants.EXIT_CODE_FILE))

  @VisibleForTesting
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

  @VisibleForTesting
  fun exitProperly() {
    logger.info { "Deliberately exiting process with code 0." }
    exitProcess(0)
  }

  @VisibleForTesting
  fun exitInternalError() {
    logger.info { "Deliberately exiting process with code 1." }
    exitProcess(1)
  }

  @VisibleForTesting
  fun exitFileNotFound() {
    logger.info { "Deliberately exiting process with code 2." }
    exitProcess(2)
  }

  fun hasFileTimeoutReached(
    stopwatch: Stopwatch,
    withinSync: Boolean,
  ): Boolean {
    val timeoutMinutes = if (withinSync) fileTimeoutMinutesWithinSync else fileTimeoutMinutes
    return stopwatch.elapsed() > Duration.ofMinutes(timeoutMinutes.toLong())
  }

  @VisibleForTesting
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

  @VisibleForTesting
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

  @VisibleForTesting
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
