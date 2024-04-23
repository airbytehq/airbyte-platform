package io.airbyte.connectorSidecar

import com.google.common.annotations.VisibleForTesting
import com.google.common.base.Stopwatch
import io.airbyte.api.client.WorkloadApiClient
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
import io.airbyte.persistence.job.models.IntegrationLauncherConfig
import io.airbyte.protocol.models.ConfiguredAirbyteCatalog
import io.airbyte.workers.helper.GsonPksExtractor
import io.airbyte.workers.internal.AirbyteStreamFactory
import io.airbyte.workers.internal.VersionedAirbyteStreamFactory
import io.airbyte.workers.internal.VersionedAirbyteStreamFactory.InvalidLineFailureConfiguration
import io.airbyte.workers.models.SidecarInput
import io.airbyte.workers.sync.OrchestratorConstants
import io.airbyte.workers.workload.JobOutputDocStore
import io.airbyte.workload.api.client.model.generated.WorkloadFailureRequest
import io.airbyte.workload.api.client.model.generated.WorkloadSuccessRequest
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.Value
import jakarta.inject.Named
import jakarta.inject.Singleton
import java.io.InputStream
import java.nio.file.Files
import java.nio.file.Path
import java.time.Duration
import java.util.Optional
import java.util.UUID
import kotlin.system.exitProcess

private val logger = KotlinLogging.logger {}

@Singleton
class ConnectorWatcher(
  @Named("output") val outputPath: Path,
  @Named("configDir") val configDir: String,
  @Value("\${airbyte.sidecar.file-timeout-minutes}") val fileTimeoutMinutes: Int,
  val connectorMessageProcessor: ConnectorMessageProcessor,
  val serDeProvider: AirbyteMessageSerDeProvider,
  val airbyteProtocolVersionedMigratorFactory: AirbyteProtocolVersionedMigratorFactory,
  val gsonPksExtractor: GsonPksExtractor,
  val workloadApiClient: WorkloadApiClient,
  val jobOutputDocStore: JobOutputDocStore,
) {
  fun run() {
    val input = Jsons.deserialize(readFile(OrchestratorConstants.SIDECAR_INPUT), SidecarInput::class.java)
    val checkConnectionConfiguration = input.checkConnectionInput
    val discoverCatalogInput = input.discoverCatalogInput
    val workloadId = input.workloadId
    val integrationLauncherConfig = input.integrationLauncherConfig
    try {
      val stopwatch: Stopwatch = Stopwatch.createStarted()
      while (!areNeededFilesPresent()) {
        Thread.sleep(100)
        if (fileTimeoutReach(stopwatch)) {
          logger.warn { "Failed to find output files from connector within timeout $fileTimeoutMinutes. Is the connector still running?" }
          val failureReason =
            FailureReason()
              .withFailureOrigin(FailureReason.FailureOrigin.UNKNOWN)
              .withExternalMessage("Failed to find output files from connector within timeout $fileTimeoutMinutes.")

          failWorkload(workloadId, failureReason)
          exitFileNotFound()
          // The return is needed for the test
          return
        }
      }
      val outputIS =
        if (Files.exists(Path.of(OrchestratorConstants.JOB_OUTPUT_FILENAME))) {
          Files.newInputStream(Path.of(OrchestratorConstants.JOB_OUTPUT_FILENAME))
        } else {
          InputStream.nullInputStream()
        }
      val exitCode = readFile(OrchestratorConstants.EXIT_CODE_FILE).trim().toInt()
      val streamFactory: AirbyteStreamFactory = getStreamFactory(integrationLauncherConfig)
      val connectorOutput: ConnectorJobOutput =
        when (input.operationType) {
          SidecarInput.OperationType.CHECK -> {
            connectorMessageProcessor.run(
              outputIS,
              streamFactory,
              ConnectorMessageProcessor.OperationInput(
                checkConnectionConfiguration,
              ),
              exitCode,
              input.operationType,
            )
          }

          SidecarInput.OperationType.DISCOVER -> {
            connectorMessageProcessor.run(
              outputIS,
              streamFactory,
              ConnectorMessageProcessor.OperationInput(
                discoveryInput = discoverCatalogInput,
              ),
              exitCode,
              input.operationType,
            )
          }

          SidecarInput.OperationType.SPEC -> {
            connectorMessageProcessor.run(
              outputIS,
              streamFactory,
              ConnectorMessageProcessor.OperationInput(),
              exitCode,
              input.operationType,
            )
          }
        }
      jobOutputDocStore.write(workloadId, connectorOutput)
      workloadApiClient.workloadApi.workloadSuccess(WorkloadSuccessRequest(workloadId))
    } catch (e: Exception) {
      logger.error(e) { "Error performing operation: ${e.javaClass.name}" }

      val output =
        when (input.operationType) {
          SidecarInput.OperationType.CHECK -> getFailedOutput(checkConnectionConfiguration, e)
          SidecarInput.OperationType.DISCOVER -> getFailedOutput(discoverCatalogInput, e)
          SidecarInput.OperationType.SPEC -> getFailedOutput(input.integrationLauncherConfig.dockerImage, e)
        }

      jobOutputDocStore.write(workloadId, output)
      failWorkload(workloadId, output.failureReason)
      exitInternalError()
    }

    exitProperly()
  }

  @VisibleForTesting
  fun readFile(fileName: String): String {
    return Files.readString(Path.of(configDir, fileName))
  }

  @VisibleForTesting
  fun areNeededFilesPresent(): Boolean {
    return Files.exists(outputPath) && Files.exists(Path.of(configDir, OrchestratorConstants.EXIT_CODE_FILE))
  }

  @VisibleForTesting
  fun getStreamFactory(integrationLauncherConfig: IntegrationLauncherConfig): AirbyteStreamFactory {
    return VersionedAirbyteStreamFactory<Any>(
      serDeProvider,
      airbyteProtocolVersionedMigratorFactory,
      if (integrationLauncherConfig.getProtocolVersion() != null) {
        integrationLauncherConfig.getProtocolVersion()
      } else {
        AirbyteProtocolVersion.DEFAULT_AIRBYTE_PROTOCOL_VERSION
      },
      Optional.empty<UUID>(),
      Optional.empty<ConfiguredAirbyteCatalog>(),
      InvalidLineFailureConfiguration(false),
      gsonPksExtractor,
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

  fun fileTimeoutReach(stopwatch: Stopwatch): Boolean {
    return stopwatch.elapsed() > Duration.ofMinutes(fileTimeoutMinutes.toLong())
  }

  @VisibleForTesting
  fun getFailedOutput(
    input: StandardCheckConnectionInput,
    e: Exception,
  ): ConnectorJobOutput {
    return ConnectorJobOutput().withOutputType(ConnectorJobOutput.OutputType.CHECK_CONNECTION)
      .withCheckConnection(
        StandardCheckConnectionOutput().withStatus(StandardCheckConnectionOutput.Status.FAILED)
          .withMessage("The check connection failed."),
      )
      .withFailureReason(
        FailureReason()
          .withFailureOrigin(
            if (input.getActorType() == ActorType.SOURCE) {
              FailureReason.FailureOrigin.SOURCE
            } else {
              FailureReason.FailureOrigin.DESTINATION
            },
          )
          .withExternalMessage("The check connection failed because of an internal error")
          .withInternalMessage(e.message)
          .withStacktrace(e.toString()),
      )
  }

  @VisibleForTesting
  fun getFailedOutput(
    input: StandardDiscoverCatalogInput,
    e: Exception,
  ): ConnectorJobOutput {
    return ConnectorJobOutput().withOutputType(ConnectorJobOutput.OutputType.DISCOVER_CATALOG_ID)
      .withDiscoverCatalogId(null)
      .withFailureReason(
        FailureReason()
          .withFailureOrigin(FailureReason.FailureOrigin.SOURCE)
          .withExternalMessage("The check connection failed because of an internal error for source: ${input.sourceId}")
          .withInternalMessage(e.message)
          .withStacktrace(e.toString()),
      )
  }

  @VisibleForTesting
  fun getFailedOutput(
    dockerImage: String,
    e: Exception,
  ): ConnectorJobOutput {
    return ConnectorJobOutput().withOutputType(ConnectorJobOutput.OutputType.SPEC)
      .withDiscoverCatalogId(null)
      .withFailureReason(
        FailureReason()
          .withFailureOrigin(FailureReason.FailureOrigin.SOURCE)
          .withExternalMessage("The spec failed because of an internal error for connector: $dockerImage")
          .withInternalMessage(e.message)
          .withStacktrace(e.toString()),
      )
  }

  private fun failWorkload(
    workloadId: String,
    failureReason: FailureReason?,
  ) {
    logger.info { "Failing workload $workloadId." }
    if (failureReason != null) {
      workloadApiClient.workloadApi.workloadFailure(
        WorkloadFailureRequest(
          workloadId,
          failureReason.failureOrigin.value(),
          failureReason.externalMessage,
        ),
      )
    } else {
      workloadApiClient.workloadApi.workloadFailure(WorkloadFailureRequest(workloadId))
    }
  }
}
