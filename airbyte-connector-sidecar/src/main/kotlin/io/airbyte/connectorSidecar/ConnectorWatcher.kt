package io.airbyte.connectorSidecar

import com.google.common.annotations.VisibleForTesting
import com.google.common.base.Stopwatch
import io.airbyte.commons.json.Jsons
import io.airbyte.commons.protocol.AirbyteMessageSerDeProvider
import io.airbyte.commons.protocol.AirbyteProtocolVersionedMigratorFactory
import io.airbyte.commons.version.AirbyteProtocolVersion
import io.airbyte.config.ActorType
import io.airbyte.config.ConnectorJobOutput
import io.airbyte.config.FailureReason
import io.airbyte.config.StandardCheckConnectionInput
import io.airbyte.config.StandardCheckConnectionOutput
import io.airbyte.persistence.job.models.IntegrationLauncherConfig
import io.airbyte.protocol.models.ConfiguredAirbyteCatalog
import io.airbyte.workers.helper.GsonPksExtractor
import io.airbyte.workers.internal.AirbyteStreamFactory
import io.airbyte.workers.internal.VersionedAirbyteStreamFactory
import io.airbyte.workers.internal.VersionedAirbyteStreamFactory.InvalidLineFailureConfiguration
import io.airbyte.workers.models.SidecarInput
import io.airbyte.workers.sync.OrchestratorConstants.EXIT_CODE_FILE
import io.airbyte.workers.sync.OrchestratorConstants.JOB_OUTPUT_FILENAME
import io.airbyte.workers.sync.OrchestratorConstants.SIDECAR_INPUT
import io.airbyte.workers.workload.JobOutputDocStore
import io.airbyte.workload.api.client.generated.WorkloadApi
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
  val workloadApi: WorkloadApi,
  val jobOutputDocStore: JobOutputDocStore,
) {
  fun run() {
    val input = Jsons.deserialize(readFile(SIDECAR_INPUT), SidecarInput::class.java)
    val checkConnectionConfiguration = input.checkConnectionInput
    val discoverCatalogInput = input.discoverCatalogInput
    val workloadId = input.workloadId
    val integrationLauncherConfig = input.integrationLauncherConfig
    try {
      val stopwatch: Stopwatch = Stopwatch.createStarted()
      while (!areNeededFilesPresent()) {
        Thread.sleep(100)
        if (fileTimeoutReach(stopwatch)) {
          failWorkload(workloadId, null)
          exitFileNotFound()
          // The return is needed for the test
          return
        }
      }
      val outputIS =
        if (Files.exists(Path.of(JOB_OUTPUT_FILENAME))) {
          Files.newInputStream(Path.of(JOB_OUTPUT_FILENAME))
        } else {
          InputStream.nullInputStream()
        }
      val exitCode = readFile(EXIT_CODE_FILE).trim().toInt()
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
              SidecarInput.OperationType.CHECK,
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
              SidecarInput.OperationType.DISCOVER,
            )
          }
        }
      jobOutputDocStore.write(workloadId, connectorOutput)
      workloadApi.workloadSuccess(WorkloadSuccessRequest(workloadId))
    } catch (e: Exception) {
      val output = getFailedOutput(checkConnectionConfiguration, e)

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
    return Files.exists(outputPath) && Files.exists(Path.of(configDir, EXIT_CODE_FILE))
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
      Optional.empty<Class<out RuntimeException?>>(),
      InvalidLineFailureConfiguration(false, false, false),
      gsonPksExtractor,
    )
  }

  @VisibleForTesting
  fun exitProperly() {
    exitProcess(0)
  }

  @VisibleForTesting
  fun exitInternalError() {
    exitProcess(1)
  }

  @VisibleForTesting
  fun exitFileNotFound() {
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

  private fun failWorkload(
    workloadId: String,
    failureReason: FailureReason?,
  ) {
    if (failureReason != null) {
      workloadApi.workloadFailure(
        WorkloadFailureRequest(
          workloadId,
          failureReason.failureOrigin.value(),
          failureReason.externalMessage,
        ),
      )
    } else {
      workloadApi.workloadFailure(WorkloadFailureRequest(workloadId))
    }
  }
}
