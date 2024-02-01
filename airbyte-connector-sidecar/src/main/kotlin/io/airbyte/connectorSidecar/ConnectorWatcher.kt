package io.airbyte.connectorSidecar

import com.google.common.annotations.VisibleForTesting
import io.airbyte.commons.json.Jsons
import io.airbyte.commons.protocol.AirbyteMessageSerDeProvider
import io.airbyte.commons.protocol.AirbyteProtocolVersionedMigratorFactory
import io.airbyte.commons.version.AirbyteProtocolVersion
import io.airbyte.config.ActorType
import io.airbyte.config.ConnectorJobOutput
import io.airbyte.config.FailureReason
import io.airbyte.config.StandardCheckConnectionInput
import io.airbyte.config.StandardCheckConnectionOutput
import io.airbyte.protocol.models.ConfiguredAirbyteCatalog
import io.airbyte.workers.helper.GsonPksExtractor
import io.airbyte.workers.internal.AirbyteStreamFactory
import io.airbyte.workers.internal.VersionedAirbyteStreamFactory
import io.airbyte.workers.internal.VersionedAirbyteStreamFactory.InvalidLineFailureConfiguration
import io.airbyte.workers.sync.OrchestratorConstants.CHECK_JOB_OUTPUT_FILENAME
import io.airbyte.workers.sync.OrchestratorConstants.CONNECTION_INPUT
import io.airbyte.workers.sync.OrchestratorConstants.EXIT_CODE_FILE
import io.airbyte.workers.sync.OrchestratorConstants.WORKLOAD_ID_FILE
import io.airbyte.workers.workload.JobOutputDocStore
import io.airbyte.workload.api.client.generated.WorkloadApi
import io.airbyte.workload.api.client.model.generated.WorkloadFailureRequest
import io.airbyte.workload.api.client.model.generated.WorkloadHeartbeatRequest
import io.airbyte.workload.api.client.model.generated.WorkloadSuccessRequest
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Named
import jakarta.inject.Singleton
import java.io.InputStream
import java.nio.file.Files
import java.nio.file.Path
import java.util.Optional
import java.util.UUID
import kotlin.system.exitProcess

private val logger = KotlinLogging.logger {}

@Singleton
class ConnectorWatcher(
  @Named("output") val outputPath: Path,
  @Named("configDir") val configDir: String,
  val connectorMessageProcessor: ConnectorMessageProcessor,
  val serDeProvider: AirbyteMessageSerDeProvider,
  val airbyteProtocolVersionedMigratorFactory: AirbyteProtocolVersionedMigratorFactory,
  val gsonPksExtractor: GsonPksExtractor,
  val workloadApi: WorkloadApi,
  val jobOutputDocStore: JobOutputDocStore,
) {
  fun run() {
    val connectionConfiguration = Jsons.deserialize(readFile(CONNECTION_INPUT), StandardCheckConnectionInput::class.java)
    val workloadId = readFile(WORKLOAD_ID_FILE)

    try {
      workloadApi.workloadHeartbeat(WorkloadHeartbeatRequest(workloadId))

      while (!areNeededFilesPresent()) {
        Thread.sleep(100)
      }

      val outputIS =
        if (Files.exists(Path.of(CHECK_JOB_OUTPUT_FILENAME))) {
          Files.newInputStream(Path.of(CHECK_JOB_OUTPUT_FILENAME))
        } else {
          InputStream.nullInputStream()
        }

      val exitCode = readFile(EXIT_CODE_FILE).trim().toInt()

      val streamFactory: AirbyteStreamFactory = getStreamFactory()

      val connectorOutput: ConnectorJobOutput = connectorMessageProcessor.runCheck(outputIS, streamFactory, connectionConfiguration, exitCode)

      jobOutputDocStore.write(workloadId, connectorOutput)

      if (connectorOutput.checkConnection.status == StandardCheckConnectionOutput.Status.SUCCEEDED) {
        workloadApi.workloadSuccess(WorkloadSuccessRequest(workloadId))
      } else {
        failWorkload(workloadId, null)
      }
    } catch (e: Exception) {
      logger.error { e }
      val output = getFailedOutput(connectionConfiguration, e)

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
  fun getStreamFactory(): AirbyteStreamFactory {
    return VersionedAirbyteStreamFactory<Any>(
      serDeProvider,
      airbyteProtocolVersionedMigratorFactory,
      // TODO: Protocol version from launcher
      AirbyteProtocolVersion.DEFAULT_AIRBYTE_PROTOCOL_VERSION,
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
