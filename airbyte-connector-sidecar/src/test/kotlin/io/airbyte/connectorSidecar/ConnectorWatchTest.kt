package io.airbyte.connectorSidecar

import io.airbyte.api.client.WorkloadApiClient
import io.airbyte.commons.protocol.AirbyteMessageSerDeProvider
import io.airbyte.commons.protocol.AirbyteProtocolVersionedMigratorFactory
import io.airbyte.config.ActorType
import io.airbyte.config.ConnectorJobOutput
import io.airbyte.config.StandardCheckConnectionInput
import io.airbyte.config.StandardCheckConnectionOutput
import io.airbyte.config.StandardDiscoverCatalogInput
import io.airbyte.persistence.job.models.IntegrationLauncherConfig
import io.airbyte.protocol.models.Jsons
import io.airbyte.workers.exception.WorkerException
import io.airbyte.workers.helper.GsonPksExtractor
import io.airbyte.workers.internal.AirbyteStreamFactory
import io.airbyte.workers.models.SidecarInput
import io.airbyte.workers.models.SidecarInput.OperationType
import io.airbyte.workers.sync.OrchestratorConstants
import io.airbyte.workers.workload.JobOutputDocStore
import io.airbyte.workload.api.client.generated.WorkloadApi
import io.airbyte.workload.api.client.model.generated.WorkloadFailureRequest
import io.airbyte.workload.api.client.model.generated.WorkloadSuccessRequest
import io.mockk.every
import io.mockk.impl.annotations.MockK
import io.mockk.junit5.MockKExtension
import io.mockk.spyk
import io.mockk.verifyOrder
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.extension.ExtendWith
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.EnumSource
import java.nio.file.Path

@ExtendWith(MockKExtension::class)
class ConnectorWatchTest {
  val outputPath = Path.of("output")
  val configDir = "config"

  @MockK
  private lateinit var connectorMessageProcessor: ConnectorMessageProcessor

  @MockK
  private lateinit var serDeProvider: AirbyteMessageSerDeProvider

  @MockK
  private lateinit var airbyteProtocolVersionedMigratorFactory: AirbyteProtocolVersionedMigratorFactory

  @MockK
  private lateinit var gsonPksExtractor: GsonPksExtractor

  @MockK
  private lateinit var workloadApi: WorkloadApi

  @MockK
  private lateinit var workloadApiClient: WorkloadApiClient

  @MockK
  private lateinit var jobOutputDocStore: JobOutputDocStore

  @MockK
  private lateinit var streamFactory: AirbyteStreamFactory

  private lateinit var connectorWatcher: ConnectorWatcher

  val workloadId = "workloadId"

  val checkInput = StandardCheckConnectionInput().withActorType(ActorType.SOURCE)

  val discoveryInput = StandardDiscoverCatalogInput()

  @BeforeEach
  fun init() {
    every { workloadApiClient.workloadApi } returns workloadApi

    connectorWatcher =
      spyk(
        ConnectorWatcher(
          outputPath,
          configDir,
          fileTimeoutMinutes = 42,
          connectorMessageProcessor,
          serDeProvider,
          airbyteProtocolVersionedMigratorFactory,
          gsonPksExtractor,
          workloadApiClient,
          jobOutputDocStore,
        ),
      )

    every { connectorWatcher.readFile(OrchestratorConstants.EXIT_CODE_FILE) } returns "0"

    every { connectorWatcher.areNeededFilesPresent() } returns true

    every { connectorWatcher.getStreamFactory(any()) } returns streamFactory

    every { connectorWatcher.exitProperly() } returns Unit

    every { connectorWatcher.exitInternalError() } returns Unit

    every { jobOutputDocStore.write(any(), any()) } returns Unit
  }

  @ParameterizedTest
  @EnumSource(OperationType::class)
  fun `run for successful check`(operationType: OperationType) {
    val output =
      ConnectorJobOutput()
        .withCheckConnection(StandardCheckConnectionOutput().withStatus(StandardCheckConnectionOutput.Status.SUCCEEDED))

    every { connectorWatcher.readFile(OrchestratorConstants.SIDECAR_INPUT) } returns
      Jsons.serialize(SidecarInput(checkInput, discoveryInput, workloadId, IntegrationLauncherConfig(), operationType))

    every { connectorMessageProcessor.run(any(), any(), any(), any(), eq(operationType)) } returns output

    every { workloadApi.workloadSuccess(WorkloadSuccessRequest(workloadId)) } returns Unit

    connectorWatcher.run()

    verifyOrder {
      connectorMessageProcessor.run(any(), any(), any(), any(), eq(operationType))
      jobOutputDocStore.write(workloadId, output)
      workloadApi.workloadSuccess(WorkloadSuccessRequest(workloadId))
      connectorWatcher.exitProperly()
    }
  }

  @ParameterizedTest
  @EnumSource(OperationType::class)
  fun `run for failed check`(operationType: OperationType) {
    val output =
      ConnectorJobOutput()
        .withCheckConnection(StandardCheckConnectionOutput().withStatus(StandardCheckConnectionOutput.Status.FAILED))

    every { connectorWatcher.readFile(OrchestratorConstants.SIDECAR_INPUT) } returns
      Jsons.serialize(SidecarInput(checkInput, discoveryInput, workloadId, IntegrationLauncherConfig(), operationType))

    every { connectorMessageProcessor.run(any(), any(), any(), any(), eq(operationType)) } returns output

    every { workloadApi.workloadSuccess(WorkloadSuccessRequest(workloadId)) } returns Unit

    connectorWatcher.run()

    verifyOrder {
      connectorMessageProcessor.run(any(), any(), any(), any(), eq(operationType))
      jobOutputDocStore.write(workloadId, output)
      workloadApi.workloadSuccess(WorkloadSuccessRequest(workloadId))
      connectorWatcher.exitProperly()
    }
  }

  @ParameterizedTest
  @EnumSource(OperationType::class)
  fun `run for failed with exception check`(operationType: OperationType) {
    val exception = WorkerException("Broken check")
    val output =
      when (operationType) {
        OperationType.CHECK -> connectorWatcher.getFailedOutput(checkInput, exception)
        OperationType.DISCOVER -> connectorWatcher.getFailedOutput(discoveryInput, exception)
        OperationType.SPEC -> connectorWatcher.getFailedOutput("", exception)
      }

    every { connectorWatcher.readFile(OrchestratorConstants.SIDECAR_INPUT) } returns
      Jsons.serialize(SidecarInput(checkInput, discoveryInput, workloadId, IntegrationLauncherConfig().withDockerImage(""), operationType))

    every { connectorMessageProcessor.run(any(), any(), any(), any(), eq(operationType)) } throws exception

    every {
      workloadApi.workloadFailure(
        WorkloadFailureRequest(
          workloadId,
          output.failureReason.failureOrigin.value(),
          output.failureReason.externalMessage,
        ),
      )
    } returns Unit

    connectorWatcher.run()

    verifyOrder {
      connectorMessageProcessor.run(any(), any(), any(), any(), eq(operationType))
      jobOutputDocStore.write(workloadId, output)
      workloadApi.workloadFailure(
        WorkloadFailureRequest(workloadId, output.failureReason.failureOrigin.value(), output.failureReason.externalMessage),
      )
      connectorWatcher.exitInternalError()
    }
  }

  @ParameterizedTest
  @EnumSource(OperationType::class)
  fun `run for failed with file timeout`(operationType: OperationType) {
    every { connectorWatcher.readFile(OrchestratorConstants.SIDECAR_INPUT) } returns
      Jsons.serialize(SidecarInput(checkInput, discoveryInput, workloadId, IntegrationLauncherConfig(), operationType))

    every { connectorWatcher.areNeededFilesPresent() } returns false

    every { connectorWatcher.fileTimeoutReach(any()) } returns true

    every { connectorWatcher.exitFileNotFound() } returns Unit

    every { workloadApi.workloadFailure(any()) } returns Unit

    connectorWatcher.run()

    verifyOrder {
      workloadApi.workloadFailure(any())
      connectorWatcher.exitFileNotFound()
    }
  }
}
