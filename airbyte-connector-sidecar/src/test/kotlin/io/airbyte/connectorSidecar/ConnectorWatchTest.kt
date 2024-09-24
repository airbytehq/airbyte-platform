package io.airbyte.connectorSidecar

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
import io.airbyte.workers.pod.FileConstants
import io.airbyte.workers.workload.JobOutputDocStore
import io.airbyte.workload.api.client.WorkloadApiClient
import io.airbyte.workload.api.client.generated.WorkloadApi
import io.airbyte.workload.api.client.model.generated.WorkloadFailureRequest
import io.airbyte.workload.api.client.model.generated.WorkloadSuccessRequest
import io.mockk.MockKException
import io.mockk.Runs
import io.mockk.every
import io.mockk.impl.annotations.MockK
import io.mockk.junit5.MockKExtension
import io.mockk.just
import io.mockk.spyk
import io.mockk.verify
import io.mockk.verifyOrder
import io.mockk.verifySequence
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.assertThrows
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
  private lateinit var logContextFactory: SidecarLogContextFactory

  @MockK
  private lateinit var streamFactory: AirbyteStreamFactory

  @MockK
  private lateinit var heartbeatMonitor: HeartbeatMonitor

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
          fileTimeoutMinutesWithinSync = 43,
          connectorMessageProcessor,
          serDeProvider,
          airbyteProtocolVersionedMigratorFactory,
          gsonPksExtractor,
          workloadApiClient,
          jobOutputDocStore,
          logContextFactory,
          heartbeatMonitor,
        ),
      )

    every { connectorWatcher.readFile(FileConstants.EXIT_CODE_FILE) } returns "0"

    every { connectorWatcher.areNeededFilesPresent() } returns true

    every { connectorWatcher.getStreamFactory(any()) } returns streamFactory

    every { connectorWatcher.exitProperly() } returns Unit

    every { connectorWatcher.exitInternalError() } returns Unit

    every { jobOutputDocStore.write(any(), any()) } returns Unit

    every { logContextFactory.create(any()) } returns mapOf()

    every { workloadApi.workloadHeartbeat(any()) } just Runs

    every { heartbeatMonitor.startHeartbeatThread(any()) } just Runs

    every { heartbeatMonitor.stopHeartbeatThread() } just Runs

    every { heartbeatMonitor.shouldAbort() } returns false
  }

  @ParameterizedTest
  @EnumSource(OperationType::class)
  fun `run for successful check`(operationType: OperationType) {
    val output =
      ConnectorJobOutput()
        .withCheckConnection(StandardCheckConnectionOutput().withStatus(StandardCheckConnectionOutput.Status.SUCCEEDED))

    every { connectorWatcher.readFile(FileConstants.SIDECAR_INPUT_FILE) } returns
      Jsons.serialize(SidecarInput(checkInput, discoveryInput, workloadId, IntegrationLauncherConfig(), operationType, ""))

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

    every { connectorWatcher.readFile(FileConstants.SIDECAR_INPUT_FILE) } returns
      Jsons.serialize(SidecarInput(checkInput, discoveryInput, workloadId, IntegrationLauncherConfig(), operationType, ""))

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

    every { connectorWatcher.readFile(FileConstants.SIDECAR_INPUT_FILE) } returns
      Jsons.serialize(SidecarInput(checkInput, discoveryInput, workloadId, IntegrationLauncherConfig().withDockerImage(""), operationType, ""))

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
    every { connectorWatcher.readFile(FileConstants.SIDECAR_INPUT_FILE) } returns
      Jsons.serialize(SidecarInput(checkInput, discoveryInput, workloadId, IntegrationLauncherConfig(), operationType, ""))
    var exitCauseFileWasNotFound = false
    every { connectorWatcher.areNeededFilesPresent() } returns false

    every { connectorWatcher.hasFileTimeoutReached(any(), any()) } returns true
    every { connectorWatcher.handleException(any(), any()) } just Runs

    every { connectorWatcher.exitFileNotFound() } answers {
      exitCauseFileWasNotFound = true
      throw RuntimeException("")
    }

    every { workloadApi.workloadFailure(any()) } returns Unit

    connectorWatcher.run()

    assertTrue(exitCauseFileWasNotFound)
  }

  @ParameterizedTest
  @EnumSource(OperationType::class)
  fun `should start and stop heartbeat monitor correctly`(operationType: OperationType) {
    val output =
      ConnectorJobOutput()
        .withCheckConnection(StandardCheckConnectionOutput().withStatus(StandardCheckConnectionOutput.Status.SUCCEEDED))

    every { connectorWatcher.readFile(FileConstants.SIDECAR_INPUT_FILE) } returns
      Jsons.serialize(SidecarInput(checkInput, discoveryInput, workloadId, IntegrationLauncherConfig(), operationType, ""))

    every { connectorMessageProcessor.run(any(), any(), any(), any(), eq(operationType)) } returns output

    every { workloadApi.workloadSuccess(WorkloadSuccessRequest(workloadId)) } returns Unit

    connectorWatcher.run()

    verifySequence {
      heartbeatMonitor.startHeartbeatThread(any())
      heartbeatMonitor.shouldAbort()
      heartbeatMonitor.stopHeartbeatThread()
    }
  }

  @ParameterizedTest
  @EnumSource(OperationType::class)
  fun `should handle heartbeat abort during waitForConnectorOutput`(operationType: OperationType) {
    val output =
      ConnectorJobOutput()
        .withCheckConnection(StandardCheckConnectionOutput().withStatus(StandardCheckConnectionOutput.Status.SUCCEEDED))

    val integrationLauncherConfig = spyk(IntegrationLauncherConfig())

    every { integrationLauncherConfig.dockerImage } returns ""

    every { connectorWatcher.readFile(FileConstants.SIDECAR_INPUT_FILE) } returns
      Jsons.serialize(SidecarInput(checkInput, discoveryInput, workloadId, integrationLauncherConfig, operationType, ""))

    every { connectorMessageProcessor.run(any(), any(), any(), any(), eq(operationType)) } returns output

    every { workloadApi.workloadSuccess(WorkloadSuccessRequest(workloadId)) } returns Unit

    every { heartbeatMonitor.shouldAbort() } returns true

    every { connectorWatcher.exitInternalError() } throws MockKException("")

    val exception =
      assertThrows<MockKException> {
        connectorWatcher.run()
      }

    verify {
      heartbeatMonitor.startHeartbeatThread(any())
      heartbeatMonitor.shouldAbort()
      heartbeatMonitor.stopHeartbeatThread()
    }
  }
}
