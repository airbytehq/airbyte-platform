package io.airbyte.connectorSidecar

import io.airbyte.commons.protocol.AirbyteMessageSerDeProvider
import io.airbyte.commons.protocol.AirbyteProtocolVersionedMigratorFactory
import io.airbyte.config.ActorType
import io.airbyte.config.ConnectorJobOutput
import io.airbyte.config.StandardCheckConnectionInput
import io.airbyte.config.StandardCheckConnectionOutput
import io.airbyte.protocol.models.Jsons
import io.airbyte.workers.exception.WorkerException
import io.airbyte.workers.helper.GsonPksExtractor
import io.airbyte.workers.internal.AirbyteStreamFactory
import io.airbyte.workers.sync.OrchestratorConstants
import io.airbyte.workers.workload.JobOutputDocStore
import io.airbyte.workload.api.client.generated.WorkloadApi
import io.airbyte.workload.api.client.model.generated.WorkloadFailureRequest
import io.airbyte.workload.api.client.model.generated.WorkloadHeartbeatRequest
import io.airbyte.workload.api.client.model.generated.WorkloadSuccessRequest
import io.mockk.every
import io.mockk.impl.annotations.MockK
import io.mockk.junit5.MockKExtension
import io.mockk.spyk
import io.mockk.verifyOrder
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
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
  private lateinit var jobOutputDocStore: JobOutputDocStore

  @MockK
  private lateinit var streamFactory: AirbyteStreamFactory

  private lateinit var connectorWatcher: ConnectorWatcher

  val workloadId = "workloadId"

  val input = StandardCheckConnectionInput().withActorType(ActorType.SOURCE)

  @BeforeEach
  fun init() {
    connectorWatcher =
      spyk(
        ConnectorWatcher(
          outputPath,
          configDir,
          connectorMessageProcessor,
          serDeProvider,
          airbyteProtocolVersionedMigratorFactory,
          gsonPksExtractor,
          workloadApi,
          jobOutputDocStore,
        ),
      )

    every { connectorWatcher.readFile(OrchestratorConstants.CONNECTION_INPUT) } returns
      Jsons.serialize(input)

    every { connectorWatcher.readFile(OrchestratorConstants.WORKLOAD_ID_FILE) } returns workloadId

    every { connectorWatcher.readFile(OrchestratorConstants.EXIT_CODE_FILE) } returns "0"

    every { connectorWatcher.areNeededFilesPresent() } returns true

    every { connectorWatcher.getStreamFactory() } returns streamFactory

    every { connectorWatcher.exitProperly() } returns Unit

    every { connectorWatcher.exitInternalError() } returns Unit

    every { jobOutputDocStore.write(any(), any()) } returns Unit

    every { workloadApi.workloadHeartbeat(WorkloadHeartbeatRequest(workloadId)) } returns Unit
  }

  @Test
  fun `run for successful check`() {
    val output =
      ConnectorJobOutput()
        .withCheckConnection(StandardCheckConnectionOutput().withStatus(StandardCheckConnectionOutput.Status.SUCCEEDED))
    every { connectorMessageProcessor.runCheck(any(), any(), any(), any()) } returns output

    every { workloadApi.workloadSuccess(WorkloadSuccessRequest(workloadId)) } returns Unit

    connectorWatcher.run()

    verifyOrder {
      workloadApi.workloadHeartbeat(WorkloadHeartbeatRequest(workloadId))
      connectorMessageProcessor.runCheck(any(), any(), any(), any())
      jobOutputDocStore.write(workloadId, output)
      workloadApi.workloadSuccess(WorkloadSuccessRequest(workloadId))
      connectorWatcher.exitProperly()
    }
  }

  @Test
  fun `run for failed check`() {
    val output =
      ConnectorJobOutput()
        .withCheckConnection(StandardCheckConnectionOutput().withStatus(StandardCheckConnectionOutput.Status.FAILED))

    every { connectorMessageProcessor.runCheck(any(), any(), any(), any()) } returns output

    every { workloadApi.workloadFailure(WorkloadFailureRequest(workloadId)) } returns Unit

    connectorWatcher.run()

    verifyOrder {
      workloadApi.workloadHeartbeat(WorkloadHeartbeatRequest(workloadId))
      connectorMessageProcessor.runCheck(any(), any(), any(), any())
      jobOutputDocStore.write(workloadId, output)
      workloadApi.workloadFailure(WorkloadFailureRequest(workloadId))
      connectorWatcher.exitProperly()
    }
  }

  @Test
  fun `run for failed with exception check`() {
    val exception = WorkerException("Broken check")
    val output = connectorWatcher.getFailedOutput(input, exception)

    every { connectorMessageProcessor.runCheck(any(), any(), any(), any()) } throws exception

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
      workloadApi.workloadHeartbeat(WorkloadHeartbeatRequest(workloadId))
      connectorMessageProcessor.runCheck(any(), any(), any(), any())
      jobOutputDocStore.write(workloadId, output)
      workloadApi.workloadFailure(
        WorkloadFailureRequest(workloadId, output.failureReason.failureOrigin.value(), output.failureReason.externalMessage),
      )
      connectorWatcher.exitInternalError()
    }
  }
}
