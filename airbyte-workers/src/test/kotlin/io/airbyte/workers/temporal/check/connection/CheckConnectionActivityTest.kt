/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */
package io.airbyte.workers.temporal.check.connection

import io.airbyte.api.client.AirbyteApiClient
import io.airbyte.api.client.model.generated.Geography
import io.airbyte.commons.logging.DEFAULT_LOG_FILENAME
import io.airbyte.commons.logging.LogClientManager
import io.airbyte.config.ActorContext
import io.airbyte.config.ActorType
import io.airbyte.config.ConnectorJobOutput
import io.airbyte.config.StandardCheckConnectionInput
import io.airbyte.config.StandardCheckConnectionOutput
import io.airbyte.config.WorkloadPriority
import io.airbyte.featureflag.TestClient
import io.airbyte.featureflag.WorkloadCheckFrequencyInSeconds
import io.airbyte.persistence.job.models.IntegrationLauncherConfig
import io.airbyte.persistence.job.models.JobRunConfig
import io.airbyte.workers.models.CheckConnectionInput
import io.airbyte.workers.sync.WorkloadClient
import io.airbyte.workers.workload.WorkloadIdGenerator
import io.airbyte.workload.api.client.model.generated.WorkloadCreateRequest
import io.airbyte.workload.api.client.model.generated.WorkloadType
import io.mockk.CapturingSlot
import io.mockk.every
import io.mockk.mockk
import io.mockk.mockkStatic
import io.mockk.slot
import io.mockk.spyk
import io.mockk.verify
import io.temporal.activity.Activity
import io.temporal.activity.ActivityExecutionContext
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.nio.file.Path
import java.util.Optional
import java.util.UUID

class CheckConnectionActivityTest {
  private val workspaceRoot = Path.of("workspace-root")
  private val airbyteApiClient: AirbyteApiClient = mockk()
  private val workloadClient: WorkloadClient = mockk()
  private val workloadIdGenerator: WorkloadIdGenerator = mockk()
  private val featureFlagClient: TestClient = mockk()
  private val logClientManager: LogClientManager = mockk()
  private val executionContext: ActivityExecutionContext =
    mockk {
      every { heartbeat(null) } returns Unit
    }
  private lateinit var createReqSlot: CapturingSlot<WorkloadCreateRequest>
  private lateinit var checkConnectionActivity: CheckConnectionActivityImpl

  @BeforeEach
  fun init() {
    checkConnectionActivity =
      spyk(
        CheckConnectionActivityImpl(
          workspaceRoot,
          airbyteApiClient,
          featureFlagClient,
          workloadClient,
          workloadIdGenerator,
          mockk(relaxed = true),
          mockk(),
          logClientManager,
        ),
      )

    every { featureFlagClient.intVariation(WorkloadCheckFrequencyInSeconds, any()) } returns WORKLOAD_CHECK_FREQUENCY_IN_SECONDS
    every { workloadIdGenerator.generateCheckWorkloadId(ACTOR_DEFINITION_ID, JOB_ID, ATTEMPT_NUMBER_AS_INT) } returns WORKLOAD_ID
    every { checkConnectionActivity.getGeography(Optional.of(CONNECTION_ID), Optional.of(WORKSPACE_ID)) } returns Geography.US
    every { logClientManager.fullLogPath(any()) } answers { Path.of(invocation.args[0].toString(), DEFAULT_LOG_FILENAME).toString() }

    mockkStatic(Activity::class)

    every { Activity.getExecutionContext() } returns executionContext

    createReqSlot = slot<WorkloadCreateRequest>()
    every {
      workloadClient.runWorkloadWithCancellationHeartbeat(
        capture(createReqSlot),
        WORKLOAD_CHECK_FREQUENCY_IN_SECONDS,
        executionContext,
      )
    } returns Unit
  }

  @Test
  fun `runWithWorkload happy path`() {
    val input = checkInput
    every { workloadClient.getConnectorJobOutput(WORKLOAD_ID, any()) } returns
      ConnectorJobOutput()
        .withOutputType(ConnectorJobOutput.OutputType.CHECK_CONNECTION)
        .withCheckConnection(
          StandardCheckConnectionOutput()
            .withStatus(StandardCheckConnectionOutput.Status.SUCCEEDED),
        )

    val output = checkConnectionActivity.runWithWorkload(input)
    verify { workloadIdGenerator.generateCheckWorkloadId(ACTOR_DEFINITION_ID, JOB_ID, ATTEMPT_NUMBER_AS_INT) }
    verify { workloadClient.runWorkloadWithCancellationHeartbeat(any(), WORKLOAD_CHECK_FREQUENCY_IN_SECONDS, executionContext) }
    assertEquals(WORKLOAD_ID, createReqSlot.captured.workloadId)
    assertEquals(WorkloadType.CHECK, createReqSlot.captured.type)
    assertEquals(ConnectorJobOutput.OutputType.CHECK_CONNECTION, output.outputType)
    assertEquals(StandardCheckConnectionOutput.Status.SUCCEEDED, output.checkConnection.status)
  }

  @Test
  fun `runWithWorkload missing output`() {
    val input = checkInput
    every { workloadClient.getConnectorJobOutput(WORKLOAD_ID, any()) } returns
      ConnectorJobOutput().withOutputType(ConnectorJobOutput.OutputType.CHECK_CONNECTION)
        .withCheckConnection(
          StandardCheckConnectionOutput()
            .withStatus(StandardCheckConnectionOutput.Status.FAILED)
            .withMessage("missing output"),
        )
    val output = checkConnectionActivity.runWithWorkload(input)
    verify { workloadIdGenerator.generateCheckWorkloadId(ACTOR_DEFINITION_ID, JOB_ID, ATTEMPT_NUMBER_AS_INT) }
    assertEquals(WORKLOAD_ID, createReqSlot.captured.workloadId)
    assertEquals(WorkloadType.CHECK, createReqSlot.captured.type)
    assertEquals(ConnectorJobOutput.OutputType.CHECK_CONNECTION, output.outputType)
    assertEquals(StandardCheckConnectionOutput.Status.FAILED, output.checkConnection.status)
  }

  companion object {
    private val ACTOR_DEFINITION_ID = UUID.randomUUID()
    private const val ATTEMPT_NUMBER = 42L
    private val ATTEMPT_NUMBER_AS_INT = Math.toIntExact(ATTEMPT_NUMBER)
    private val CONNECTION_ID = UUID.randomUUID()
    private const val JOB_ID = "jobId"
    private const val WORKLOAD_ID = "workloadId"
    private val WORKSPACE_ID = UUID.randomUUID()
    private const val WORKLOAD_CHECK_FREQUENCY_IN_SECONDS = 10

    private val checkInput: CheckConnectionInput
      get() {
        val input = CheckConnectionInput()
        input.jobRunConfig = JobRunConfig().withJobId(JOB_ID).withAttemptId(ATTEMPT_NUMBER)
        input.checkConnectionInput =
          StandardCheckConnectionInput()
            .withActorType(ActorType.SOURCE)
            .withActorContext(
              ActorContext().withActorDefinitionId(ACTOR_DEFINITION_ID)
                .withWorkspaceId(WORKSPACE_ID),
            )
        input.launcherConfig = IntegrationLauncherConfig().withConnectionId(CONNECTION_ID).withPriority(WorkloadPriority.DEFAULT)
        return input
      }
  }
}
