/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.workflows

import io.airbyte.commons.temporal.TemporalConstants
import io.airbyte.commons.temporal.WorkflowClientWrapped
import io.airbyte.commons.temporal.scheduling.CheckCommandInput
import io.airbyte.commons.temporal.scheduling.ConnectorCommandWorkflow
import io.airbyte.config.ConnectorJobOutput
import io.airbyte.config.StandardCheckConnectionInput
import io.airbyte.micronaut.temporal.TemporalProxyHelper
import io.airbyte.persistence.job.models.IntegrationLauncherConfig
import io.airbyte.persistence.job.models.JobRunConfig
import io.airbyte.workers.commands.CheckCommand
import io.airbyte.workers.commands.CheckCommandThroughApi
import io.airbyte.workers.commands.DiscoverCommand
import io.airbyte.workers.commands.DiscoverCommandV2
import io.airbyte.workers.commands.ReplicationCommand
import io.airbyte.workers.commands.SpecCommand
import io.micronaut.context.BeanRegistration
import io.mockk.every
import io.mockk.mockk
import io.temporal.activity.ActivityCancellationType
import io.temporal.activity.ActivityOptions
import io.temporal.client.WorkflowClient
import io.temporal.client.WorkflowOptions
import io.temporal.common.RetryOptions
import io.temporal.testing.TestWorkflowEnvironment
import io.temporal.worker.Worker
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import kotlin.time.Duration.Companion.seconds
import kotlin.time.toJavaDuration

class ConnectorCommandWorkflowTest {
  companion object {
    const val QUEUE_NAME = "connector_command_queue"

    lateinit var checkCommand: CheckCommand
    lateinit var checkCommandThroughApi: CheckCommandThroughApi
    lateinit var discoverCommand: DiscoverCommand
    lateinit var discoverCommandV2: DiscoverCommandV2
    lateinit var specCommand: SpecCommand
    lateinit var replicationCommand: ReplicationCommand
    lateinit var activityExecutionContextProvider: ActivityExecutionContextProvider
    lateinit var connectorCommandActivity: ConnectorCommandActivity

    lateinit var testEnv: TestWorkflowEnvironment
    lateinit var worker: Worker
    lateinit var client: WorkflowClient
    lateinit var temporalProxyHelper: TemporalProxyHelper

    @JvmStatic
    @BeforeAll
    fun setup() {
      val shortActivityOptions =
        ActivityOptions
          .newBuilder()
          .setStartToCloseTimeout(10.seconds.toJavaDuration())
          .setCancellationType(ActivityCancellationType.WAIT_CANCELLATION_COMPLETED)
          .setRetryOptions(RetryOptions.newBuilder().setMaximumAttempts(1).build())
          .setHeartbeatTimeout(TemporalConstants.HEARTBEAT_TIMEOUT)
          .build()
      val activityOptionsBeanRegistration: BeanRegistration<ActivityOptions> =
        mockk(relaxed = true) {
          every { identifier } returns
            mockk {
              every { name } returns "shortActivityOptions"
            }
          every { bean } returns shortActivityOptions
        }
      temporalProxyHelper = TemporalProxyHelper(listOf(activityOptionsBeanRegistration))

      testEnv = TestWorkflowEnvironment.newInstance()
      worker = testEnv.newWorker(QUEUE_NAME)
      worker.registerWorkflowImplementationTypes(temporalProxyHelper.proxyWorkflowClass(ConnectorCommandWorkflowImpl::class.java))
      client = testEnv.workflowClient

      checkCommand = mockk()
      checkCommandThroughApi = mockk()
      discoverCommand = mockk()
      discoverCommandV2 = mockk()
      specCommand = mockk()
      replicationCommand = mockk()
      activityExecutionContextProvider = ActivityExecutionContextProvider()
      connectorCommandActivity =
        ConnectorCommandActivityImpl(
          checkCommand,
          checkCommandThroughApi,
          discoverCommand,
          discoverCommandV2,
          specCommand,
          replicationCommand,
          activityExecutionContextProvider,
          mockk(relaxed = true),
        )
      worker.registerActivitiesImplementations(connectorCommandActivity)
      testEnv.start()
    }

    @JvmStatic
    @AfterAll
    fun teardown() {
      testEnv.close()
    }
  }

  @Test
  fun `testing check`() {
    val workflowClient = WorkflowClientWrapped(client, mockk(relaxed = true))
    val workloadId = "test workload"
    val output = ConnectorJobOutput().withOutputType(ConnectorJobOutput.OutputType.CHECK_CONNECTION)

    every { checkCommand.start(any(), any()) } returns workloadId
    every { checkCommand.isTerminal(workloadId) } returns false andThen false andThen true
    every { checkCommand.getOutput(workloadId) } returns output

    val input =
      CheckCommandInput(
        input =
          CheckCommandInput.CheckConnectionInput(
            jobRunConfig = JobRunConfig(),
            integrationLauncherConfig = IntegrationLauncherConfig(),
            checkConnectionInput = StandardCheckConnectionInput(),
          ),
      )

    val result =
      workflowClient
        .newWorkflowStub(ConnectorCommandWorkflow::class.java, WorkflowOptions.newBuilder().setTaskQueue(QUEUE_NAME).build())
        .run(input)
    assertEquals(output, result)
  }
}
