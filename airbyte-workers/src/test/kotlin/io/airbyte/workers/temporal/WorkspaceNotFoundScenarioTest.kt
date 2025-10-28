/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal

import io.airbyte.api.client.AirbyteApiClient
import io.airbyte.api.client.generated.WorkspaceApi
import io.airbyte.commons.temporal.TemporalConstants
import io.airbyte.commons.temporal.WorkflowClientWrapped
import io.airbyte.commons.temporal.scheduling.ConnectorCommandWorkflow
import io.airbyte.commons.temporal.scheduling.DiscoverCommandInput
import io.airbyte.config.ConnectorJobOutput
import io.airbyte.config.StandardDiscoverCatalogInput
import io.airbyte.metrics.MetricClient
import io.airbyte.micronaut.temporal.TemporalProxyHelper
import io.airbyte.persistence.job.models.IntegrationLauncherConfig
import io.airbyte.persistence.job.models.JobRunConfig
import io.airbyte.workers.commands.CheckCommand
import io.airbyte.workers.commands.CheckCommandV2
import io.airbyte.workers.commands.DiscoverCommand
import io.airbyte.workers.commands.DiscoverCommandV2
import io.airbyte.workers.commands.ReplicationCommand
import io.airbyte.workers.commands.SpecCommand
import io.airbyte.workers.commands.SpecCommandV2
import io.airbyte.workers.temporal.workflows.ActivityExecutionContextProvider
import io.airbyte.workers.temporal.workflows.ConnectorCommandActivity
import io.airbyte.workers.temporal.workflows.ConnectorCommandActivityImpl
import io.airbyte.workers.temporal.workflows.ConnectorCommandWorkflowImpl
import io.airbyte.workers.workload.DataplaneGroupResolver
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
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.openapitools.client.infrastructure.ClientException
import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds
import kotlin.time.toJavaDuration

class WorkspaceNotFoundScenarioTest {
  companion object {
    const val QUEUE_NAME = "workspace_not_found_scenario_queue"

    lateinit var airbyteApiClient: AirbyteApiClient
    lateinit var workspaceApi: WorkspaceApi
    lateinit var dataplaneGroupResolver: DataplaneGroupResolver
    lateinit var checkCommand: CheckCommand
    lateinit var checkCommandThroughApi: CheckCommandV2
    lateinit var discoverCommand: DiscoverCommand
    lateinit var discoverCommandV2: DiscoverCommandV2
    lateinit var specCommand: SpecCommand
    lateinit var specCommandV2: SpecCommandV2
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
      // Setup API mocks
      airbyteApiClient = mockk()
      workspaceApi = mockk()
      every { airbyteApiClient.workspaceApi } returns workspaceApi

      // Setup resolver that will call the mocked API
      dataplaneGroupResolver = DataplaneGroupResolver(airbyteApiClient)

      // Setup activity options with no retry on WorkspaceNotFoundException
      val shortActivityOptions =
        ActivityOptions
          .newBuilder()
          .setStartToCloseTimeout(10.seconds.toJavaDuration())
          .setCancellationType(ActivityCancellationType.WAIT_CANCELLATION_COMPLETED)
          .setRetryOptions(
            RetryOptions
              .newBuilder()
              .setMaximumAttempts(3)
              .setDoNotRetry("io.airbyte.workers.workload.WorkspaceNotFoundException")
              .build(),
          ).setHeartbeatTimeout(TemporalConstants.HEARTBEAT_TIMEOUT)
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
      specCommandV2 = mockk()
      replicationCommand = mockk()
      activityExecutionContextProvider = ActivityExecutionContextProvider()
      connectorCommandActivity =
        ConnectorCommandActivityImpl(
          checkCommand,
          checkCommandThroughApi,
          discoverCommand,
          discoverCommandV2,
          specCommand,
          specCommandV2,
          replicationCommand,
          activityExecutionContextProvider,
          mockk<MetricClient>(relaxed = true),
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
  fun `discover workflow handles deleted workspace gracefully end-to-end`() {
    val workflowClient = WorkflowClientWrapped(client, mockk(relaxed = true))
    val workspaceId = UUID.randomUUID()

    // Mock discover command to use the real dataplaneGroupResolver which will hit the 404
    every { discoverCommand.start(any(), any()) } answers {
      // This will trigger the workspace lookup, which will throw WorkspaceNotFoundException
      dataplaneGroupResolver.resolveForDiscover(workspaceId, UUID.randomUUID())
      "workload-id"
    }
    every { discoverCommand.getAwaitDuration() } returns 1.minutes

    // Mock API to return 404
    every { workspaceApi.getWorkspace(any()) } throws ClientException("Not Found", 404)

    val input =
      DiscoverCommandInput(
        input =
          DiscoverCommandInput.DiscoverCatalogInput(
            jobRunConfig = JobRunConfig(),
            integrationLauncherConfig = IntegrationLauncherConfig(),
            discoverCatalogInput = StandardDiscoverCatalogInput(),
          ),
      )

    val output =
      workflowClient
        .newWorkflowStub(ConnectorCommandWorkflow::class.java, WorkflowOptions.newBuilder().setTaskQueue(QUEUE_NAME).build())
        .run(input)

    // Verify graceful handling
    assertNotNull(output)
    assertNotNull(output.failureReason)
    assertEquals(
      "WORKSPACE_NOT_FOUND",
      output.failureReason.metadata.additionalProperties["errorCode"],
    )
    assertFalse(output.failureReason.retryable)
  }

  @Test
  fun `activity does not retry on WorkspaceNotFoundException`() {
    val workflowClient = WorkflowClientWrapped(client, mockk(relaxed = true))
    val workspaceId = UUID.randomUUID()

    val attemptCount = AtomicInteger(0)
    every { discoverCommand.start(any(), any()) } answers {
      attemptCount.incrementAndGet()
      dataplaneGroupResolver.resolveForDiscover(workspaceId, UUID.randomUUID())
      "workload-id"
    }
    every { discoverCommand.getAwaitDuration() } returns 1.minutes

    // Mock API to return 404
    every { workspaceApi.getWorkspace(any()) } throws ClientException("Not Found", 404)

    val input =
      DiscoverCommandInput(
        input =
          DiscoverCommandInput.DiscoverCatalogInput(
            jobRunConfig = JobRunConfig(),
            integrationLauncherConfig = IntegrationLauncherConfig(),
            discoverCatalogInput = StandardDiscoverCatalogInput(),
          ),
      )

    workflowClient
      .newWorkflowStub(ConnectorCommandWorkflow::class.java, WorkflowOptions.newBuilder().setTaskQueue(QUEUE_NAME).build())
      .run(input)

    // Should only attempt once (no retries due to doNotRetry configuration)
    assertEquals(1, attemptCount.get())
  }

  @Test
  fun `transient errors still retry normally`() {
    val workflowClient = WorkflowClientWrapped(client, mockk(relaxed = true))
    val workspaceId = UUID.randomUUID()

    val attemptCount = AtomicInteger(0)
    every { discoverCommand.start(any(), any()) } answers {
      val count = attemptCount.incrementAndGet()
      if (count < 3) {
        // First 2 attempts throw 500 error (should retry)
        every { workspaceApi.getWorkspace(any()) } throws ClientException("Server Error", 500)
        dataplaneGroupResolver.resolveForDiscover(workspaceId, UUID.randomUUID())
      } else {
        // Third attempt succeeds
        every { workspaceApi.getWorkspace(any()) } returns
          mockk {
            every { dataplaneGroupId } returns UUID.randomUUID()
          }
        dataplaneGroupResolver.resolveForDiscover(workspaceId, UUID.randomUUID())
      }
      "workload-id"
    }
    every { discoverCommand.isTerminal(any()) } returns true
    every { discoverCommand.getOutput(any()) } returns
      ConnectorJobOutput().withOutputType(ConnectorJobOutput.OutputType.DISCOVER_CATALOG_ID)
    every { discoverCommand.getAwaitDuration() } returns 1.minutes

    val input =
      DiscoverCommandInput(
        input =
          DiscoverCommandInput.DiscoverCatalogInput(
            jobRunConfig = JobRunConfig(),
            integrationLauncherConfig = IntegrationLauncherConfig(),
            discoverCatalogInput = StandardDiscoverCatalogInput(),
          ),
      )

    val output =
      workflowClient
        .newWorkflowStub(ConnectorCommandWorkflow::class.java, WorkflowOptions.newBuilder().setTaskQueue(QUEUE_NAME).build())
        .run(input)

    // Should retry and eventually succeed
    assertEquals(3, attemptCount.get())
    assertNotNull(output)
  }
}
