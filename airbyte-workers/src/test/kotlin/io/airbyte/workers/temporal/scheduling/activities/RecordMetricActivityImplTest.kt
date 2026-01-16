/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.scheduling.activities

import io.airbyte.api.client.AirbyteApiClient
import io.airbyte.api.client.generated.WorkspaceApi
import io.airbyte.api.client.model.generated.ConnectionIdRequestBody
import io.airbyte.api.client.model.generated.WorkspaceRead
import io.airbyte.commons.temporal.scheduling.ConnectionUpdaterInput
import io.airbyte.metrics.MetricAttribute
import io.airbyte.metrics.MetricClient
import io.airbyte.metrics.OssMetricsRegistry
import io.airbyte.metrics.lib.MetricTags
import io.airbyte.workers.temporal.scheduling.activities.RecordMetricActivity.FailureCause
import io.airbyte.workers.temporal.scheduling.activities.RecordMetricActivity.RecordMetricInput
import io.micronaut.http.HttpStatus
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.openapitools.client.infrastructure.ClientException
import java.util.Optional
import java.util.UUID

/**
 * Test suite for the [RecordMetricActivityImpl] class.
 */
internal class RecordMetricActivityImplTest {
  private lateinit var airbyteApiClient: AirbyteApiClient
  private lateinit var metricClient: MetricClient
  private lateinit var connectionUpdaterInput: ConnectionUpdaterInput
  private lateinit var activity: RecordMetricActivityImpl

  @BeforeEach
  fun setup() {
    airbyteApiClient = mockk()
    metricClient = mockk(relaxed = true)
    connectionUpdaterInput = mockk(relaxed = true)

    val workspaceApi = mockk<WorkspaceApi>()

    every { connectionUpdaterInput.connectionId } returns CONNECTION_ID
    every { workspaceApi.getWorkspaceByConnectionId(ConnectionIdRequestBody(CONNECTION_ID)) } returns
      WorkspaceRead(
        WORKSPACE_ID,
        UUID.randomUUID(),
        "name",
        "slug",
        false,
        UUID.randomUUID(),
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
      )
    every { workspaceApi.getWorkspaceByConnectionId(ConnectionIdRequestBody(CONNECTION_ID_WITHOUT_WORKSPACE)) } throws
      ClientException("Not Found", HttpStatus.NOT_FOUND.code, null)
    every { airbyteApiClient.workspaceApi } returns workspaceApi

    activity = RecordMetricActivityImpl(airbyteApiClient, metricClient)
  }

  @Test
  fun testRecordingMetricCounter() {
    val metricInput = RecordMetricInput(connectionUpdaterInput, Optional.empty<FailureCause>(), METRIC_NAME, null)

    activity.recordWorkflowCountMetric(metricInput)

    verify {
      metricClient.count(
        METRIC_NAME,
        1L,
        MetricAttribute(MetricTags.CONNECTION_ID, CONNECTION_ID.toString()),
        MetricAttribute(MetricTags.WORKSPACE_ID, WORKSPACE_ID.toString()),
      )
    }
  }

  @Test
  fun testRecordingMetricCounterWithAdditionalAttributes() {
    val additionalAttribute = MetricAttribute(MetricTags.JOB_STATUS, "test")
    val metricInput =
      RecordMetricInput(connectionUpdaterInput, Optional.empty<FailureCause>(), METRIC_NAME, arrayOf(additionalAttribute))

    activity.recordWorkflowCountMetric(metricInput)

    verify {
      metricClient.count(
        METRIC_NAME,
        1L,
        MetricAttribute(MetricTags.CONNECTION_ID, CONNECTION_ID.toString()),
        MetricAttribute(MetricTags.WORKSPACE_ID, WORKSPACE_ID.toString()),
        additionalAttribute,
      )
    }
  }

  @Test
  fun testRecordingMetricCounterWithFailureCause() {
    val failureCause = FailureCause.CANCELED
    val metricInput = RecordMetricInput(connectionUpdaterInput, Optional.of<FailureCause>(failureCause), METRIC_NAME, null)

    activity.recordWorkflowCountMetric(metricInput)

    verify {
      metricClient.count(
        METRIC_NAME,
        1L,
        MetricAttribute(MetricTags.CONNECTION_ID, CONNECTION_ID.toString()),
        MetricAttribute(MetricTags.WORKSPACE_ID, WORKSPACE_ID.toString()),
        MetricAttribute(MetricTags.FAILURE_CAUSE, failureCause.name),
      )
    }
  }

  @Test
  fun testRecordingMetricCounterDoesntCrashOnApiNotFoundErrors() {
    val inputForUnknownWorkspaceId =
      ConnectionUpdaterInput(
        connectionId = CONNECTION_ID_WITHOUT_WORKSPACE,
        jobId = null,
        attemptId = null,
        fromFailure = false,
        attemptNumber = null,
        workflowState = null,
        resetConnection = false,
        fromJobResetFailure = false,
        skipScheduling = false,
      )
    val metricInput = RecordMetricInput(inputForUnknownWorkspaceId, Optional.empty<FailureCause>(), METRIC_NAME, null)

    activity.recordWorkflowCountMetric(metricInput)

    verify {
      metricClient.count(
        METRIC_NAME,
        1L,
        MetricAttribute(MetricTags.CONNECTION_ID, CONNECTION_ID_WITHOUT_WORKSPACE.toString()),
      )
    }
  }

  companion object {
    private val CONNECTION_ID: UUID = UUID.randomUUID()
    private val CONNECTION_ID_WITHOUT_WORKSPACE: UUID = UUID.randomUUID()
    private val WORKSPACE_ID: UUID = UUID.randomUUID()
    private val METRIC_NAME = OssMetricsRegistry.TEMPORAL_WORKFLOW_ATTEMPT
  }
}
