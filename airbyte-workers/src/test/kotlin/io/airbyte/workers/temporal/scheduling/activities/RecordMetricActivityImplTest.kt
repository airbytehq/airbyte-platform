/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
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
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.mockito.Mock
import org.mockito.junit.jupiter.MockitoExtension
import org.mockito.junit.jupiter.MockitoSettings
import org.mockito.kotlin.eq
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import org.mockito.quality.Strictness
import org.openapitools.client.infrastructure.ClientException
import java.io.IOException
import java.util.Optional
import java.util.UUID

/**
 * Test suite for the [RecordMetricActivityImpl] class.
 */
@ExtendWith(MockitoExtension::class)
@MockitoSettings(strictness = Strictness.LENIENT)
internal class RecordMetricActivityImplTest {
  @Mock
  private lateinit var airbyteApiClient: AirbyteApiClient

  @Mock
  private lateinit var metricClient: MetricClient

  @Mock
  private lateinit var connectionUpdaterInput: ConnectionUpdaterInput

  private lateinit var activity: RecordMetricActivityImpl

  @BeforeEach
  @Throws(IOException::class)
  fun setup() {
    val workspaceApi = org.mockito.Mockito.mock(WorkspaceApi::class.java)

    whenever(connectionUpdaterInput.connectionId).thenReturn(CONNECTION_ID)
    whenever(workspaceApi.getWorkspaceByConnectionId(ConnectionIdRequestBody(CONNECTION_ID)))
      .thenReturn(
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
        ),
      )
    whenever(workspaceApi.getWorkspaceByConnectionId(ConnectionIdRequestBody(CONNECTION_ID_WITHOUT_WORKSPACE)))
      .thenThrow(ClientException("Not Found", HttpStatus.NOT_FOUND.code, null))
    whenever(airbyteApiClient.workspaceApi).thenReturn(workspaceApi)

    activity = RecordMetricActivityImpl(airbyteApiClient, metricClient)
  }

  @Test
  fun testRecordingMetricCounter() {
    val metricInput = RecordMetricInput(connectionUpdaterInput, Optional.empty<FailureCause>(), METRIC_NAME, null)

    activity.recordWorkflowCountMetric(metricInput)

    verify(metricClient).count(
      eq(METRIC_NAME),
      eq(1L),
      eq(MetricAttribute(MetricTags.CONNECTION_ID, CONNECTION_ID.toString())),
      eq(MetricAttribute(MetricTags.WORKSPACE_ID, WORKSPACE_ID.toString())),
    )
  }

  @Test
  fun testRecordingMetricCounterWithAdditionalAttributes() {
    val additionalAttribute = MetricAttribute(MetricTags.JOB_STATUS, "test")
    val metricInput =
      RecordMetricInput(connectionUpdaterInput, Optional.empty<FailureCause>(), METRIC_NAME, arrayOf<MetricAttribute>(additionalAttribute))

    activity.recordWorkflowCountMetric(metricInput)

    verify(metricClient).count(
      eq(METRIC_NAME),
      eq(1L),
      eq(MetricAttribute(MetricTags.CONNECTION_ID, CONNECTION_ID.toString())),
      eq(MetricAttribute(MetricTags.WORKSPACE_ID, WORKSPACE_ID.toString())),
      eq(additionalAttribute),
    )
  }

  @Test
  fun testRecordingMetricCounterWithFailureCause() {
    val failureCause = FailureCause.CANCELED
    val metricInput = RecordMetricInput(connectionUpdaterInput, Optional.of<FailureCause>(failureCause), METRIC_NAME, null)

    activity.recordWorkflowCountMetric(metricInput)

    verify(metricClient).count(
      eq(METRIC_NAME),
      eq(1L),
      eq(MetricAttribute(MetricTags.CONNECTION_ID, CONNECTION_ID.toString())),
      eq(MetricAttribute(MetricTags.WORKSPACE_ID, WORKSPACE_ID.toString())),
      eq(MetricAttribute(MetricTags.FAILURE_CAUSE, failureCause.name)),
    )
  }

  @Test
  fun testRecordingMetricCounterDoesntCrashOnApiNotFoundErrors() {
    val inputForUnkwnownWorkspaceId =
      ConnectionUpdaterInput(
        CONNECTION_ID_WITHOUT_WORKSPACE,
        null,
        null,
        false,
        null,
        null,
        false,
        false,
        false,
      )
    val metricInput = RecordMetricInput(inputForUnkwnownWorkspaceId, Optional.empty<FailureCause>(), METRIC_NAME, null)

    activity.recordWorkflowCountMetric(metricInput)

    verify(metricClient).count(
      eq(METRIC_NAME),
      eq(1L),
      eq(MetricAttribute(MetricTags.CONNECTION_ID, CONNECTION_ID_WITHOUT_WORKSPACE.toString())),
    )
  }

  companion object {
    private val CONNECTION_ID: UUID = UUID.randomUUID()
    private val CONNECTION_ID_WITHOUT_WORKSPACE: UUID = UUID.randomUUID()
    private val WORKSPACE_ID: UUID = UUID.randomUUID()
    private val METRIC_NAME = OssMetricsRegistry.TEMPORAL_WORKFLOW_ATTEMPT
  }
}
