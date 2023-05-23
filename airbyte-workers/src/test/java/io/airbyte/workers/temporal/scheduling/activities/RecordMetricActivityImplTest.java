/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.scheduling.activities;

import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.airbyte.api.client.generated.WorkspaceApi;
import io.airbyte.api.client.invoker.generated.ApiException;
import io.airbyte.api.client.model.generated.ConnectionIdRequestBody;
import io.airbyte.api.client.model.generated.WorkspaceRead;
import io.airbyte.commons.temporal.scheduling.ConnectionUpdaterInput;
import io.airbyte.metrics.lib.MetricAttribute;
import io.airbyte.metrics.lib.MetricClient;
import io.airbyte.metrics.lib.MetricTags;
import io.airbyte.metrics.lib.OssMetricsRegistry;
import io.airbyte.workers.temporal.scheduling.activities.RecordMetricActivity.FailureCause;
import io.airbyte.workers.temporal.scheduling.activities.RecordMetricActivity.RecordMetricInput;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Test suite for the {@link RecordMetricActivityImpl} class.
 */
class RecordMetricActivityImplTest {

  private static final UUID CONNECTION_ID = UUID.randomUUID();
  private static final UUID CONNECTION_ID_WITHOUT_WORKSPACE = UUID.randomUUID();
  private static final UUID WORKSPACE_ID = UUID.randomUUID();
  private static final OssMetricsRegistry METRIC_NAME = OssMetricsRegistry.TEMPORAL_WORKFLOW_ATTEMPT;

  private MetricClient metricClient;
  private ConnectionUpdaterInput connectionUpdaterInput;
  private RecordMetricActivityImpl activity;

  @BeforeEach
  void setup() throws ApiException {
    metricClient = mock(MetricClient.class);
    final WorkspaceApi workspaceApi = mock(WorkspaceApi.class);
    connectionUpdaterInput = mock(ConnectionUpdaterInput.class);

    when(connectionUpdaterInput.getConnectionId()).thenReturn(CONNECTION_ID);
    when(workspaceApi.getWorkspaceByConnectionId(new ConnectionIdRequestBody().connectionId(CONNECTION_ID)))
        .thenReturn(new WorkspaceRead().workspaceId(WORKSPACE_ID));
    when(workspaceApi.getWorkspaceByConnectionId(new ConnectionIdRequestBody().connectionId(CONNECTION_ID_WITHOUT_WORKSPACE)))
        .thenThrow(new ApiException(404, "Not Found"));

    activity = new RecordMetricActivityImpl(metricClient, workspaceApi);
  }

  @Test
  void testRecordingMetricCounter() {
    final RecordMetricInput metricInput = new RecordMetricInput(connectionUpdaterInput, Optional.empty(), METRIC_NAME, null);

    activity.recordWorkflowCountMetric(metricInput);

    verify(metricClient).count(
        eq(METRIC_NAME),
        eq(1L),
        eq(new MetricAttribute(MetricTags.CONNECTION_ID, String.valueOf(CONNECTION_ID))),
        eq(new MetricAttribute(MetricTags.WORKSPACE_ID, String.valueOf(WORKSPACE_ID))));
  }

  @Test
  void testRecordingMetricCounterWithAdditionalAttributes() {
    final MetricAttribute additionalAttribute = new MetricAttribute(MetricTags.JOB_STATUS, "test");
    final RecordMetricInput metricInput =
        new RecordMetricInput(connectionUpdaterInput, Optional.empty(), METRIC_NAME, new MetricAttribute[] {additionalAttribute});

    activity.recordWorkflowCountMetric(metricInput);

    verify(metricClient).count(
        eq(METRIC_NAME),
        eq(1L),
        eq(new MetricAttribute(MetricTags.CONNECTION_ID, String.valueOf(CONNECTION_ID))),
        eq(new MetricAttribute(MetricTags.WORKSPACE_ID, String.valueOf(WORKSPACE_ID))),
        eq(additionalAttribute));
  }

  @Test
  void testRecordingMetricCounterWithFailureCause() {
    final FailureCause failureCause = FailureCause.CANCELED;
    final RecordMetricInput metricInput = new RecordMetricInput(connectionUpdaterInput, Optional.of(failureCause), METRIC_NAME, null);

    activity.recordWorkflowCountMetric(metricInput);

    verify(metricClient).count(
        eq(METRIC_NAME),
        eq(1L),
        eq(new MetricAttribute(MetricTags.CONNECTION_ID, String.valueOf(CONNECTION_ID))),
        eq(new MetricAttribute(MetricTags.WORKSPACE_ID, String.valueOf(WORKSPACE_ID))),
        eq(new MetricAttribute(MetricTags.RESET_WORKFLOW_FAILURE_CAUSE, failureCause.name())));
  }

  @Test
  void testRecordingMetricCounterDoesntCrashOnApiNotFoundErrors() {
    final ConnectionUpdaterInput inputForUnkwnownWorkspaceId = new ConnectionUpdaterInput();
    inputForUnkwnownWorkspaceId.setConnectionId(CONNECTION_ID_WITHOUT_WORKSPACE);
    final RecordMetricInput metricInput = new RecordMetricInput(inputForUnkwnownWorkspaceId, Optional.empty(), METRIC_NAME, null);

    activity.recordWorkflowCountMetric(metricInput);

    verify(metricClient).count(
        eq(METRIC_NAME),
        eq(1L),
        eq(new MetricAttribute(MetricTags.CONNECTION_ID, String.valueOf(CONNECTION_ID_WITHOUT_WORKSPACE))));
  }

}
