/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.scheduling.activities;

import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.airbyte.api.client.AirbyteApiClient;
import io.airbyte.api.client.generated.WorkspaceApi;
import io.airbyte.api.client.model.generated.ConnectionIdRequestBody;
import io.airbyte.api.client.model.generated.WorkspaceRead;
import io.airbyte.commons.temporal.scheduling.ConnectionUpdaterInput;
import io.airbyte.metrics.lib.MetricAttribute;
import io.airbyte.metrics.lib.MetricClient;
import io.airbyte.metrics.lib.MetricTags;
import io.airbyte.metrics.lib.OssMetricsRegistry;
import io.airbyte.workers.temporal.scheduling.activities.RecordMetricActivity.FailureCause;
import io.airbyte.workers.temporal.scheduling.activities.RecordMetricActivity.RecordMetricInput;
import io.micronaut.http.HttpStatus;
import java.io.IOException;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.openapitools.client.infrastructure.ClientException;

/**
 * Test suite for the {@link RecordMetricActivityImpl} class.
 */
class RecordMetricActivityImplTest {

  private static final UUID CONNECTION_ID = UUID.randomUUID();
  private static final UUID CONNECTION_ID_WITHOUT_WORKSPACE = UUID.randomUUID();
  private static final UUID WORKSPACE_ID = UUID.randomUUID();
  private static final OssMetricsRegistry METRIC_NAME = OssMetricsRegistry.TEMPORAL_WORKFLOW_ATTEMPT;

  private AirbyteApiClient airbyteApiClient;
  private MetricClient metricClient;
  private ConnectionUpdaterInput connectionUpdaterInput;
  private RecordMetricActivityImpl activity;

  @BeforeEach
  void setup() throws IOException {
    airbyteApiClient = mock(AirbyteApiClient.class);
    metricClient = mock(MetricClient.class);
    final WorkspaceApi workspaceApi = mock(WorkspaceApi.class);
    connectionUpdaterInput = mock(ConnectionUpdaterInput.class);

    when(connectionUpdaterInput.getConnectionId()).thenReturn(CONNECTION_ID);
    when(workspaceApi.getWorkspaceByConnectionId(new ConnectionIdRequestBody(CONNECTION_ID)))
        .thenReturn(new WorkspaceRead(WORKSPACE_ID, UUID.randomUUID(), "name", "slug", false, UUID.randomUUID(), null, null, null, null, null, null,
            null, null, null, null, null, null, null));
    when(workspaceApi.getWorkspaceByConnectionId(new ConnectionIdRequestBody(CONNECTION_ID_WITHOUT_WORKSPACE)))
        .thenThrow(new ClientException("Not Found", HttpStatus.NOT_FOUND.getCode(), null));
    when(airbyteApiClient.getWorkspaceApi()).thenReturn(workspaceApi);

    activity = new RecordMetricActivityImpl(airbyteApiClient, metricClient);
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
        eq(new MetricAttribute(MetricTags.FAILURE_CAUSE, failureCause.name())));
  }

  @Test
  void testRecordingMetricCounterDoesntCrashOnApiNotFoundErrors() {
    final ConnectionUpdaterInput inputForUnkwnownWorkspaceId = new ConnectionUpdaterInput(
        CONNECTION_ID_WITHOUT_WORKSPACE,
        null, null, false, null, null, false, false, false);
    final RecordMetricInput metricInput = new RecordMetricInput(inputForUnkwnownWorkspaceId, Optional.empty(), METRIC_NAME, null);

    activity.recordWorkflowCountMetric(metricInput);

    verify(metricClient).count(
        eq(METRIC_NAME),
        eq(1L),
        eq(new MetricAttribute(MetricTags.CONNECTION_ID, String.valueOf(CONNECTION_ID_WITHOUT_WORKSPACE))));
  }

}
