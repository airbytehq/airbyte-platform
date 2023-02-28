/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.scheduling.activities;

import static io.airbyte.metrics.lib.ApmTraceConstants.ACTIVITY_TRACE_OPERATION_NAME;
import static io.airbyte.metrics.lib.ApmTraceConstants.Tags.CONNECTION_ID_KEY;
import static io.airbyte.metrics.lib.ApmTraceConstants.Tags.JOB_ID_KEY;
import static io.airbyte.metrics.lib.ApmTraceConstants.Tags.WORKSPACE_ID_KEY;

import datadog.trace.api.Trace;
import io.airbyte.api.client.generated.WorkspaceApi;
import io.airbyte.api.client.invoker.generated.ApiException;
import io.airbyte.api.client.model.generated.ConnectionIdRequestBody;
import io.airbyte.api.client.model.generated.WorkspaceRead;
import io.airbyte.commons.temporal.config.WorkerMode;
import io.airbyte.commons.temporal.exception.RetryableException;
import io.airbyte.commons.temporal.scheduling.ConnectionUpdaterInput;
import io.airbyte.metrics.lib.ApmTraceUtils;
import io.airbyte.metrics.lib.MetricAttribute;
import io.airbyte.metrics.lib.MetricClient;
import io.airbyte.metrics.lib.MetricTags;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Singleton;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/**
 * Implementation of the {@link RecordMetricActivity} that is managed by the application framework
 * and therefore has access to other singletons managed by the framework.
 */
@Slf4j
@Singleton
@Requires(env = WorkerMode.CONTROL_PLANE)
public class RecordMetricActivityImpl implements RecordMetricActivity {

  private final MetricClient metricClient;
  private final WorkspaceApi workspaceApi;

  public RecordMetricActivityImpl(final MetricClient metricClient, final WorkspaceApi workspaceApi) {
    this.metricClient = metricClient;
    this.workspaceApi = workspaceApi;
  }

  /**
   * Records a workflow counter for the specified metric.
   *
   * @param metricInput The information about the metric to record.
   */
  @Trace(operationName = ACTIVITY_TRACE_OPERATION_NAME)
  @Override
  public void recordWorkflowCountMetric(final RecordMetricInput metricInput) {
    final Optional<UUID> workspaceIdOptional = getWorkspaceId(metricInput);
    ApmTraceUtils.addTagsToTrace(generateTags(metricInput.getConnectionUpdaterInput(), workspaceIdOptional));
    final List<MetricAttribute> baseMetricAttributes = generateMetricAttributes(metricInput.getConnectionUpdaterInput(), workspaceIdOptional);
    if (metricInput.getMetricAttributes() != null) {
      baseMetricAttributes.addAll(Stream.of(metricInput.getMetricAttributes()).collect(Collectors.toList()));
    }
    metricInput.getFailureCause().ifPresent(fc -> baseMetricAttributes.add(new MetricAttribute(MetricTags.RESET_WORKFLOW_FAILURE_CAUSE, fc.name())));
    metricClient.count(metricInput.getMetricName(), 1L, baseMetricAttributes.toArray(new MetricAttribute[] {}));
  }

  /**
   * Generates the list of {@link MetricAttribute}s to be included when recording a metric.
   *
   * @param connectionUpdaterInput The {@link ConnectionUpdaterInput} that represents the workflow to
   *        be executed.
   * @return The list of {@link MetricAttribute}s to be included when recording a metric.
   */
  private List<MetricAttribute> generateMetricAttributes(final ConnectionUpdaterInput connectionUpdaterInput, final Optional<UUID> workspaceIdOptional) {
    final List<MetricAttribute> metricAttributes = new ArrayList<>();
    metricAttributes.add(new MetricAttribute(MetricTags.CONNECTION_ID, String.valueOf(connectionUpdaterInput.getConnectionId())));
    workspaceIdOptional.ifPresent(id -> metricAttributes.add(new MetricAttribute(MetricTags.WORKSPACE_ID, id.toString())));
    return metricAttributes;
  }

  /**
   * Build the map of tags for instrumentation.
   *
   * @param connectionUpdaterInput The connection update input information.
   * @return The map of tags for instrumentation.
   */
  private Map<String, Object> generateTags(final ConnectionUpdaterInput connectionUpdaterInput, final Optional<UUID> workspaceIdOptional) {
    final Map<String, Object> tags = new HashMap();

    if (connectionUpdaterInput != null) {
      if (connectionUpdaterInput.getConnectionId() != null) {
        tags.put(CONNECTION_ID_KEY, connectionUpdaterInput.getConnectionId());
      }
      if (connectionUpdaterInput.getJobId() != null) {
        tags.put(JOB_ID_KEY, connectionUpdaterInput.getJobId());
      }
      workspaceIdOptional.ifPresent(id -> tags.put(WORKSPACE_ID_KEY, id));
    }

    return tags;
  }

  private Optional<UUID> getWorkspaceId(final RecordMetricInput recordMetricInput) {
    if (recordMetricInput.getConnectionUpdaterInput() == null) {
      return Optional.empty();
    }
    try {
      final UUID connectionId = recordMetricInput.getConnectionUpdaterInput().getConnectionId();
      final WorkspaceRead workspaceRead = workspaceApi.getWorkspaceByConnectionId(new ConnectionIdRequestBody().connectionId(connectionId));
      return Optional.of(workspaceRead.getWorkspaceId());
    } catch (final ApiException e) {
      throw new RetryableException(e);
    }
  }
}
