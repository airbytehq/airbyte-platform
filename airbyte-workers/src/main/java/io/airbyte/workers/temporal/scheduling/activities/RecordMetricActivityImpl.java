/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.scheduling.activities;

import static io.airbyte.metrics.lib.ApmTraceConstants.ACTIVITY_TRACE_OPERATION_NAME;
import static io.airbyte.metrics.lib.ApmTraceConstants.Tags.CONNECTION_ID_KEY;
import static io.airbyte.metrics.lib.ApmTraceConstants.Tags.JOB_ID_KEY;
import static io.airbyte.metrics.lib.ApmTraceConstants.Tags.WORKSPACE_ID_KEY;

import datadog.trace.api.Trace;
import io.airbyte.api.client.AirbyteApiClient;
import io.airbyte.api.client.model.generated.ConnectionIdRequestBody;
import io.airbyte.api.client.model.generated.WorkspaceRead;
import io.airbyte.commons.micronaut.EnvConstants;
import io.airbyte.commons.temporal.exception.RetryableException;
import io.airbyte.commons.temporal.scheduling.ConnectionUpdaterInput;
import io.airbyte.metrics.lib.ApmTraceUtils;
import io.airbyte.metrics.lib.MetricAttribute;
import io.airbyte.metrics.lib.MetricClient;
import io.airbyte.metrics.lib.MetricTags;
import io.micronaut.cache.annotation.Cacheable;
import io.micronaut.context.annotation.Requires;
import io.micronaut.http.HttpStatus;
import jakarta.inject.Singleton;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;
import org.openapitools.client.infrastructure.ClientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of the {@link RecordMetricActivity} that is managed by the application framework
 * and therefore has access to other singletons managed by the framework.
 */
@Singleton
@Requires(env = EnvConstants.CONTROL_PLANE)
public class RecordMetricActivityImpl implements RecordMetricActivity {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final AirbyteApiClient airbyteApiClient;
  private final MetricClient metricClient;

  public RecordMetricActivityImpl(final AirbyteApiClient airbyteApiClient, final MetricClient metricClient) {
    this.airbyteApiClient = airbyteApiClient;
    this.metricClient = metricClient;
  }

  /**
   * Records a workflow counter for the specified metric.
   *
   * @param metricInput The information about the metric to record.
   */
  @Trace(operationName = ACTIVITY_TRACE_OPERATION_NAME)
  @Override
  public void recordWorkflowCountMetric(final RecordMetricInput metricInput) {
    ApmTraceUtils.addTagsToTrace(generateTags(metricInput.getConnectionUpdaterInput()));
    final List<MetricAttribute> baseMetricAttributes = generateMetricAttributes(metricInput.getConnectionUpdaterInput());
    if (metricInput.getMetricAttributes() != null) {
      baseMetricAttributes.addAll(Stream.of(metricInput.getMetricAttributes()).toList());
    }
    metricInput.getFailureCause().ifPresent(fc -> baseMetricAttributes.add(new MetricAttribute(MetricTags.FAILURE_CAUSE, fc.name())));
    metricClient.count(metricInput.getMetricName(), 1L, baseMetricAttributes.toArray(new MetricAttribute[] {}));
  }

  /**
   * Generates the list of {@link MetricAttribute}s to be included when recording a metric.
   *
   * @param connectionUpdaterInput The {@link ConnectionUpdaterInput} that represents the workflow to
   *        be executed.
   * @return The list of {@link MetricAttribute}s to be included when recording a metric.
   */
  private List<MetricAttribute> generateMetricAttributes(final ConnectionUpdaterInput connectionUpdaterInput) {
    final List<MetricAttribute> metricAttributes = new ArrayList<>();
    metricAttributes.add(new MetricAttribute(MetricTags.CONNECTION_ID, String.valueOf(connectionUpdaterInput.getConnectionId())));

    final String workspaceId = getWorkspaceId(connectionUpdaterInput.getConnectionId());
    if (workspaceId != null) {
      metricAttributes.add(new MetricAttribute(MetricTags.WORKSPACE_ID, workspaceId));
    } else {
      log.warn("unable to find a workspace for connectionId {}", connectionUpdaterInput.getConnectionId());
    }
    log.debug("generated metric attributes for workspaceId {} and connectionId {}", workspaceId, connectionUpdaterInput.getConnectionId());
    return metricAttributes;
  }

  /**
   * Build the map of tags for instrumentation.
   *
   * @param connectionUpdaterInput The connection update input information.
   * @return The map of tags for instrumentation.
   */
  private Map<String, Object> generateTags(final ConnectionUpdaterInput connectionUpdaterInput) {
    final Map<String, Object> tags = new HashMap<>();

    if (connectionUpdaterInput != null) {
      if (connectionUpdaterInput.getConnectionId() != null) {
        tags.put(CONNECTION_ID_KEY, connectionUpdaterInput.getConnectionId());
        final String workspaceId = getWorkspaceId(connectionUpdaterInput.getConnectionId());
        if (workspaceId != null) {
          tags.put(WORKSPACE_ID_KEY, workspaceId);
          log.debug("generated tags for workspaceId {} and connectionId {}", workspaceId, connectionUpdaterInput.getConnectionId());
        } else {
          log.debug("unable to find workspaceId for connectionId {}", connectionUpdaterInput.getConnectionId());
        }
      }
      if (connectionUpdaterInput.getJobId() != null) {
        tags.put(JOB_ID_KEY, connectionUpdaterInput.getJobId());
      }
    }

    return tags;
  }

  @Cacheable("connection-workspace-id")
  String getWorkspaceId(final UUID connectionId) {
    try {
      log.debug("Calling workspaceApi to fetch workspace ID for connection ID {}", connectionId);
      final WorkspaceRead workspaceRead =
          airbyteApiClient.getWorkspaceApi().getWorkspaceByConnectionId(new ConnectionIdRequestBody(connectionId));
      return workspaceRead.getWorkspaceId().toString();
    } catch (final ClientException e) {
      if (e.getStatusCode() == HttpStatus.NOT_FOUND.getCode()) {
        // Metric recording should not fail because of a 404
        return null;
      }
      throw new RetryableException(e);
    } catch (final IOException e) {
      throw new RetryableException(e);
    }
  }

}
