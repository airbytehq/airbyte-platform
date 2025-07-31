/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.scheduling.activities

import datadog.trace.api.Trace
import io.airbyte.api.client.AirbyteApiClient
import io.airbyte.api.client.model.generated.ConnectionIdRequestBody
import io.airbyte.commons.micronaut.EnvConstants
import io.airbyte.commons.temporal.exception.RetryableException
import io.airbyte.commons.temporal.scheduling.ConnectionUpdaterInput
import io.airbyte.metrics.MetricAttribute
import io.airbyte.metrics.MetricClient
import io.airbyte.metrics.lib.ApmTraceConstants.ACTIVITY_TRACE_OPERATION_NAME
import io.airbyte.metrics.lib.ApmTraceConstants.Tags.CONNECTION_ID_KEY
import io.airbyte.metrics.lib.ApmTraceConstants.Tags.JOB_ID_KEY
import io.airbyte.metrics.lib.ApmTraceConstants.Tags.WORKSPACE_ID_KEY
import io.airbyte.metrics.lib.ApmTraceUtils
import io.airbyte.metrics.lib.MetricTags
import io.airbyte.workers.temporal.scheduling.activities.RecordMetricActivity.FailureCause
import io.airbyte.workers.temporal.scheduling.activities.RecordMetricActivity.RecordMetricInput
import io.micronaut.cache.annotation.Cacheable
import io.micronaut.context.annotation.Requires
import io.micronaut.http.HttpStatus
import jakarta.inject.Singleton
import org.openapitools.client.infrastructure.ClientException
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.io.IOException
import java.lang.invoke.MethodHandles
import java.util.UUID
import java.util.function.Consumer

/**
 * Implementation of the [RecordMetricActivity] that is managed by the application framework
 * and therefore has access to other singletons managed by the framework.
 */
@Singleton
@Requires(env = [EnvConstants.CONTROL_PLANE])
open class RecordMetricActivityImpl(
  private val airbyteApiClient: AirbyteApiClient,
  private val metricClient: MetricClient,
) : RecordMetricActivity {
  /**
   * Records a workflow counter for the specified metric.
   *
   * @param metricInput The information about the metric to record.
   */
  @Trace(operationName = ACTIVITY_TRACE_OPERATION_NAME)
  override fun recordWorkflowCountMetric(metricInput: RecordMetricInput) {
    ApmTraceUtils.addTagsToTrace(generateTags(metricInput.connectionUpdaterInput))
    val baseMetricAttributes = generateMetricAttributes(metricInput.connectionUpdaterInput!!)
    if (metricInput.metricAttributes != null) {
      baseMetricAttributes.addAll(metricInput.metricAttributes!!.toList())
    }
    metricInput.failureCause!!.ifPresent(
      Consumer { fc: FailureCause? ->
        baseMetricAttributes.add(
          MetricAttribute(
            MetricTags.FAILURE_CAUSE,
            fc!!.name,
          ),
        )
      },
    )
    metricClient.count(metricInput.metricName!!, 1L, *baseMetricAttributes.toTypedArray<MetricAttribute>())
  }

  /**
   * Generates the list of [MetricAttribute]s to be included when recording a metric.
   *
   * @param connectionUpdaterInput The [ConnectionUpdaterInput] that represents the workflow to
   * be executed.
   * @return The list of [MetricAttribute]s to be included when recording a metric.
   */
  private fun generateMetricAttributes(connectionUpdaterInput: ConnectionUpdaterInput): MutableList<MetricAttribute> {
    val metricAttributes: MutableList<MetricAttribute> = mutableListOf()
    metricAttributes.add(MetricAttribute(MetricTags.CONNECTION_ID, connectionUpdaterInput.connectionId.toString()))

    val workspaceId = getWorkspaceId(connectionUpdaterInput.connectionId!!)
    if (workspaceId != null) {
      metricAttributes.add(MetricAttribute(MetricTags.WORKSPACE_ID, workspaceId))
    } else {
      log.warn("unable to find a workspace for connectionId {}", connectionUpdaterInput.connectionId)
    }
    log.debug("generated metric attributes for workspaceId {} and connectionId {}", workspaceId, connectionUpdaterInput.connectionId)
    return metricAttributes
  }

  /**
   * Build the map of tags for instrumentation.
   *
   * @param connectionUpdaterInput The connection update input information.
   * @return The map of tags for instrumentation.
   */
  private fun generateTags(connectionUpdaterInput: ConnectionUpdaterInput?): MutableMap<String?, Any?> {
    val tags: MutableMap<String?, Any?> = HashMap<String?, Any?>()

    if (connectionUpdaterInput != null) {
      if (connectionUpdaterInput.connectionId != null) {
        tags.put(CONNECTION_ID_KEY, connectionUpdaterInput.connectionId)
        val workspaceId = getWorkspaceId(connectionUpdaterInput.connectionId!!)
        if (workspaceId != null) {
          tags.put(WORKSPACE_ID_KEY, workspaceId)
          log.debug("generated tags for workspaceId {} and connectionId {}", workspaceId, connectionUpdaterInput.connectionId)
        } else {
          log.debug("unable to find workspaceId for connectionId {}", connectionUpdaterInput.connectionId)
        }
      }
      if (connectionUpdaterInput.jobId != null) {
        tags.put(JOB_ID_KEY, connectionUpdaterInput.jobId)
      }
    }

    return tags
  }

  @Cacheable("connection-workspace-id")
  open fun getWorkspaceId(connectionId: UUID): String? {
    try {
      log.debug("Calling workspaceApi to fetch workspace ID for connection ID {}", connectionId)
      val workspaceRead =
        airbyteApiClient.workspaceApi.getWorkspaceByConnectionId(ConnectionIdRequestBody(connectionId))
      return workspaceRead.workspaceId.toString()
    } catch (e: ClientException) {
      if (e.statusCode == HttpStatus.NOT_FOUND.code) {
        // Metric recording should not fail because of a 404
        return null
      }
      throw RetryableException(e)
    } catch (e: IOException) {
      throw RetryableException(e)
    }
  }

  companion object {
    private val log: Logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass())
  }
}
