/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.scheduling.activities

import io.airbyte.api.client.AirbyteApiClient
import io.airbyte.api.client.model.generated.CheckDataWorkerCapacityRequest
import io.airbyte.commons.temporal.exception.RetryableException
import io.airbyte.metrics.MetricAttribute
import io.airbyte.metrics.MetricClient
import io.airbyte.metrics.OssMetricsRegistry
import io.airbyte.metrics.lib.MetricTags
import io.airbyte.workers.temporal.scheduling.activities.CapacityCheckActivity.CapacityCheckInput
import io.airbyte.workers.temporal.scheduling.activities.CapacityCheckActivity.CapacityCheckOutput
import io.micronaut.http.HttpStatus
import jakarta.inject.Singleton
import org.openapitools.client.infrastructure.ClientException
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.io.IOException
import java.lang.invoke.MethodHandles

/**
 * API-backed implementation of CapacityCheckActivity.
 */
@Singleton
class CapacityCheckActivityImpl(
  private val airbyteApiClient: AirbyteApiClient,
  private val metricClient: MetricClient,
) : CapacityCheckActivity {
  override fun checkCapacity(input: CapacityCheckInput): CapacityCheckOutput {
    val jobId = input.jobId
    val connectionId = input.connectionId
    val organizationId = input.organizationId

    if (!input.enforcementEnabled || jobId == null || connectionId == null || organizationId == null) {
      return CapacityCheckOutput(
        capacityAvailable = true,
        useOnDemandCapacity = false,
        enforcementEnabled = input.enforcementEnabled,
      )
    }

    try {
      val response =
        airbyteApiClient.jobsApi.checkDataWorkerCapacity(
          CheckDataWorkerCapacityRequest(
            jobId,
            connectionId,
            organizationId,
          ),
        )

      val output =
        CapacityCheckOutput(
          capacityAvailable = response.capacityAvailable,
          useOnDemandCapacity = response.useOnDemandCapacity,
          enforcementEnabled = true,
        )
      emitCapacityMetrics(input, output)
      return output
    } catch (e: ClientException) {
      if (e.statusCode == HttpStatus.NOT_FOUND.getCode()) {
        throw e
      }
      throw RetryableException(e)
    } catch (e: IOException) {
      throw RetryableException(e)
    }
  }

  private fun emitCapacityMetrics(
    input: CapacityCheckInput,
    output: CapacityCheckOutput,
  ) {
    val metricAttributes = input.metricAttributes ?: return
    if (
      metricAttributes.none { it.key == MetricTags.CONNECTION_ID } ||
      metricAttributes.none { it.key == MetricTags.WORKSPACE_ID } ||
      metricAttributes.none { it.key == MetricTags.ORGANIZATION_ID } ||
      metricAttributes.none { it.key == MetricTags.JOB_ID }
    ) {
      return
    }

    if (output.capacityAvailable) {
      val admissionResult =
        when {
          output.useOnDemandCapacity -> "on_demand"
          input.queueAgeSeconds != null -> "committed_after_queue"
          else -> "committed_immediate"
        }
      emitMetric {
        metricClient.count(
          OssMetricsRegistry.DATA_WORKER_CAPACITY_ADMISSION,
          1L,
          *metricAttributes,
          MetricAttribute(MetricTags.DATA_WORKER_CAPACITY_ADMISSION_RESULT, admissionResult),
        )
      }
    }

    input.queueAgeSeconds?.let { queueAgeSeconds ->
      emitMetric {
        metricClient.distribution(
          OssMetricsRegistry.DATA_WORKER_CAPACITY_QUEUE_AGE_SECONDS,
          queueAgeSeconds,
          *metricAttributes,
        )
      }
    }
  }

  private fun emitMetric(action: () -> Unit) {
    try {
      action()
    } catch (e: Exception) {
      log.warn("Failed to emit Data Worker capacity metric", e)
    }
  }

  companion object {
    private val log: Logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass())
  }
}
