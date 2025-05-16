/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.signal

import io.airbyte.api.client.AirbyteApiClient
import io.airbyte.config.SignalInput
import io.airbyte.metrics.MetricAttribute
import io.airbyte.metrics.MetricClient
import io.airbyte.metrics.OssMetricsRegistry
import io.airbyte.metrics.lib.MetricTags
import io.airbyte.workload.repository.domain.WorkloadType
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Singleton
import io.airbyte.api.client.model.generated.SignalInput as ApiSignalInput

private val logger = KotlinLogging.logger {}

@Singleton
class ApiSignalSender(
  private val airbyteApi: AirbyteApiClient,
  private val metricClient: MetricClient,
) : AbstractSignalSender(metricClient) {
  override fun sendSignal(
    workloadType: WorkloadType,
    signal: SignalInput,
  ) {
    try {
      airbyteApi.signalApi.signal(
        ApiSignalInput(
          workflowType = signal.workflowType,
          workflowId = signal.workflowId,
        ),
      )
      metricClient.count(
        metric = OssMetricsRegistry.WORKLOADS_SIGNAL,
        attributes =
          arrayOf(
            MetricAttribute(MetricTags.WORKFLOW_TYPE, signal.workflowType),
            MetricAttribute(MetricTags.WORKLOAD_TYPE, workloadType.toString()),
            MetricAttribute(MetricTags.STATUS, MetricTags.SUCCESS),
          ),
      )
    } catch (e: Exception) {
      logger.error(e) { "Failed to send signal for the payload: $signal" }
      metricClient.count(
        metric = OssMetricsRegistry.WORKLOADS_SIGNAL,
        attributes =
          arrayOf(
            MetricAttribute(MetricTags.WORKFLOW_TYPE, signal.workflowType),
            MetricAttribute(MetricTags.WORKLOAD_TYPE, workloadType.toString()),
            MetricAttribute(MetricTags.STATUS, MetricTags.FAILURE),
            e.message?.let { m ->
              MetricAttribute(MetricTags.FAILURE_TYPE, m)
            },
          ),
      )
    }
  }
}
