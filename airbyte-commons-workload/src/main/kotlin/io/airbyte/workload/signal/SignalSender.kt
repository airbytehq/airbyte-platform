/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.signal

import io.airbyte.commons.json.Jsons
import io.airbyte.config.SignalInput
import io.airbyte.metrics.MetricAttribute
import io.airbyte.metrics.MetricClient
import io.airbyte.metrics.OssMetricsRegistry
import io.airbyte.metrics.lib.MetricTags
import io.airbyte.workload.repository.domain.WorkloadType
import io.github.oshai.kotlinlogging.KotlinLogging

private val logger = KotlinLogging.logger {}

interface SignalSender {
  fun sendSignal(
    workloadType: WorkloadType,
    signalPayload: String?,
  )
}

abstract class AbstractSignalSender(
  private val metricClient: MetricClient,
) : SignalSender {
  override fun sendSignal(
    workloadType: WorkloadType,
    signalPayload: String?,
  ) {
    val signalInput =
      if (signalPayload == null) {
        null
      } else {
        try {
          Jsons.deserialize(signalPayload, SignalInput::class.java)
        } catch (e: Exception) {
          logger.error(e) { "Failed to deserialize signal payload: $signalPayload" }
          metricClient.count(
            metric = OssMetricsRegistry.WORKLOADS_SIGNAL,
            attributes =
              arrayOf(
                MetricAttribute(MetricTags.STATUS, MetricTags.FAILURE),
                MetricAttribute(MetricTags.FAILURE_TYPE, "deserialization"),
                MetricAttribute(MetricTags.WORKLOAD_TYPE_TAG, workloadType.toString()),
              ),
          )
          return
        }
      }
    if (signalInput != null) {
      sendSignal(workloadType, signalInput)
    }
  }

  protected abstract fun sendSignal(
    workloadType: WorkloadType,
    signal: SignalInput,
  )
}
