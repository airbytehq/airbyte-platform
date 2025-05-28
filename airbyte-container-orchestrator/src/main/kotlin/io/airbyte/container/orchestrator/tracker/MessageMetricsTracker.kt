/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.container.orchestrator.tracker

import io.airbyte.metrics.MetricAttribute
import io.airbyte.metrics.MetricClient
import io.airbyte.metrics.OssMetricsRegistry
import io.airbyte.metrics.lib.MetricTags
import io.airbyte.protocol.models.v0.AirbyteMessage
import jakarta.inject.Singleton
import java.util.UUID
import java.util.concurrent.atomic.AtomicLong

private val RECORD_ATTRIBUTE: MetricAttribute = MetricAttribute(MetricTags.MESSAGE_TYPE, AirbyteMessage.Type.RECORD.toString())
private val STATE_ATTRIBUTE: MetricAttribute = MetricAttribute(MetricTags.MESSAGE_TYPE, AirbyteMessage.Type.STATE.toString())

/**
 * Helper to emit metrics around messages exchanged with connectors.
 */
@Singleton
class MessageMetricsTracker(
  private val metricClient: MetricClient,
) {
  private val destRecordReadCount = AtomicLong()
  private val destStateReadCount = AtomicLong()
  private val destRecordSentCount = AtomicLong()
  private val destStateSentCount = AtomicLong()
  private val sourceRecordReadCount = AtomicLong()
  private val sourceStateReadCount = AtomicLong()
  private var connectionAttribute: MetricAttribute? = null

  fun trackConnectionId(connectionId: UUID?) {
    connectionId?.let {
      connectionAttribute = MetricAttribute(MetricTags.CONNECTION_ID, connectionId.toString())
    }
  }

  fun trackDestRead(type: AirbyteMessage.Type?) {
    if (type == AirbyteMessage.Type.RECORD) {
      destRecordReadCount.incrementAndGet()
    } else if (type == AirbyteMessage.Type.STATE) {
      destStateReadCount.incrementAndGet()
    }
  }

  fun trackDestSent(type: AirbyteMessage.Type?) {
    if (type == AirbyteMessage.Type.RECORD) {
      destRecordSentCount.incrementAndGet()
    } else if (type == AirbyteMessage.Type.STATE) {
      destStateSentCount.incrementAndGet()
    }
  }

  fun trackSourceRead(type: AirbyteMessage.Type?) {
    if (type == AirbyteMessage.Type.RECORD) {
      sourceRecordReadCount.incrementAndGet()
    } else if (type == AirbyteMessage.Type.STATE) {
      sourceStateReadCount.incrementAndGet()
    }
  }

  fun flushDestReadCountMetric() {
    emitMetric(OssMetricsRegistry.WORKER_DESTINATION_MESSAGE_READ, destRecordReadCount, RECORD_ATTRIBUTE)
    emitMetric(OssMetricsRegistry.WORKER_DESTINATION_MESSAGE_READ, destStateReadCount, STATE_ATTRIBUTE)
  }

  fun flushDestSentCountMetric() {
    emitMetric(OssMetricsRegistry.WORKER_DESTINATION_MESSAGE_SENT, destRecordSentCount, RECORD_ATTRIBUTE)
    emitMetric(OssMetricsRegistry.WORKER_DESTINATION_MESSAGE_SENT, destStateSentCount, STATE_ATTRIBUTE)
  }

  fun flushSourceReadCountMetric() {
    emitMetric(OssMetricsRegistry.WORKER_SOURCE_MESSAGE_READ, sourceRecordReadCount, RECORD_ATTRIBUTE)
    emitMetric(OssMetricsRegistry.WORKER_SOURCE_MESSAGE_READ, sourceStateReadCount, STATE_ATTRIBUTE)
  }

  private fun emitMetric(
    metric: OssMetricsRegistry,
    value: AtomicLong,
    typeAttribute: MetricAttribute?,
  ) {
    if (connectionAttribute != null) {
      metricClient.count(metric, value.getAndSet(0), connectionAttribute, typeAttribute)
    } else {
      metricClient.count(metric, value.getAndSet(0), typeAttribute)
    }
  }
}
