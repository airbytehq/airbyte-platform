/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.internal;

import io.airbyte.metrics.lib.MetricAttribute;
import io.airbyte.metrics.lib.MetricClient;
import io.airbyte.metrics.lib.MetricTags;
import io.airbyte.metrics.lib.OssMetricsRegistry;
import io.airbyte.protocol.models.AirbyteMessage.Type;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Helper to emit metrics around messages exchanged with connectors.
 */
public class MessageMetricsTracker {

  private static final MetricAttribute RECORD_ATTRIBUTE = new MetricAttribute(MetricTags.MESSAGE_TYPE, Type.RECORD.toString());
  private static final MetricAttribute STATE_ATTRIBUTE = new MetricAttribute(MetricTags.MESSAGE_TYPE, Type.STATE.toString());

  private final MetricClient metricClient;
  private final AtomicLong destRecordReadCount = new AtomicLong();
  private final AtomicLong destStateReadCount = new AtomicLong();
  private final AtomicLong destRecordSentCount = new AtomicLong();
  private final AtomicLong destStateSentCount = new AtomicLong();
  private final AtomicLong sourceRecordReadCount = new AtomicLong();
  private final AtomicLong sourceStateReadCount = new AtomicLong();
  private MetricAttribute connectionAttribute = null;

  public MessageMetricsTracker(final MetricClient metricClient) {
    this.metricClient = metricClient;
  }

  public void trackConnectionId(final UUID connectionId) {
    connectionAttribute = new MetricAttribute(MetricTags.CONNECTION_ID, connectionId.toString());
  }

  public void trackDestRead(final Type type) {
    if (type == Type.RECORD) {
      destRecordReadCount.incrementAndGet();
    } else if (type == Type.STATE) {
      destStateReadCount.incrementAndGet();
    }
  }

  public void trackDestSent(final Type type) {
    if (type == Type.RECORD) {
      destRecordSentCount.incrementAndGet();
    } else if (type == Type.STATE) {
      destStateSentCount.incrementAndGet();
    }
  }

  public void trackSourceRead(final Type type) {
    if (type == Type.RECORD) {
      sourceRecordReadCount.incrementAndGet();
    } else if (type == Type.STATE) {
      sourceStateReadCount.incrementAndGet();
    }
  }

  public void flushDestReadCountMetric() {
    emitMetric(OssMetricsRegistry.WORKER_DESTINATION_MESSAGE_READ, destRecordReadCount, RECORD_ATTRIBUTE);
    emitMetric(OssMetricsRegistry.WORKER_DESTINATION_MESSAGE_READ, destStateReadCount, STATE_ATTRIBUTE);
  }

  public void flushDestSentCountMetric() {
    emitMetric(OssMetricsRegistry.WORKER_DESTINATION_MESSAGE_SENT, destRecordSentCount, RECORD_ATTRIBUTE);
    emitMetric(OssMetricsRegistry.WORKER_DESTINATION_MESSAGE_SENT, destStateSentCount, STATE_ATTRIBUTE);
  }

  public void flushSourceReadCountMetric() {
    emitMetric(OssMetricsRegistry.WORKER_SOURCE_MESSAGE_READ, sourceRecordReadCount, RECORD_ATTRIBUTE);
    emitMetric(OssMetricsRegistry.WORKER_SOURCE_MESSAGE_READ, sourceStateReadCount, STATE_ATTRIBUTE);
  }

  private void emitMetric(final OssMetricsRegistry metric, final AtomicLong value, final MetricAttribute typeAttribute) {
    if (connectionAttribute != null) {
      metricClient.count(metric, value.getAndSet(0), connectionAttribute, typeAttribute);
    } else {
      metricClient.count(metric, value.getAndSet(0), typeAttribute);
    }
  }

}
