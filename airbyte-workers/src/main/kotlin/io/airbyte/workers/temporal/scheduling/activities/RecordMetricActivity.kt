/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.scheduling.activities

import io.airbyte.commons.temporal.scheduling.ConnectionUpdaterInput
import io.airbyte.metrics.MetricAttribute
import io.airbyte.metrics.OssMetricsRegistry
import io.temporal.activity.ActivityInterface
import io.temporal.activity.ActivityMethod
import java.util.Objects
import java.util.Optional

/**
 * Custom Temporal activity that records metrics.
 */
@ActivityInterface
interface RecordMetricActivity {
  /**
   * FailureCause.
   */
  enum class FailureCause {
    ACTIVITY,
    CANCELED,
    CONNECTION,
    UNKNOWN,
    WORKFLOW,
  }

  /**
   * RecordMetricInput.
   */
  class RecordMetricInput {
    @JvmField
    var connectionUpdaterInput: ConnectionUpdaterInput? = null

    @JvmField
    var failureCause: Optional<FailureCause>? = null

    @JvmField
    var metricName: OssMetricsRegistry? = null

    @JvmField
    var metricAttributes: Array<MetricAttribute>? = null

    constructor()

    constructor(
      connectionUpdaterInput: ConnectionUpdaterInput?,
      failureCause: Optional<FailureCause>?,
      metricName: OssMetricsRegistry?,
      metricAttributes: Array<MetricAttribute>?,
    ) {
      this.connectionUpdaterInput = connectionUpdaterInput
      this.failureCause = failureCause
      this.metricName = metricName
      this.metricAttributes = metricAttributes
    }

    override fun equals(o: Any?): Boolean {
      if (o == null || javaClass != o.javaClass) {
        return false
      }
      val that = o as RecordMetricInput
      return connectionUpdaterInput == that.connectionUpdaterInput &&
        failureCause == that.failureCause &&
        metricName == that.metricName &&
        Objects.deepEquals(metricAttributes, that.metricAttributes)
    }

    override fun hashCode(): Int = Objects.hash(connectionUpdaterInput, failureCause, metricName, metricAttributes.contentHashCode())

    override fun toString(): String =
      (
        "RecordMetricInput{" +
          "connectionUpdaterInput=" + connectionUpdaterInput +
          ", failureCause=" + failureCause +
          ", metricName=" + metricName +
          ", metricAttributes=" + metricAttributes.contentToString() + '}'
      )
  }

  /**
   * Records a counter metric.
   *
   * @param metricInput The metric information.
   */
  @ActivityMethod
  fun recordWorkflowCountMetric(metricInput: RecordMetricInput)
}
