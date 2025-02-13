/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.scheduling.activities;

import io.airbyte.commons.temporal.scheduling.ConnectionUpdaterInput;
import io.airbyte.metrics.lib.MetricAttribute;
import io.airbyte.metrics.lib.OssMetricsRegistry;
import io.temporal.activity.ActivityInterface;
import io.temporal.activity.ActivityMethod;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;

/**
 * Custom Temporal activity that records metrics.
 */
@ActivityInterface
public interface RecordMetricActivity {

  /**
   * FailureCause.
   */
  enum FailureCause {
    ACTIVITY,
    CANCELED,
    CONNECTION,
    UNKNOWN,
    WORKFLOW
  }

  /**
   * RecordMetricInput.
   */
  @SuppressWarnings({
    "PMD.ArrayIsStoredDirectly",
    "PMD.MethodReturnsInternalArray",
    "PMD.UseVarargs"
  })
  class RecordMetricInput {

    private ConnectionUpdaterInput connectionUpdaterInput;
    private Optional<FailureCause> failureCause;
    private OssMetricsRegistry metricName;
    private MetricAttribute[] metricAttributes;

    public RecordMetricInput() {}

    public RecordMetricInput(ConnectionUpdaterInput connectionUpdaterInput,
                             Optional<FailureCause> failureCause,
                             OssMetricsRegistry metricName,
                             MetricAttribute[] metricAttributes) {
      this.connectionUpdaterInput = connectionUpdaterInput;
      this.failureCause = failureCause;
      this.metricName = metricName;
      this.metricAttributes = metricAttributes;
    }

    public ConnectionUpdaterInput getConnectionUpdaterInput() {
      return connectionUpdaterInput;
    }

    public void setConnectionUpdaterInput(ConnectionUpdaterInput connectionUpdaterInput) {
      this.connectionUpdaterInput = connectionUpdaterInput;
    }

    public Optional<FailureCause> getFailureCause() {
      return failureCause;
    }

    public void setFailureCause(Optional<FailureCause> failureCause) {
      this.failureCause = failureCause;
    }

    public OssMetricsRegistry getMetricName() {
      return metricName;
    }

    public void setMetricName(OssMetricsRegistry metricName) {
      this.metricName = metricName;
    }

    public MetricAttribute[] getMetricAttributes() {
      return metricAttributes;
    }

    public void setMetricAttributes(MetricAttribute[] metricAttributes) {
      this.metricAttributes = metricAttributes;
    }

    @Override
    public boolean equals(Object o) {
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      RecordMetricInput that = (RecordMetricInput) o;
      return Objects.equals(connectionUpdaterInput, that.connectionUpdaterInput) && Objects.equals(failureCause, that.failureCause)
          && metricName == that.metricName && Objects.deepEquals(metricAttributes, that.metricAttributes);
    }

    @Override
    public int hashCode() {
      return Objects.hash(connectionUpdaterInput, failureCause, metricName, Arrays.hashCode(metricAttributes));
    }

    @Override
    public String toString() {
      return "RecordMetricInput{"
          + "connectionUpdaterInput=" + connectionUpdaterInput
          + ", failureCause=" + failureCause
          + ", metricName=" + metricName
          + ", metricAttributes=" + Arrays.toString(metricAttributes)
          + '}';
    }

  }

  /**
   * Records a counter metric.
   *
   * @param metricInput The metric information.
   */
  @ActivityMethod
  void recordWorkflowCountMetric(final RecordMetricInput metricInput);

}
