/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import io.airbyte.metrics.lib.MetricAttribute;
import io.airbyte.metrics.lib.MetricClient;
import io.airbyte.metrics.lib.OssMetricsRegistry;
import io.airbyte.protocol.models.AirbyteStreamNameNamespacePair;
import java.util.HashSet;
import org.junit.jupiter.api.Test;

class WorkerMetricReporterTest {

  private static final String DOCKER_IMAGE = "scratch";
  final MetricClient metricClient = mock(MetricClient.class);

  @Test
  void trackSchemaValidationErrors() {
    final var reporter = new WorkerMetricReporter(metricClient, DOCKER_IMAGE);
    final var stream = new AirbyteStreamNameNamespacePair("name", "namespace");
    final var errors = new HashSet<String>();
    for (var i = 0; i < 10; i++) {
      errors.add(String.valueOf(i));
    }

    reporter.trackSchemaValidationErrors(stream, errors);

    verify(metricClient).count(
        eq(OssMetricsRegistry.NUM_DISTINCT_SCHEMA_VALIDATION_ERRORS_IN_STREAMS),
        eq(10L),
        any(MetricAttribute.class), // new MetricAttribute("validation_error", "0"),
        any(MetricAttribute.class), // new MetricAttribute("validation_error", "1"),
        any(MetricAttribute.class), // new MetricAttribute("validation_error", "2"),
        any(MetricAttribute.class), // new MetricAttribute("validation_error", "3"),
        any(MetricAttribute.class), // new MetricAttribute("validation_error", "4"),
        any(MetricAttribute.class), // new MetricAttribute("validation_error", "5"),
        any(MetricAttribute.class), // new MetricAttribute("validation_error", "6"),
        any(MetricAttribute.class), // new MetricAttribute("validation_error", "7"),
        any(MetricAttribute.class), // new MetricAttribute("validation_error", "8"),
        any(MetricAttribute.class), // new MetricAttribute("validation_error", "9"),
        any(MetricAttribute.class), // new MetricAttribute("docker_repo", "scratch"),
        any(MetricAttribute.class), // new MetricAttribute("docker_version", ""),
        any(MetricAttribute.class) // new MetricAttribute("stream", stream.toString())
    );
  }

  @Test
  void trackUnexpectedFields() {
    final var reporter = new WorkerMetricReporter(metricClient, DOCKER_IMAGE);
    final var stream = new AirbyteStreamNameNamespacePair("name", "namespace");
    final var fields = new HashSet<String>();
    for (var i = 0; i < 10; i++) {
      fields.add(String.valueOf(i));
    }

    reporter.trackUnexpectedFields(stream, fields);

    verify(metricClient).count(
        eq(OssMetricsRegistry.NUM_UNEXPECTED_FIELDS_IN_STREAMS),
        eq(10L),
        any(MetricAttribute.class), // new MetricAttribute("field_name", "0"),
        any(MetricAttribute.class), // new MetricAttribute("field_name", "1"),
        any(MetricAttribute.class), // new MetricAttribute("field_name", "2"),
        any(MetricAttribute.class), // new MetricAttribute("field_name", "3"),
        any(MetricAttribute.class), // new MetricAttribute("field_name", "4"),
        any(MetricAttribute.class), // new MetricAttribute("field_name", "5"),
        any(MetricAttribute.class), // new MetricAttribute("field_name", "6"),
        any(MetricAttribute.class), // new MetricAttribute("field_name", "7"),
        any(MetricAttribute.class), // new MetricAttribute("field_name", "8"),
        any(MetricAttribute.class), // new MetricAttribute("field_name", "9"),
        any(MetricAttribute.class), // new MetricAttribute("docker_repo", "scratch"),
        any(MetricAttribute.class), // new MetricAttribute("docker_version", ""),
        any(MetricAttribute.class) // new MetricAttribute("stream", stream.toString())
    );
  }

  @Test
  void trackStateMetricTrackerError() {
    final var reporter = new WorkerMetricReporter(metricClient, DOCKER_IMAGE);
    reporter.trackStateMetricTrackerError();

    verify(metricClient).count(
        eq(OssMetricsRegistry.STATE_METRIC_TRACKER_ERROR),
        eq(1L),
        any(MetricAttribute.class), // new MetricAttribute("docker_repo", "scratch"),
        any(MetricAttribute.class) // new MetricAttribute("docker_version", "")
    );
  }

}
