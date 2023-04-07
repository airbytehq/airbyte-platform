/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers;

import io.airbyte.metrics.lib.MetricAttribute;
import io.airbyte.metrics.lib.MetricClient;
import io.airbyte.metrics.lib.OssMetricsRegistry;
import io.airbyte.protocol.models.AirbyteStreamNameNamespacePair;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Reporter of errors that happen in the worker.
 */
public class WorkerMetricReporter {

  private final String dockerRepo;
  private final String dockerVersion;
  private final MetricClient metricClient;

  public WorkerMetricReporter(final MetricClient metricClient, final String dockerImage) {
    final String[] dockerImageInfo = dockerImage.split(":");
    this.dockerRepo = dockerImageInfo[0];
    this.dockerVersion = dockerImageInfo.length > 1 ? dockerImageInfo[1] : "";
    this.metricClient = metricClient;
  }

  /**
   * Given a AirbyteStreamNameNamespacePair and a String Set of validationErrors, produce a DataDog
   * count + a metric for each error.
   */
  public void trackSchemaValidationErrors(final AirbyteStreamNameNamespacePair stream, final Set<String> validationErrors) {
    final List<MetricAttribute> attributes = new ArrayList<>();
    attributes.addAll(validationErrors.stream().map(f -> new MetricAttribute("validation_error", f)).collect(Collectors.toList()));
    attributes.add(new MetricAttribute("docker_repo", dockerRepo));
    attributes.add(new MetricAttribute("docker_version", dockerVersion));
    attributes.add(new MetricAttribute("stream", stream.toString()));
    final MetricAttribute[] attributesArray = attributes.toArray(new MetricAttribute[0]);
    metricClient.count(OssMetricsRegistry.NUM_DISTINCT_SCHEMA_VALIDATION_ERRORS_IN_STREAMS, validationErrors.size(), attributesArray);
  }

  /**
   * Given a AirbyteStreamNameNamespacePair and a String Set of unexpectedFieldNames, produce a
   * DataDog count + a metric for each unexpected field.
   */
  public void trackUnexpectedFields(final AirbyteStreamNameNamespacePair stream, Set<String> unexpectedFieldNames) {
    final List<MetricAttribute> attributes = new ArrayList<>();
    attributes.addAll(unexpectedFieldNames.stream().map(f -> new MetricAttribute("field_name", f)).collect(Collectors.toList()));
    attributes.add(new MetricAttribute("docker_repo", dockerRepo));
    attributes.add(new MetricAttribute("docker_version", dockerVersion));
    attributes.add(new MetricAttribute("stream", stream.toString()));
    final MetricAttribute[] attributesArr = attributes.toArray(new MetricAttribute[0]);
    metricClient.count(OssMetricsRegistry.NUM_UNEXPECTED_FIELDS_IN_STREAMS, unexpectedFieldNames.size(), attributesArr);
  }

  public void trackStateMetricTrackerError() {
    metricClient.count(OssMetricsRegistry.STATE_METRIC_TRACKER_ERROR, 1, new MetricAttribute("docker_repo", dockerRepo),
        new MetricAttribute("docker_version", dockerVersion));
  }

}
