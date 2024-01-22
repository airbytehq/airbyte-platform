/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.init;

import io.airbyte.metrics.lib.MetricAttribute;
import jakarta.inject.Singleton;
import java.util.ArrayList;
import java.util.List;

/**
 * Helper class for populating metrics for the ApplyDefinitionsHelper.
 */
@Singleton
public class ApplyDefinitionMetricsHelper {

  public enum DefinitionProcessingSuccessOutcome {
    INITIAL_VERSION_ADDED,
    DEFAULT_VERSION_UPDATED,
    VERSION_UNCHANGED
  }

  public enum DefinitionProcessingFailureReason {
    INCOMPATIBLE_PROTOCOL_VERSION,
    DEFINITION_CONVERSION_FAILED
  }

  /**
   * Get attributes for a successful definition processing event. The docker repository is not added
   * to keep the number of unique metrics low - this can be added if we want it later.
   *
   * @param successOutcome The outcome of the processing event.
   * @return A list of attributes for the successful event.
   */
  public static MetricAttribute[] getSuccessAttributes(final DefinitionProcessingSuccessOutcome successOutcome) {
    final List<MetricAttribute> metricAttributes = new ArrayList<>();
    metricAttributes.add(new MetricAttribute("status", "ok"));
    metricAttributes.add(new MetricAttribute("success_outcome", successOutcome.toString()));
    return metricAttributes.toArray(metricAttributes.toArray(new MetricAttribute[0]));
  }

  /**
   * Get attributes for a failed definition processing event. The docker repository is added for
   * easier debugging of failures.
   *
   * @param dockerRepository The docker repository that was unsuccessfully processed.
   * @param failureReason The reason for the failure.
   * @return A list of attributes for the failed event.
   */
  public static MetricAttribute[] getFailureAttributes(final String dockerRepository, final DefinitionProcessingFailureReason failureReason) {
    final List<MetricAttribute> metricAttributes = new ArrayList<>();
    metricAttributes.add(new MetricAttribute("status", "failed"));
    metricAttributes.add(new MetricAttribute("failure_reason", failureReason.toString()));
    metricAttributes.add(new MetricAttribute("docker_repository", dockerRepository));
    return metricAttributes.toArray(metricAttributes.toArray(new MetricAttribute[0]));
  }

}
