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

  public interface DefinitionProcessingOutcome {

    String getStatus();

  }

  public enum DefinitionProcessingSuccessOutcome implements DefinitionProcessingOutcome {

    INITIAL_VERSION_ADDED,
    DEFAULT_VERSION_UPDATED,
    VERSION_UNCHANGED;

    public String getStatus() {
      return "ok";
    }

  }

  public enum DefinitionProcessingFailureReason implements DefinitionProcessingOutcome {

    INCOMPATIBLE_PROTOCOL_VERSION,
    DEFINITION_CONVERSION_FAILED;

    public String getStatus() {
      return "failed";
    }

  }

  /**
   * Get metric attributes for a definition processing event.
   *
   * @param outcome The outcome of the processing event.
   * @return A list of attributes for the event.
   */
  public static MetricAttribute[] getMetricAttributes(final String dockerRepository,
                                                      final String dockerImageTag,
                                                      final DefinitionProcessingOutcome outcome) {
    final List<MetricAttribute> metricAttributes = new ArrayList<>();
    metricAttributes.add(new MetricAttribute("status", outcome.getStatus()));
    metricAttributes.add(new MetricAttribute("outcome", outcome.toString()));
    // Don't add the docker repository or version if the outcome is that version is unchanged -
    // this blows up the number of unique metrics per hour and is not that useful
    if (!outcome.equals(DefinitionProcessingSuccessOutcome.VERSION_UNCHANGED)) {
      metricAttributes.add(new MetricAttribute("docker_repository", dockerRepository));
      metricAttributes.add(new MetricAttribute("docker_image_tag", dockerImageTag));
    }
    return metricAttributes.toArray(metricAttributes.toArray(new MetricAttribute[0]));
  }

}
