/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */
package io.airbyte.config.init

import io.airbyte.metrics.lib.MetricAttribute
import jakarta.inject.Singleton

/**
 * Helper class for populating metrics for the ApplyDefinitionsHelper.
 */
@Singleton
object ApplyDefinitionMetricsHelper {
  /**
   * Get metric attributes for a definition processing event.
   *
   * @param outcome The outcome of the processing event.
   * @return A list of attributes for the event.
   */
  @JvmStatic
  fun getMetricAttributes(
    dockerRepository: String,
    dockerImageTag: String,
    outcome: DefinitionProcessingOutcome,
  ): Array<MetricAttribute> {
    val metricAttributes: MutableList<MetricAttribute> = ArrayList()
    metricAttributes.add(MetricAttribute("status", outcome.status))
    metricAttributes.add(MetricAttribute("outcome", outcome.toString()))
    // Don't add the docker repository or version if the outcome is that version is unchanged -
    // this blows up the number of unique metrics per hour and is not that useful
    if (outcome != DefinitionProcessingSuccessOutcome.VERSION_UNCHANGED) {
      metricAttributes.add(MetricAttribute("docker_repository", dockerRepository))
      metricAttributes.add(MetricAttribute("docker_image_tag", dockerImageTag))
    }
    return metricAttributes.toTypedArray()
  }

  interface DefinitionProcessingOutcome {
    val status: String
  }

  enum class DefinitionProcessingSuccessOutcome : DefinitionProcessingOutcome {
    INITIAL_VERSION_ADDED,
    DEFAULT_VERSION_UPDATED,
    REFRESH_VERSION,
    VERSION_UNCHANGED,
    ;

    override val status: String
      get() = "ok"
  }

  enum class DefinitionProcessingFailureReason : DefinitionProcessingOutcome {
    INCOMPATIBLE_PROTOCOL_VERSION,
    DEFINITION_CONVERSION_FAILED,
    INCOMPATIBLE_AIRBYTE_VERSION,
    ;

    override val status: String
      get() = "failed"
  }
}
