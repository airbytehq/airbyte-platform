package io.airbyte.metrics.lib

import io.airbyte.commons.envvar.EnvVar

/**
 * Configuration for publishing metrics.
 */
data class DatadogClientConfiguration(
  val ddAgentHost: String = EnvVar.DD_AGENT_HOST.fetch() ?: "",
  val ddPort: String = EnvVar.DD_DOGSTATSD_PORT.fetch() ?: "",
  val publish: Boolean = EnvVar.PUBLISH_METRICS.fetch().toBoolean(),
  val constantTags: List<String> = constantTags(),
)

private fun constantTags(): List<String> {
  val tags = EnvVar.DD_CONSTANT_TAGS.fetch() ?: ""
  return tags.split(",").map { it.trim() }.filter { it.isNotBlank() }.toList()
}
