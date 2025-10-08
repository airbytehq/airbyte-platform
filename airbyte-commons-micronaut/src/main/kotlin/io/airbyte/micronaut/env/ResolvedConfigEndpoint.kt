/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.micronaut.env

import io.micronaut.context.env.Environment
import io.micronaut.context.env.PropertySource
import io.micronaut.management.endpoint.annotation.Endpoint
import io.micronaut.management.endpoint.annotation.Read
import io.micronaut.management.endpoint.annotation.Selector

internal const val MASK_VALUE = "*****"
internal const val MISSING_DEFAULT_VALUE = "PLACEHOLDER_MISSING_DEFAULT_VALUE"
internal const val UNMASKED_LENGTH = 4

private val ALWAYS_MASK_PATTERNS =
  setOf("password", "credential", "certificate", "key", "secret", "token", "auth", "pwd", "pass", "cert", "pem", "session", "pat", "account")
    .map { ".*$it.*".toRegex(RegexOption.IGNORE_CASE).toPattern() }
    .toSet()

/**
 * Custom Micronaut management endpoint that exposes the resolved configuration pieced together from
 * all the property sources loaded by the application.  The result is a JSON document that shows
 * each configuration property with its final value (masked) and the property source for debugging purposes.
 *
 * To enable this endpoint for debugging purposes, set the `endpoints.resolved-config.enabled` property to `true`
 * in the `application.yml` file OR set the `ENDPOINTS_RESOLVED_CONFIG_ENABLED` environment variable to `true`.
 */
@Endpoint(id = "resolved-config", defaultEnabled = false, defaultSensitive = true)
class ResolvedConfigEndpoint(
  private val environment: Environment,
) {
  private val exclusions = { name: String -> ALWAYS_MASK_PATTERNS.any { p -> p.matcher(name).matches() } }

  /**
   * Returns the resolved configuration by gathering all configuration property keys and values from the configured
   * property sources in order of priority (lowest/the least priority first).
   *
   * @return a map of the final resolved configuration value for each key detected in each property source, with the highest
   *  priority property source winning in the case of multiple overrides of the same configuration key.
   */
  @Read
  fun getResolvedConfiguration(): Map<String, ResolvedConfigurationDetails> =
    environment.propertySources
      .sortedBy { it.order }
      .flatMap(::toResolvedConfiguration)
      .associate { it.key to it.details }

  /**
   * Returns the entire audit trail for a specific configuration property key.  If multiple property sources contain the same
   * key (e.g. there are overrides of the configuration value), each is returned in reverse priority order (the first element in the
   * list comes from the highest priority property source, or in other words, the actual value that Micronaut will use).  This endpoint
   * can be used to dig into a particular property to see where it is defined.
   *
   * @param property The property key (e.g. airbyte.some.nested.property.key)
   * @return The list of values associated with that property key from each property source where it is contained in order from highest
   *  priority property source to lowest.
   */
  @Read
  fun getResolvedConfigurationProperty(
    @Selector property: String,
  ): List<ResolvedConfiguration> =
    environment.propertySources
      .sortedBy { -it.order } // reverse the order so the highest priority is listed first
      .map { propertySource -> toResolvedConfiguration(propertySource, property) }
      .flatten()

  internal fun maskValue(
    value: Any?,
    fullMask: Boolean,
  ): String =
    if (value == null) {
      "null"
    } else if (value.toString().isEmpty()) {
      ""
    } else if (value is Number) {
      value.toString()
    } else {
      // Values may be an array or list, so try to resolve each enumerated value separately and then re-join
      val propertyValues =
        if (value is Array<*>) {
          value.toList()
        } else {
          value as? List<*> ?: listOf(value.toString())
        }

      propertyValues
        .map { v ->
          val resolvedPropertyValue = resolvePropertyValue(v.toString())
          if (resolvedPropertyValue == MISSING_DEFAULT_VALUE) {
            resolvedPropertyValue
          } else if (fullMask) {
            MASK_VALUE
          } else {
            val length = resolvedPropertyValue.length
            when {
              length == 0 -> ""
              length <= UNMASKED_LENGTH -> MASK_VALUE
              else -> MASK_VALUE + resolvedPropertyValue.substring(length - UNMASKED_LENGTH, length)
            }
          }
        }.filter { it.trim().isNotBlank() }
        .joinToString(separator = ",")
    }

  internal fun resolvePropertyValue(v: Any): String {
    val resolved = environment.placeholderResolver.resolvePlaceholders(v.toString()).orElse(v.toString())
    // Property placeholder values that do not have a default declared in the placeholder (e.g. ${PROP:}) and no
    // value is present in the environment should just be replaced with a marker to indicate that they are missing.
    // Otherwise, return the resolved value.
    return if (resolved.startsWith("\${")) {
      MISSING_DEFAULT_VALUE
    } else {
      resolved
    }
  }

  internal fun toResolvedConfiguration(
    propertySource: PropertySource,
    propertyFilter: String? = null,
  ) = propertySource
    .filter { property -> propertyFilter == null || property == propertyFilter }
    .mapNotNull {
      if (it == null) {
        return@mapNotNull null
      }

      val fullMask = exclusions.invoke(it)

      ResolvedConfiguration(
        key = it,
        details = ResolvedConfigurationDetails(value = maskValue(propertySource.get(it), fullMask), location = propertySource.origin.location()),
      )
    }
}

data class ResolvedConfiguration(
  val key: String,
  val details: ResolvedConfigurationDetails,
)

data class ResolvedConfigurationDetails(
  val value: Any,
  val location: String,
)
