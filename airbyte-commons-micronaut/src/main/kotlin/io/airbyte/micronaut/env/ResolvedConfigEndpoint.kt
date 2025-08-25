/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.micronaut.env

import io.micronaut.context.env.Environment
import io.micronaut.management.endpoint.annotation.Endpoint
import io.micronaut.management.endpoint.annotation.Read

private const val MASK_VALUE = "*****"
private const val UNMASKED_LENGTH = 4

/**
 * Custom Micronaut management endpoint that exposes the resolved configuration pieced together from
 * all the property sources loaded by the application.  The result is a JSON document that shows
 * each configuration property with its final value (masked) and the property source for debugging purposes.
 */
@Endpoint(id = "resolved-config", defaultEnabled = true, defaultSensitive = true)
class ResolvedConfigEndpoint(
  private val environment: Environment,
) {
  @Read
  fun getResolvedConfiguration(): Map<String, ResolvedConfigurationDetails> =
    environment.propertySources
      .sortedBy { it.order }
      .flatMap { propertySource ->
        propertySource.mapNotNull {
          if (it == null) {
            return@mapNotNull null
          }

          ResolvedConfiguration(
            key = it,
            details = ResolvedConfigurationDetails(value = maskValue(propertySource.get(it)), location = propertySource.origin.location()),
          )
        }
      }.associate { it.key to it.details }

  internal fun maskValue(value: Any?): String {
    if (value == null) {
      return "null"
    }
    val resolvedPropertyValue = environment.placeholderResolver.resolvePlaceholders(value.toString()).orElse(value.toString())
    val length = resolvedPropertyValue.length
    return if (length <= UNMASKED_LENGTH) MASK_VALUE else MASK_VALUE + resolvedPropertyValue.substring(length - UNMASKED_LENGTH, length)
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
