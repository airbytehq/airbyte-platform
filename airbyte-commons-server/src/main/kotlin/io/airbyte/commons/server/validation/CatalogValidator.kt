/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.validation

import com.google.common.collect.Iterators
import io.airbyte.api.model.generated.AirbyteCatalog
import io.airbyte.commons.server.validation.CatalogValidator.Constants.FIELD_COUNT_LIMIT_KEY
import io.airbyte.commons.server.validation.CatalogValidator.Constants.FIELD_COUNT_PROVIDED_KEY
import io.airbyte.commons.server.validation.CatalogValidator.Constants.PROPERTIES_KEY
import io.airbyte.featureflag.ConnectionFieldLimitOverride
import io.airbyte.featureflag.Context
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.metrics.lib.MetricAttribute
import io.airbyte.metrics.lib.MetricClient
import io.airbyte.metrics.lib.OssMetricsRegistry
import io.micronaut.context.annotation.Value
import jakarta.inject.Singleton

/**
 * Singleton for encapsulating catalog validation logic. Please add new catalog validations here.
 */
@Singleton
class CatalogValidator(
  @Value("\${airbyte.server.connection.limits.max-fields-per-connection}") private val maxFieldLimit: Int,
  private val metricClient: MetricClient,
  private val featureFlagClient: FeatureFlagClient,
) {
  fun fieldCount(
    catalog: AirbyteCatalog,
    ctx: Context,
  ): ValidationError? {
    val fieldLimit = resolveLimit(ctx)
    val fieldCount =
      catalog.streams
        .filter { s -> s.config.selected }
        .sumOf { s ->
          if (s.config.fieldSelectionEnabled == true) {
            s.config.selectedFields.size
          } else {
            val fieldNames: Iterator<String> =
              if (s.stream.jsonSchema.get(PROPERTIES_KEY) != null) {
                s.stream.jsonSchema
                  .get(PROPERTIES_KEY)
                  .fieldNames()
              } else {
                emptyList<String>().iterator()
              }
            Iterators.size(fieldNames)
          }
        }

    if (fieldCount <= fieldLimit) return null

    metricClient.distribution(
      OssMetricsRegistry.EXCESSIVE_CATALOG_SIZE,
      fieldCount.toDouble(),
      MetricAttribute(FIELD_COUNT_LIMIT_KEY, maxFieldLimit.toString()),
      MetricAttribute(FIELD_COUNT_PROVIDED_KEY, fieldCount.toString()),
    )

    return ValidationError(
      "Total number of selected fields ($fieldCount) is greater than the limit ($fieldLimit) configured for this workspace. " +
        "Please de-select fields or see documentation about raising the limit.",
    )
  }

  private fun resolveLimit(ctx: Context): Int {
    val override = featureFlagClient.intVariation(ConnectionFieldLimitOverride, ctx)

    return if (override > 0) {
      override
    } else {
      maxFieldLimit
    }
  }

  object Constants {
    // key for node whose keys represents the number of columns on a stream
    const val PROPERTIES_KEY = "properties"
    const val FIELD_COUNT_PROVIDED_KEY = "field_count_provided"
    const val FIELD_COUNT_LIMIT_KEY = "field_count_limit"
  }
}
