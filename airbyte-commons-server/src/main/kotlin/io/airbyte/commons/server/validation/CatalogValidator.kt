/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.validation

import io.airbyte.api.model.generated.AirbyteCatalog
import io.airbyte.commons.server.runtime.AirbyteServerConfiguration
import io.airbyte.commons.server.validation.CatalogValidator.Constants.FIELD_COUNT_LIMIT_KEY
import io.airbyte.commons.server.validation.CatalogValidator.Constants.FIELD_COUNT_PROVIDED_KEY
import io.airbyte.commons.server.validation.CatalogValidator.Constants.PROPERTIES_KEY
import io.airbyte.featureflag.ConnectionFieldLimitOverride
import io.airbyte.featureflag.Context
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.metrics.MetricAttribute
import io.airbyte.metrics.MetricClient
import io.airbyte.metrics.OssMetricsRegistry
import jakarta.inject.Singleton

/**
 * Singleton for encapsulating catalog validation logic. Please add new catalog validations here.
 */
@Singleton
class CatalogValidator(
  private val airbyteServerConfiguration: AirbyteServerConfiguration,
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
            fieldNames.asSequence().count()
          }
        }

    if (fieldCount <= fieldLimit) return null

    metricClient.distribution(
      OssMetricsRegistry.EXCESSIVE_CATALOG_SIZE,
      fieldCount.toDouble(),
      MetricAttribute(
        FIELD_COUNT_LIMIT_KEY,
        airbyteServerConfiguration.connectionLimits.limits.maxFieldsPerConnection
          .toString(),
      ),
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
      airbyteServerConfiguration.connectionLimits.limits.maxFieldsPerConnection
        .toInt()
    }
  }

  object Constants {
    // key for node whose keys represents the number of columns on a stream
    const val PROPERTIES_KEY = "properties"
    const val FIELD_COUNT_PROVIDED_KEY = "field_count_provided"
    const val FIELD_COUNT_LIMIT_KEY = "field_count_limit"
  }
}
