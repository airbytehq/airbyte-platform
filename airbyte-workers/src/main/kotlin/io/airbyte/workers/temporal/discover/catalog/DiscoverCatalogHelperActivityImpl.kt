/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.discover.catalog

import io.airbyte.api.client.AirbyteApiClient
import io.airbyte.api.client.model.generated.PostprocessDiscoveredCatalogRequestBody
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.metrics.MetricClient
import io.airbyte.workers.helper.CatalogDiffConverter
import io.airbyte.workers.models.PostprocessCatalogInput
import io.airbyte.workers.models.PostprocessCatalogOutput
import io.airbyte.workers.models.PostprocessCatalogOutput.Companion.failure
import io.airbyte.workers.models.PostprocessCatalogOutput.Companion.success
import jakarta.inject.Singleton
import java.util.Objects
import java.util.UUID

@Singleton
class DiscoverCatalogHelperActivityImpl(
  private val airbyteApiClient: AirbyteApiClient,
  private val featureFlagClient: FeatureFlagClient?,
  private val metricClient: MetricClient?,
) : DiscoverCatalogHelperActivity {
  override fun postprocess(input: PostprocessCatalogInput): PostprocessCatalogOutput {
    try {
      Objects.requireNonNull<UUID?>(input.connectionId)

      if (input.catalogId == null) {
        return success(null)
      }

      val reqBody =
        PostprocessDiscoveredCatalogRequestBody(
          input.catalogId,
          input.connectionId!!,
        )

      val resp = airbyteApiClient.connectionApi.postprocessDiscoveredCatalogForConnection(reqBody)

      val domainDiff = if (resp.appliedDiff != null) CatalogDiffConverter.toDomain(resp.appliedDiff!!) else null

      return success(domainDiff)
    } catch (e: Exception) {
      return failure(e)
    }
  }
}
