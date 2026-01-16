/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.discover.catalog

import io.airbyte.api.client.AirbyteApiClient
import io.airbyte.api.client.generated.ConnectionApi
import io.airbyte.api.client.model.generated.CatalogDiff
import io.airbyte.api.client.model.generated.PostprocessDiscoveredCatalogRequestBody
import io.airbyte.api.client.model.generated.PostprocessDiscoveredCatalogResult
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.featureflag.TestClient
import io.airbyte.metrics.MetricClient
import io.airbyte.workers.helper.CatalogDiffConverter
import io.airbyte.workers.models.PostprocessCatalogInput
import io.airbyte.workers.models.PostprocessCatalogOutput
import io.mockk.every
import io.mockk.mockk
import io.mockk.spyk
import io.mockk.verify
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.io.IOException
import java.util.UUID

class DiscoverCatalogHelperActivityTest {
  private val metricClient: MetricClient = mockk()
  private val featureFlagClient: FeatureFlagClient = spyk(TestClient())
  private val airbyteApiClient: AirbyteApiClient = mockk()
  private val connectionApi: ConnectionApi = mockk()
  private lateinit var discoverCatalogReportActivity: DiscoverCatalogHelperActivityImpl

  @BeforeEach
  fun init() {
    every { airbyteApiClient.connectionApi }.returns(connectionApi)
    discoverCatalogReportActivity =
      spyk(
        DiscoverCatalogHelperActivityImpl(
          airbyteApiClient,
          featureFlagClient,
          metricClient,
        ),
      )
  }

  @Test
  fun postprocessHappyPath() {
    val diff1: CatalogDiff =
      mockk {
        every { transforms } returns listOf()
      }
    val apiResult: PostprocessDiscoveredCatalogResult =
      mockk {
        every { appliedDiff } returns diff1
      }
    every { connectionApi.postprocessDiscoveredCatalogForConnection(any()) } returns apiResult

    val input = PostprocessCatalogInput(UUID.randomUUID(), UUID.randomUUID())
    val result = discoverCatalogReportActivity.postprocess(input)

    val expectedReqBody = PostprocessDiscoveredCatalogRequestBody(input.catalogId!!, input.connectionId!!)

    verify { connectionApi.postprocessDiscoveredCatalogForConnection(eq(expectedReqBody)) }

    val expected = PostprocessCatalogOutput.success(CatalogDiffConverter.toDomain(diff1))
    Assertions.assertEquals(expected, result)
    Assertions.assertTrue(result.isSuccess)
    Assertions.assertFalse(result.isFailure)
  }

  @Test
  fun postprocessExceptionalPath() {
    val exception = IOException("not happy")

    val apiResult: PostprocessDiscoveredCatalogResult =
      mockk {
        every { appliedDiff } throws exception
      }
    every { connectionApi.postprocessDiscoveredCatalogForConnection(any()) } returns apiResult

    val input = PostprocessCatalogInput(UUID.randomUUID(), UUID.randomUUID())
    val result = discoverCatalogReportActivity.postprocess(input)

    val expectedReqBody = PostprocessDiscoveredCatalogRequestBody(input.catalogId!!, input.connectionId!!)

    verify { connectionApi.postprocessDiscoveredCatalogForConnection(eq(expectedReqBody)) }

    val expected = PostprocessCatalogOutput.failure(exception)
    Assertions.assertEquals(expected, result)
    Assertions.assertFalse(result.isSuccess)
    Assertions.assertTrue(result.isFailure)
  }
}
