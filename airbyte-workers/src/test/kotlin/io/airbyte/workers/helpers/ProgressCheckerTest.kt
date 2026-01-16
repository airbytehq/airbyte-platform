/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.helpers

import io.airbyte.api.client.AirbyteApiClient
import io.airbyte.api.client.generated.AttemptApi
import io.airbyte.api.client.model.generated.AttemptStats
import io.airbyte.api.client.model.generated.GetAttemptStatsRequestBody
import io.micronaut.http.HttpStatus
import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import org.openapitools.client.infrastructure.ClientException

internal class ProgressCheckerTest {
  private lateinit var mAirbyteApiClient: AirbyteApiClient
  private lateinit var mAttemptApi: AttemptApi
  private lateinit var mPredicates: ProgressCheckerPredicates

  @BeforeEach
  fun setup() {
    mAirbyteApiClient = mockk()
    mAttemptApi = mockk()
    mPredicates = mockk(relaxed = true)
    every { mAirbyteApiClient.attemptApi } returns mAttemptApi
  }

  @Test
  fun noRespReturnsFalse() {
    val activity = ProgressChecker(mAirbyteApiClient, mPredicates)
    val requestBody = GetAttemptStatsRequestBody(Fixtures.jobId1, Fixtures.attemptNo1)
    every { mAttemptApi.getAttemptCombinedStats(requestBody) } returns mockk(relaxed = true)

    val result = activity.check(Fixtures.jobId1, Fixtures.attemptNo1)

    Assertions.assertFalse(result)
  }

  @ParameterizedTest
  @ValueSource(booleans = [true, false])
  fun respReturnsCheckedValue(madeProgress: Boolean) {
    val activity = ProgressChecker(mAirbyteApiClient, mPredicates)
    val requestBody = GetAttemptStatsRequestBody(Fixtures.jobId1, Fixtures.attemptNo1)
    val stats = AttemptStats()
    every { mAttemptApi.getAttemptCombinedStats(requestBody) } returns stats
    every { mPredicates.test(stats) } returns madeProgress

    val result = activity.check(Fixtures.jobId1, Fixtures.attemptNo1)

    Assertions.assertEquals(madeProgress, result)
  }

  @Test
  fun notFoundsAreTreatedAsNoProgress() {
    val activity = ProgressChecker(mAirbyteApiClient, mPredicates)
    val requestBody = GetAttemptStatsRequestBody(Fixtures.jobId1, Fixtures.attemptNo1)
    every { mAttemptApi.getAttemptCombinedStats(requestBody) } throws ClientException("Not Found.", HttpStatus.NOT_FOUND.code, null)

    val result = activity.check(Fixtures.jobId1, Fixtures.attemptNo1)

    Assertions.assertFalse(result)
  }

  private object Fixtures {
    var jobId1: Long = 1

    var attemptNo1: Int = 0
  }
}
