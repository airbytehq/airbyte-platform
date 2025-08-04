/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.helpers

import io.airbyte.api.client.AirbyteApiClient
import io.airbyte.api.client.generated.AttemptApi
import io.airbyte.api.client.model.generated.AttemptStats
import io.airbyte.api.client.model.generated.GetAttemptStatsRequestBody
import io.micronaut.http.HttpStatus
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import org.mockito.Mock
import org.mockito.Mockito
import org.mockito.junit.jupiter.MockitoExtension
import org.openapitools.client.infrastructure.ClientException

@ExtendWith(MockitoExtension::class)
internal class ProgressCheckerTest {
  @Mock
  private lateinit var mAirbyteApiClient: AirbyteApiClient

  @Mock
  private lateinit var mAttemptApi: AttemptApi

  @Mock
  private lateinit var mPredicates: ProgressCheckerPredicates

  @BeforeEach
  fun setup() {
    Mockito.`when`(mAirbyteApiClient.attemptApi).thenReturn(mAttemptApi)
  }

  @Test
  @Throws(Exception::class)
  fun noRespReturnsFalse() {
    val activity = ProgressChecker(mAirbyteApiClient, mPredicates)
    val requestBody = GetAttemptStatsRequestBody(Fixtures.jobId1, Fixtures.attemptNo1)
    Mockito
      .`when`(mAttemptApi.getAttemptCombinedStats(requestBody))
      .thenReturn(null)

    val result = activity.check(Fixtures.jobId1, Fixtures.attemptNo1)

    Assertions.assertFalse(result)
  }

  @ParameterizedTest
  @ValueSource(booleans = [true, false])
  @Throws(Exception::class)
  fun respReturnsCheckedValue(madeProgress: Boolean) {
    val activity = ProgressChecker(mAirbyteApiClient, mPredicates)
    val requestBody = GetAttemptStatsRequestBody(Fixtures.jobId1, Fixtures.attemptNo1)
    val stats = AttemptStats()
    Mockito
      .`when`(mAttemptApi.getAttemptCombinedStats(requestBody))
      .thenReturn(stats)
    Mockito
      .`when`(mPredicates.test(stats))
      .thenReturn(madeProgress)

    val result = activity.check(Fixtures.jobId1, Fixtures.attemptNo1)

    Assertions.assertEquals(madeProgress, result)
  }

  @Test
  @Throws(Exception::class)
  fun notFoundsAreTreatedAsNoProgress() {
    val activity = ProgressChecker(mAirbyteApiClient, mPredicates)
    val requestBody = GetAttemptStatsRequestBody(Fixtures.jobId1, Fixtures.attemptNo1)
    Mockito
      .`when`(mAttemptApi.getAttemptCombinedStats(requestBody))
      .thenThrow(ClientException("Not Found.", HttpStatus.NOT_FOUND.code, null))

    val result = activity.check(Fixtures.jobId1, Fixtures.attemptNo1)

    Assertions.assertFalse(result)
  }

  private object Fixtures {
    var jobId1: Long = 1

    var attemptNo1: Int = 0
  }
}
