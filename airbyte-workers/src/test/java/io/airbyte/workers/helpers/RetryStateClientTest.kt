/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.helpers

import io.airbyte.api.client.AirbyteApiClient
import io.airbyte.api.client.generated.JobRetryStatesApi
import io.airbyte.api.client.generated.WorkspaceApi
import io.airbyte.api.client.model.generated.JobIdRequestBody
import io.airbyte.api.client.model.generated.RetryStateRead
import io.airbyte.featureflag.CompleteFailureBackoffBase
import io.airbyte.featureflag.CompleteFailureBackoffMaxInterval
import io.airbyte.featureflag.CompleteFailureBackoffMinInterval
import io.airbyte.featureflag.Context
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.featureflag.SuccessiveCompleteFailureLimit
import io.airbyte.featureflag.SuccessivePartialFailureLimit
import io.airbyte.featureflag.TestClient
import io.airbyte.featureflag.TotalCompleteFailureLimit
import io.airbyte.featureflag.TotalPartialFailureLimit
import io.airbyte.featureflag.Workspace
import io.micronaut.http.HttpStatus
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.ArgumentMatchers
import org.mockito.Mockito
import org.openapitools.client.infrastructure.ClientException
import java.time.Duration
import java.util.UUID
import java.util.concurrent.ThreadLocalRandom

internal class RetryStateClientTest {
  private lateinit var mAirbyteApiClient: AirbyteApiClient
  private lateinit var mJobRetryStatesApi: JobRetryStatesApi
  private lateinit var mWorkspaceApi: WorkspaceApi
  private lateinit var mFeatureFlagClient: FeatureFlagClient

  @BeforeEach
  fun setup() {
    mAirbyteApiClient = Mockito.mock(AirbyteApiClient::class.java)
    mJobRetryStatesApi = Mockito.mock(JobRetryStatesApi::class.java)
    mWorkspaceApi = Mockito.mock(WorkspaceApi::class.java)
    mFeatureFlagClient = Mockito.mock(TestClient::class.java)
    Mockito.`when`(mAirbyteApiClient.jobRetryStatesApi).thenReturn(mJobRetryStatesApi)
    Mockito.`when`(mAirbyteApiClient.workspaceApi).thenReturn(mWorkspaceApi)
  }

  @Test
  fun hydratesBackoffAndLimitsFromConstructor() {
    // Mock feature flag client to return constructor values (i.e., don't override them)
    Mockito
      .`when`(
        mFeatureFlagClient.intVariation(
          SuccessiveCompleteFailureLimit,
          Fixtures.testContext,
        ),
      ).thenReturn(Fixtures.successiveCompleteFailureLimit)

    Mockito
      .`when`(
        mFeatureFlagClient.intVariation(
          TotalCompleteFailureLimit,
          Fixtures.testContext,
        ),
      ).thenReturn(Fixtures.totalCompleteFailureLimit)

    Mockito
      .`when`(
        mFeatureFlagClient.intVariation(
          SuccessivePartialFailureLimit,
          Fixtures.testContext,
        ),
      ).thenReturn(Fixtures.successivePartialFailureLimit)

    Mockito
      .`when`(
        mFeatureFlagClient.intVariation(
          TotalPartialFailureLimit,
          Fixtures.testContext,
        ),
      ).thenReturn(Fixtures.totalPartialFailureLimit)

    Mockito
      .`when`(
        mFeatureFlagClient.intVariation(
          CompleteFailureBackoffMinInterval,
          Fixtures.testContext,
        ),
      ).thenReturn(Fixtures.minInterval)

    Mockito
      .`when`(
        mFeatureFlagClient.intVariation(
          CompleteFailureBackoffMaxInterval,
          Fixtures.testContext,
        ),
      ).thenReturn(Fixtures.maxInterval)

    Mockito
      .`when`(
        mFeatureFlagClient.intVariation(
          CompleteFailureBackoffBase,
          Fixtures.testContext,
        ),
      ).thenReturn(Fixtures.base)

    val client =
      RetryStateClient(
        mAirbyteApiClient,
        mFeatureFlagClient,
        Fixtures.successiveCompleteFailureLimit,
        Fixtures.totalCompleteFailureLimit,
        Fixtures.successivePartialFailureLimit,
        Fixtures.totalPartialFailureLimit,
        Fixtures.minInterval,
        Fixtures.maxInterval,
        Fixtures.base,
      )

    val manager = client.hydrateRetryState(Fixtures.jobId, Fixtures.workspaceId)

    Assertions.assertEquals(Fixtures.successiveCompleteFailureLimit, manager.successiveCompleteFailureLimit)
    Assertions.assertEquals(Fixtures.totalCompleteFailureLimit, manager.totalCompleteFailureLimit)
    Assertions.assertEquals(Fixtures.successivePartialFailureLimit, manager.successivePartialFailureLimit)
    Assertions.assertEquals(Fixtures.totalPartialFailureLimit, manager.totalPartialFailureLimit)

    val backoffPolicy = manager.completeFailureBackoffPolicy
    Assertions.assertEquals(Duration.ofSeconds(Fixtures.minInterval.toLong()), backoffPolicy!!.minInterval)
    Assertions.assertEquals(Duration.ofSeconds(Fixtures.maxInterval.toLong()), backoffPolicy.maxInterval)
    Assertions.assertEquals(Fixtures.base.toLong(), backoffPolicy.base)
  }

  @Test
  fun featureFlagsOverrideValues() {
    val client =
      RetryStateClient(
        mAirbyteApiClient,
        mFeatureFlagClient,
        Fixtures.successiveCompleteFailureLimit,
        Fixtures.totalCompleteFailureLimit,
        Fixtures.successivePartialFailureLimit,
        Fixtures.totalPartialFailureLimit,
        Fixtures.minInterval,
        Fixtures.maxInterval,
        Fixtures.base,
      )

    Mockito
      .`when`(
        mFeatureFlagClient.intVariation(
          SuccessiveCompleteFailureLimit,
          Fixtures.testContext,
        ),
      ).thenReturn(91)

    Mockito
      .`when`(
        mFeatureFlagClient.intVariation(
          TotalCompleteFailureLimit,
          Fixtures.testContext,
        ),
      ).thenReturn(92)

    Mockito
      .`when`(
        mFeatureFlagClient.intVariation(
          SuccessivePartialFailureLimit,
          Fixtures.testContext,
        ),
      ).thenReturn(93)

    Mockito
      .`when`(
        mFeatureFlagClient.intVariation(
          TotalPartialFailureLimit,
          Fixtures.testContext,
        ),
      ).thenReturn(94)

    Mockito
      .`when`(
        mFeatureFlagClient.intVariation(
          CompleteFailureBackoffMinInterval,
          Fixtures.testContext,
        ),
      ).thenReturn(0)

    Mockito
      .`when`(
        mFeatureFlagClient.intVariation(
          CompleteFailureBackoffMaxInterval,
          Fixtures.testContext,
        ),
      ).thenReturn(96)

    Mockito
      .`when`(
        mFeatureFlagClient.intVariation(
          CompleteFailureBackoffBase,
          Fixtures.testContext,
        ),
      ).thenReturn(97)

    val manager = client.hydrateRetryState(Fixtures.jobId, Fixtures.workspaceId)

    Assertions.assertEquals(91, manager.successiveCompleteFailureLimit)
    Assertions.assertEquals(92, manager.totalCompleteFailureLimit)
    Assertions.assertEquals(93, manager.successivePartialFailureLimit)
    Assertions.assertEquals(94, manager.totalPartialFailureLimit)

    val backoffPolicy = manager.completeFailureBackoffPolicy
    Assertions.assertEquals(Duration.ofSeconds(0), backoffPolicy!!.minInterval)
    Assertions.assertEquals(Duration.ofSeconds(96), backoffPolicy.maxInterval)
    Assertions.assertEquals(97, backoffPolicy.base)
    Assertions.assertEquals(Duration.ZERO, backoffPolicy.getBackoff(0))
    Assertions.assertEquals(Duration.ZERO, backoffPolicy.getBackoff(1))
    Assertions.assertEquals(Duration.ZERO, backoffPolicy.getBackoff(2))
    Assertions.assertEquals(Duration.ZERO, backoffPolicy.getBackoff(3))
  }

  @Test
  @Throws(Exception::class)
  fun hydratesFailureCountsFromApiIfPresent() {
    val retryStateRead =
      RetryStateRead(
        UUID.randomUUID(),
        UUID.randomUUID(),
        Fixtures.jobId,
        Fixtures.successiveCompleteFailures,
        Fixtures.totalCompleteFailures,
        Fixtures.successivePartialFailures,
        Fixtures.totalPartialFailures,
      )

    Mockito
      .`when`(mJobRetryStatesApi.get(ArgumentMatchers.any<JobIdRequestBody>()))
      .thenReturn(retryStateRead)

    val client =
      RetryStateClient(
        mAirbyteApiClient,
        mFeatureFlagClient,
        Fixtures.successiveCompleteFailureLimit,
        Fixtures.totalCompleteFailureLimit,
        Fixtures.successivePartialFailureLimit,
        Fixtures.totalPartialFailureLimit,
        Fixtures.minInterval,
        Fixtures.maxInterval,
        Fixtures.base,
      )

    val manager = client.hydrateRetryState(Fixtures.jobId, Fixtures.workspaceId)

    Assertions.assertEquals(Fixtures.totalCompleteFailures, manager.totalCompleteFailures)
    Assertions.assertEquals(Fixtures.totalPartialFailures, manager.totalPartialFailures)
    Assertions.assertEquals(Fixtures.successiveCompleteFailures, manager.successiveCompleteFailures)
    Assertions.assertEquals(Fixtures.successivePartialFailures, manager.successivePartialFailures)
  }

  @Test
  @Throws(Exception::class)
  fun initializesFailureCountsFreshWhenApiReturnsNothing() {
    Mockito
      .`when`(mJobRetryStatesApi.get(ArgumentMatchers.any<JobIdRequestBody>()))
      .thenThrow(ClientException("Not Found.", HttpStatus.NOT_FOUND.code, null))

    val client =
      RetryStateClient(
        mAirbyteApiClient,
        mFeatureFlagClient,
        Fixtures.successiveCompleteFailureLimit,
        Fixtures.totalCompleteFailureLimit,
        Fixtures.successivePartialFailureLimit,
        Fixtures.totalPartialFailureLimit,
        Fixtures.minInterval,
        Fixtures.maxInterval,
        Fixtures.base,
      )

    val manager = client.hydrateRetryState(Fixtures.jobId, Fixtures.workspaceId)

    Assertions.assertEquals(0, manager.totalCompleteFailures)
    Assertions.assertEquals(0, manager.totalPartialFailures)
    Assertions.assertEquals(0, manager.successiveCompleteFailures)
    Assertions.assertEquals(0, manager.successivePartialFailures)
  }

  @Test
  fun initializesFailureCountsFreshWhenJobIdNull() {
    val client =
      RetryStateClient(
        mAirbyteApiClient,
        mFeatureFlagClient,
        Fixtures.successiveCompleteFailureLimit,
        Fixtures.totalCompleteFailureLimit,
        Fixtures.successivePartialFailureLimit,
        Fixtures.totalPartialFailureLimit,
        Fixtures.minInterval,
        Fixtures.maxInterval,
        Fixtures.base,
      )

    val manager = client.hydrateRetryState(null, Fixtures.workspaceId)

    Assertions.assertEquals(0, manager.totalCompleteFailures)
    Assertions.assertEquals(0, manager.totalPartialFailures)
    Assertions.assertEquals(0, manager.successiveCompleteFailures)
    Assertions.assertEquals(0, manager.successivePartialFailures)
  }

  internal object Fixtures {
    var jobId: Long = ThreadLocalRandom.current().nextLong()
    var workspaceId: UUID = UUID.randomUUID()

    var successiveCompleteFailureLimit: Int = ThreadLocalRandom.current().nextInt()
    var totalCompleteFailureLimit: Int = ThreadLocalRandom.current().nextInt()
    var successivePartialFailureLimit: Int = ThreadLocalRandom.current().nextInt()
    var totalPartialFailureLimit: Int = ThreadLocalRandom.current().nextInt()

    var totalCompleteFailures: Int = ThreadLocalRandom.current().nextInt()
    var totalPartialFailures: Int = ThreadLocalRandom.current().nextInt()
    var successiveCompleteFailures: Int = ThreadLocalRandom.current().nextInt()
    var successivePartialFailures: Int = ThreadLocalRandom.current().nextInt()

    var minInterval: Int = 10
    var maxInterval: Int = 1000
    var base: Int = 2

    val testContext: Context = Workspace(workspaceId)
  }
}
