/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.cron.jobs

import io.airbyte.config.persistence.WorkloadPurge
import io.airbyte.featureflag.Empty
import io.airbyte.featureflag.EnableWorkloadPurge
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.featureflag.WorkloadPurgeRunRequest
import io.airbyte.metrics.MetricClient
import io.airbyte.metrics.OssMetricsRegistry
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

internal class WorkloadPurgeRequestTest {
  private val purge = mockk<WorkloadPurge>()
  private val metrics = mockk<MetricClient>(relaxed = true)
  private val featureFlags = mockk<FeatureFlagClient>()
  private var requestToken = ""
  private lateinit var workflow: WorkloadPurgeWorkflow

  @BeforeEach
  fun setup() {
    every { featureFlags.stringVariation(WorkloadPurgeRunRequest, Empty) } answers { requestToken }
    every { featureFlags.boolVariation(EnableWorkloadPurge, Empty) } returns true
    every { purge.prune(any()) } returns 0
    workflow = WorkloadPurgeWorkflow(purge, metrics, featureFlags)
  }

  @Test
  fun `new request token runs one purge and repeated token is ignored`() {
    requestToken = "request-1"

    workflow.pollRunRequests()
    workflow.pollRunRequests()

    verify(exactly = 1) { purge.prune(any()) }
  }

  @Test
  fun `another request token runs another purge`() {
    requestToken = "request-1"
    workflow.pollRunRequests()
    requestToken = "request-2"
    workflow.pollRunRequests()
    workflow.pollRunRequests()

    verify(exactly = 2) { purge.prune(any()) }
  }

  @Test
  fun `blank request tokens do not run a purge`() {
    listOf("", " ", "\t").forEach {
      requestToken = it
      workflow.pollRunRequests()
    }

    verify(exactly = 0) { purge.prune(any()) }
    verify(exactly = 0) { featureFlags.boolVariation(EnableWorkloadPurge, Empty) }
  }

  @Test
  fun `disabled feature flag leaves the request pending until enabled`() {
    requestToken = "pending"
    every { featureFlags.boolVariation(EnableWorkloadPurge, Empty) } returns false

    workflow.pollRunRequests()
    workflow.pollRunRequests()

    verify(exactly = 0) { purge.prune(any()) }
    every { featureFlags.boolVariation(EnableWorkloadPurge, Empty) } returns true
    workflow.pollRunRequests()
    workflow.pollRunRequests()

    verify(exactly = 1) { purge.prune(any()) }
  }

  @Test
  fun `accepted request executes once with one authoritative enable check`() {
    requestToken = "accepted-request"
    every { featureFlags.boolVariation(EnableWorkloadPurge, Empty) } returnsMany listOf(true, false)

    workflow.pollRunRequests()
    workflow.pollRunRequests()

    verify(exactly = 1) { purge.prune(any()) }
    verify(exactly = 1) { featureFlags.boolVariation(EnableWorkloadPurge, Empty) }
  }

  @Test
  fun `request token retained at process startup is not replayed`() {
    requestToken = "historical-request"
    workflow = WorkloadPurgeWorkflow(purge, metrics, featureFlags)

    workflow.pollRunRequests()

    verify(exactly = 0) { purge.prune(any()) }
    requestToken = "new-request"
    workflow.pollRunRequests()

    verify(exactly = 1) { purge.prune(any()) }
  }

  @Test
  fun `failed request is consumed until another token arrives`() {
    requestToken = "failed-request"
    every { purge.prune(any()) } throws IllegalStateException("permanent database failure")

    assertThrows<IllegalStateException> { workflow.pollRunRequests() }
    workflow.pollRunRequests()

    verify(exactly = 1) { purge.prune(any()) }
    verify(exactly = 1) { metrics.count(OssMetricsRegistry.WORKLOAD_PRUNING_FAILURE) }

    every { purge.prune(any()) } returns 0
    requestToken = "retry-request"
    workflow.pollRunRequests()

    verify(exactly = 2) { purge.prune(any()) }
  }

  @Test
  fun `daily purge does not consume a pending request token`() {
    requestToken = "pending-request"

    workflow.pruneRecords()
    workflow.pollRunRequests()

    verify(exactly = 2) { purge.prune(any()) }
  }

  @ParameterizedTest
  @ValueSource(booleans = [true, false])
  fun `requested and daily purges share the same lock`(requestedFirst: Boolean) {
    requestToken = "request-1"
    val firstEntered = CountDownLatch(1)
    val secondEntered = CountDownLatch(1)
    val release = CountDownLatch(1)
    var calls = 0
    every { purge.prune(any()) } answers {
      if (++calls == 1) {
        firstEntered.countDown()
        check(release.await(10, TimeUnit.SECONDS)) { "Test did not release the first purge" }
      } else {
        secondEntered.countDown()
      }
      0
    }

    val executor = Executors.newFixedThreadPool(2)
    try {
      val first =
        executor.submit {
          if (requestedFirst) workflow.pollRunRequests() else workflow.pruneRecords()
        }
      assertTrue(firstEntered.await(5, TimeUnit.SECONDS), "First purge did not begin")
      val secondRequested = CountDownLatch(1)
      val second =
        executor.submit {
          secondRequested.countDown()
          if (requestedFirst) workflow.pruneRecords() else workflow.pollRunRequests()
        }
      assertTrue(secondRequested.await(5, TimeUnit.SECONDS), "Second entry point was not called")
      assertFalse(secondEntered.await(200, TimeUnit.MILLISECONDS), "Requested and daily purges overlapped")
      release.countDown()
      first.get(5, TimeUnit.SECONDS)
      second.get(5, TimeUnit.SECONDS)
      assertTrue(secondEntered.count == 0L, "Second purge did not begin after the first finished")
      verify(exactly = 2) { purge.prune(any()) }
    } finally {
      release.countDown()
      executor.shutdownNow()
    }
  }
}
