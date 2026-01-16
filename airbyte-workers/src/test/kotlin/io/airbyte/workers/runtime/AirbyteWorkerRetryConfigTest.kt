/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.runtime

import io.micronaut.context.env.Environment
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import jakarta.inject.Inject
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

@MicronautTest(environments = [Environment.TEST])
internal class AirbyteWorkerRetryConfigDefaultTest {
  @Inject
  private lateinit var airbyteWorkerRetryConfig: AirbyteWorkerRetryConfig

  @Test
  fun testLoadingValuesFromConfig() {
    assertEquals(DEFAULT_WORKER_COMPLETE_FAILURES_BASE_INTERVAL_SECONDS, airbyteWorkerRetryConfig.completeFailures.backoff.base)
    assertEquals(DEFAULT_WORKER_COMPLETE_FAILURES_MAX_INTERVAL_SECONDS, airbyteWorkerRetryConfig.completeFailures.backoff.maxIntervalS)
    assertEquals(DEFAULT_WORKER_COMPLETE_FAILURES_MIN_INTERVAL_SECONDS, airbyteWorkerRetryConfig.completeFailures.backoff.minIntervalS)
    assertEquals(DEFAULT_WORKER_MAX_SUCCESSIVE_COMPLETE_FAILURES, airbyteWorkerRetryConfig.completeFailures.maxSuccessive)
    assertEquals(DEFAULT_WORKER_MAX_TOTAL_COMPLETE_FAILURES, airbyteWorkerRetryConfig.completeFailures.maxTotal)
    assertEquals(DEFAULT_WORKER_MAX_SUCCESSIVE_PARTIAL_FAILURES, airbyteWorkerRetryConfig.partialFailures.maxSuccessive)
    assertEquals(DEFAULT_WORKER_MAX_TOTAL_PARTIAL_FAILURES, airbyteWorkerRetryConfig.partialFailures.maxTotal)
  }
}

@MicronautTest(propertySources = ["classpath:application-worker-retries.yml"])
internal class AirbyteWorkerRetryConfigOverridesTest {
  @Inject
  private lateinit var airbyteWorkerRetryConfig: AirbyteWorkerRetryConfig

  @Test
  fun testLoadingValuesFromConfig() {
    assertEquals(1, airbyteWorkerRetryConfig.completeFailures.backoff.base)
    assertEquals(2, airbyteWorkerRetryConfig.completeFailures.backoff.maxIntervalS)
    assertEquals(3, airbyteWorkerRetryConfig.completeFailures.backoff.minIntervalS)
    assertEquals(4, airbyteWorkerRetryConfig.completeFailures.maxSuccessive)
    assertEquals(5, airbyteWorkerRetryConfig.completeFailures.maxTotal)
    assertEquals(6, airbyteWorkerRetryConfig.partialFailures.maxSuccessive)
    assertEquals(7, airbyteWorkerRetryConfig.partialFailures.maxTotal)
  }
}
