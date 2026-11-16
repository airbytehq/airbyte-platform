/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher.diagnostics

import io.airbyte.api.client.model.generated.DataplaneDiagnosticResult
import io.airbyte.micronaut.runtime.WORKLOAD_LAUNCHER_STORAGE_REACHABILITY_PROBE_CHECK_TIMEOUT
import io.airbyte.micronaut.runtime.WORKLOAD_LAUNCHER_STORAGE_REACHABILITY_PROBE_RATE
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.Value
import io.micronaut.scheduling.annotation.Scheduled
import jakarta.inject.Named
import jakarta.inject.Singleton
import java.time.Duration
import java.util.concurrent.ExecutorService
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException

private val logger = KotlinLogging.logger {}

/**
 * Checks on a schedule that this data plane can upload to where its sync logs go.
 */
@Singleton
class StorageReachabilityProbe(
  private val checker: StorageReachabilityChecker,
  @Named("storage-reachability-check") private val checkExecutor: ExecutorService,
  @Value("\${$WORKLOAD_LAUNCHER_STORAGE_REACHABILITY_PROBE_CHECK_TIMEOUT}") private val checkTimeout: Duration,
) : ScheduledDiagnosticProbe() {
  // The initial delay gives the launcher time to learn its data plane.
  @Scheduled(fixedRate = "\${$WORKLOAD_LAUNCHER_STORAGE_REACHABILITY_PROBE_RATE}", initialDelay = "1m", scheduler = "storage-reachability-probe")
  fun probe() {
    val dataplane = dataplane ?: return

    // Runs on its own thread, so a stalled upload can be given up on.
    val check = checkExecutor.submit<DataplaneDiagnosticResult> { checker.check(dataplane.dataplaneId) }
    val result =
      try {
        check.get(checkTimeout.toNanos(), TimeUnit.NANOSECONDS)
      } catch (_: TimeoutException) {
        check.cancel(true)
        logger.warn { "Sync log storage did not respond within $checkTimeout" }
        checker.timedOut(dataplane.dataplaneId)
      }
    publish(listOf(result))
    logger.info { "Checked sync log storage: ${result.status}" }
  }
}
