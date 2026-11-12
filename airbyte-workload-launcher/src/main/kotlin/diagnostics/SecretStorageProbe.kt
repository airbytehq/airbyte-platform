/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher.diagnostics

import io.airbyte.api.client.model.generated.DataplaneDiagnosticResult
import io.airbyte.micronaut.runtime.WORKLOAD_LAUNCHER_SECRET_STORAGE_PROBE_CHECK_TIMEOUT
import io.airbyte.micronaut.runtime.WORKLOAD_LAUNCHER_SECRET_STORAGE_PROBE_RATE
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.Value
import io.micronaut.scheduling.annotation.Scheduled
import jakarta.inject.Named
import jakarta.inject.Singleton
import java.time.Duration
import java.util.concurrent.ExecutorService
import java.util.concurrent.Future
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException

private val logger = KotlinLogging.logger {}

// Gives the launcher time to learn its data plane before the first run.
private const val INITIAL_DELAY = "1m"

/**
 * Checks on a schedule that this data plane can read from each secret store its workloads use.
 */
@Singleton
class SecretStorageProbe(
  private val targets: SecretStorageProbeTargets,
  private val checker: SecretStorageChecker,
  @Named("secret-storage-check") private val checkExecutor: ExecutorService,
  @Value("\${$WORKLOAD_LAUNCHER_SECRET_STORAGE_PROBE_CHECK_TIMEOUT}") private val checkTimeout: Duration,
) : ScheduledDiagnosticProbe() {
  @Scheduled(fixedRate = "\${$WORKLOAD_LAUNCHER_SECRET_STORAGE_PROBE_RATE}", initialDelay = INITIAL_DELAY, scheduler = "secret-storage-probe")
  fun probe() {
    val dataplane = dataplane ?: return
    // The test secret is named after the organization; older control planes don't send it.
    val organizationId = dataplane.organizationId ?: return

    val stores = targets.current()
    if (stores.isEmpty()) {
      publish(listOf(checker.notConfigured(dataplane.dataplaneId, organizationId)))
      logger.info { "No secret stores to check" }
      return
    }

    // Every store is checked at the same time, so one that stalls can't hold back the others.
    val checks = stores.map { it to checkExecutor.submit<DataplaneDiagnosticResult> { checker.check(it, organizationId) } }
    val deadline = System.nanoTime() + checkTimeout.toNanos()

    val results = checks.map { (target, check) -> awaitResult(target, check, deadline) }
    publish(results)
    logger.info { "Checked ${results.size} secret stores" }
  }

  private fun awaitResult(
    target: SecretStorageProbeTarget,
    check: Future<DataplaneDiagnosticResult>,
    deadline: Long,
  ): DataplaneDiagnosticResult =
    try {
      check.get(maxOf(0, deadline - System.nanoTime()), TimeUnit.NANOSECONDS)
    } catch (_: TimeoutException) {
      check.cancel(true)
      logger.warn { "Secret store ${target.secretStorageId} did not respond within $checkTimeout" }
      checker.timedOut(target)
    }
}
