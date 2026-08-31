/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.cron.jobs

import io.airbyte.data.repositories.DataWorkerUsageReservationCandidate
import io.airbyte.domain.services.dataworker.DataWorkerUsageService
import io.airbyte.featureflag.DataWorkerUsageReconciliationMode
import io.airbyte.featureflag.Empty
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.metrics.MetricAttribute
import io.airbyte.metrics.MetricClient
import io.airbyte.metrics.OssMetricsRegistry
import io.airbyte.metrics.lib.MetricTags
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.scheduling.annotation.Scheduled
import io.opentelemetry.instrumentation.annotations.WithSpan
import jakarta.inject.Singleton
import java.time.OffsetDateTime
import java.time.ZoneId
import java.time.ZoneOffset
import java.util.Optional
import kotlin.jvm.optionals.getOrElse

private val log = KotlinLogging.logger {}

@Singleton
class DataWorkerUsageReconciliationCron(
  private val dataWorkerUsageService: DataWorkerUsageService,
  private val featureFlagClient: FeatureFlagClient,
  private val metricClient: MetricClient,
  private val timeProvider: Optional<(ZoneId) -> OffsetDateTime>,
) {
  private var candidateCursor: DataWorkerUsageReservationCandidate? = null
  private var previousMode: ReconciliationMode? = null

  @WithSpan
  @Synchronized
  @Scheduled(fixedRate = "30m", initialDelay = "5m")
  fun reconcileDataWorkerUsage() {
    metricClient.count(
      metric = OssMetricsRegistry.CRON_JOB_RUN_BY_CRON_TYPE,
      attributes = arrayOf(MetricAttribute(MetricTags.CRON_TYPE, CRON_TYPE)),
    )

    val mode =
      try {
        val modeValue = featureFlagClient.stringVariation(DataWorkerUsageReconciliationMode, Empty)
        ReconciliationMode.entries.find { it.value == modeValue }
      } catch (e: Exception) {
        recordOutcome(OUTCOME_FLAG_ERROR)
        log.error(e) { "Failed to evaluate the data worker usage reconciliation mode feature flag" }
        return
      }

    if (mode == null) {
      recordOutcome(OUTCOME_FLAG_ERROR)
      log.error { "Unsupported data worker usage reconciliation mode returned by feature flag" }
      return
    }

    if (mode != previousMode) {
      candidateCursor = null
      previousMode = mode
    }

    if (mode == ReconciliationMode.DISABLED) {
      recordOutcome(OUTCOME_DISABLED)
      log.info { "Data worker usage reconciliation is disabled via feature flag" }
      return
    }

    val dryRun = mode == ReconciliationMode.DRY_RUN
    val candidates =
      try {
        val terminalBefore =
          timeProvider
            .getOrElse { DEFAULT_TIME_PROVIDER }
            .invoke(ZoneOffset.UTC)
            .minusMinutes(TERMINAL_GRACE_PERIOD_MINUTES)
        dataWorkerUsageService.findTerminalReservationCandidates(terminalBefore, candidateCursor)
      } catch (e: Exception) {
        recordOutcome(OUTCOME_FAILURE, dryRun)
        log.error(e) { "Failed to discover data worker usage reconciliation candidates" }
        throw e
      }

    metricClient.distribution(
      metric = OssMetricsRegistry.DATA_WORKER_USAGE_RECONCILIATION_CANDIDATE_COUNT,
      value = candidates.size.toDouble(),
      attributes = arrayOf(MetricAttribute(MetricTags.DRY_RUN, dryRun.toString())),
    )

    val oldestTerminalAt = candidates.minOfOrNull { it.terminalAt.toInstant() }
    val newestTerminalAt = candidates.maxOfOrNull { it.terminalAt.toInstant() }
    val batchSaturated = candidates.size >= DataWorkerUsageService.RECONCILIATION_BATCH_SIZE
    log.info {
      "Data worker usage reconciliation candidates discovered: " +
        "mode=${mode.value}, candidates=${candidates.size}, " +
        "oldestTerminalAt=$oldestTerminalAt, newestTerminalAt=$newestTerminalAt, " +
        "batchSaturated=$batchSaturated"
    }

    if (dryRun) {
      candidates.forEach { candidate ->
        log.info {
          "Data worker usage reconciliation dry-run candidate: " +
            "jobId=${candidate.jobId}, organizationId=${candidate.organizationId}, terminalAt=${candidate.terminalAt}"
        }
      }
      candidateCursor = if (batchSaturated) candidates.last() else null
      recordOutcome(OUTCOME_SUCCESS, dryRun = true)
      return
    }

    var releasedCount = 0
    var alreadyReleasedCount = 0
    var failedCount = 0

    candidates.forEach { candidate ->
      try {
        if (dataWorkerUsageService.releaseReservedUsageForJob(candidate.jobId, candidate.organizationId)) {
          releasedCount++
        } else {
          alreadyReleasedCount++
        }
      } catch (e: Exception) {
        failedCount++
        log.error(e) {
          "Failed to reconcile data worker usage reservation for job ${candidate.jobId} and organization ${candidate.organizationId}"
        }
      }
    }
    candidateCursor = if (batchSaturated) candidates.last() else null

    log.info {
      "Data worker usage reconciliation completed: " +
        "candidates=${candidates.size}, released=$releasedCount, " +
        "alreadyReleased=$alreadyReleasedCount, failed=$failedCount"
    }
    recordOutcome(if (failedCount > 0) OUTCOME_PARTIAL_FAILURE else OUTCOME_SUCCESS)
  }

  private fun recordOutcome(
    outcome: String,
    dryRun: Boolean = false,
  ) {
    metricClient.count(
      metric = OssMetricsRegistry.DATA_WORKER_USAGE_RECONCILIATION_RUN,
      attributes =
        arrayOf(
          MetricAttribute(MetricTags.STATUS, outcome),
          MetricAttribute(MetricTags.DRY_RUN, dryRun.toString()),
        ),
    )
  }

  companion object {
    private const val CRON_TYPE = "data_worker_usage_reconciliation"
    private const val TERMINAL_GRACE_PERIOD_MINUTES = 15L
    private const val OUTCOME_DISABLED = "disabled"
    private const val OUTCOME_FLAG_ERROR = "flag_error"
    private const val OUTCOME_SUCCESS = "success"
    private const val OUTCOME_PARTIAL_FAILURE = "partial_failure"
    private const val OUTCOME_FAILURE = "failure"
    private val DEFAULT_TIME_PROVIDER: (ZoneId) -> OffsetDateTime = OffsetDateTime::now
  }

  private enum class ReconciliationMode(
    val value: String,
  ) {
    DISABLED("disabled"),
    DRY_RUN("dry_run"),
    ENABLED("enabled"),
  }
}
