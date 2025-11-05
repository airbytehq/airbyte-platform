/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.cron.jobs

import datadog.trace.api.Trace
import io.airbyte.api.client.AirbyteApiClient
import io.airbyte.api.client.model.generated.DomainVerificationIdRequestBody
import io.airbyte.api.client.model.generated.DomainVerificationResponse
import io.airbyte.metrics.annotations.Instrument
import io.airbyte.metrics.annotations.Tag
import io.airbyte.metrics.lib.MetricTags
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.scheduling.annotation.Scheduled
import jakarta.inject.Singleton
import java.time.Duration
import java.time.OffsetDateTime
import kotlin.math.min
import kotlin.math.pow

private val logger = KotlinLogging.logger { }

/**
 * Cron job that periodically checks pending domain verifications by calling the Airbyte API.
 * This job only contains scheduling logic - the actual DNS lookup and database updates
 * are performed by the API endpoints.
 *
 * Implements tiered exponential backoff to balance responsiveness with load:
 * - First hour (attempts 0-59): Check every 1 minute
 *   - Provides immediate feedback for new domain verifications
 *   - Results in 60 checks in the first hour
 * - After first hour (attempts 60+): Exponential backoff capped at 60 minutes
 *   - Reduces DNS query load for persistent failures
 *   - Backoff schedule: 1min, 2min, 4min, 8min, 16min, 32min, then 60min cap
 */
@Singleton
open class DomainVerificationJob(
  private val airbyteApiClient: AirbyteApiClient,
) {
  companion object {
    const val CHECK_DOMAIN_VERIFICATIONS = "domain-verification-check"
    const val FREQUENT_CHECK_THRESHOLD = 60 // Check every minute for first 60 attempts (1 hour)
    const val INITIAL_BACKOFF_MINUTES = 1L
    const val MAX_BACKOFF_MINUTES = 60L // Cap at 1 hour between checks
  }

  @Trace
  @Instrument(
    start = "DOMAIN_VERIFICATION_RUN",
    end = "DOMAIN_VERIFICATION_DONE",
    duration = "DOMAIN_VERIFICATION_DURATION",
    tags = [Tag(key = MetricTags.CRON_TYPE, value = CHECK_DOMAIN_VERIFICATIONS)],
  )
  @Scheduled(fixedRate = "1m")
  open fun checkPendingDomainVerifications() {
    logger.info { "Starting domain verification check" }

    val pendingVerifications = airbyteApiClient.domainVerificationsApi.listPendingDomainVerifications()

    val verifications = pendingVerifications.domainVerifications ?: emptyList()
    logger.info { "Found ${verifications.size} pending domain verifications" }

    var successCount = 0
    var failureCount = 0

    val now = OffsetDateTime.now()
    val verificationsToCheck = verifications.filter { shouldCheckFrom(it, now) }
    val skippedCount = verifications.size - verificationsToCheck.size

    if (skippedCount > 0) {
      logger.debug { "Skipping $skippedCount verifications (backoff not elapsed)" }
    }

    // Check each pending verification (with exponential backoff)
    verificationsToCheck.forEach { verification ->
      try {
        logger.debug {
          "Checking domain verification ${verification.id} for domain ${verification.domain} " +
            "(attempt ${verification.attempts})"
        }

        val requestBody = DomainVerificationIdRequestBody(verification.id)
        val result = airbyteApiClient.domainVerificationsApi.checkDomainVerification(requestBody)

        logger.debug {
          "Domain verification ${verification.id} check completed with status ${result.status}"
        }

        successCount++
      } catch (e: Exception) {
        logger.error(e) { "Failed to check domain verification ${verification.id}" }
        failureCount++
      }
    }

    val checkedCount = verificationsToCheck.size

    logger.info {
      "Domain verification check completed. " +
        "Total pending: ${verifications.size}, " +
        "Checked: $checkedCount (Success: $successCount, Failures: $failureCount), " +
        "Skipped: $skippedCount"
    }
  }

  /**
   * Determines if enough time has elapsed since the last check based on attempt count.
   *
   * Two-phase strategy:
   * - First hour (attempts 0-59): Check every time cron runs (every 1 minute)
   *   - Provides immediate feedback for new verifications
   *   - 60 checks in first hour
   * - After first hour (attempts 60+): Exponential backoff capped at 60 minutes
   *   - Reduces DNS query load for persistent failures
   *
   * Backoff schedule after first hour:
   * - Attempt 60: 1 minute
   * - Attempt 61: 2 minutes
   * - Attempt 62: 4 minutes
   * - Attempt 63: 8 minutes
   * - Attempt 64: 16 minutes
   * - Attempt 65: 32 minutes
   * - Attempt 66+: 60 minutes (capped)
   *
   * @param verification The domain verification to check
   * @param from The reference time to calculate elapsed time from
   */
  private fun shouldCheckFrom(
    verification: DomainVerificationResponse,
    from: OffsetDateTime,
  ): Boolean {
    // If never checked before, check now
    if (verification.lastCheckedAt == null) {
      return true
    }

    val attempts = verification.attempts ?: 0

    // First hour: check every time (every minute)
    if (attempts < FREQUENT_CHECK_THRESHOLD) {
      return true
    }

    // After first hour: apply exponential backoff
    val lastChecked = verification.lastCheckedAt?.let { OffsetDateTime.ofInstant(java.time.Instant.ofEpochSecond(it), java.time.ZoneOffset.UTC) }
    val minutesSinceLastCheck = Duration.between(lastChecked, from).toMinutes()

    // Calculate backoff based on attempts past the threshold
    val attemptsOverThreshold = attempts - FREQUENT_CHECK_THRESHOLD
    val exponentialDelay = INITIAL_BACKOFF_MINUTES * (2.0.pow(attemptsOverThreshold.toDouble()))
    val backoffMinutes = min(exponentialDelay, MAX_BACKOFF_MINUTES.toDouble()).toLong()

    return minutesSinceLastCheck >= backoffMinutes
  }
}
