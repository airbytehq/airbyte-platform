/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config

import io.airbyte.config.JobConfig.ConfigType
import jakarta.annotation.Nullable
import java.util.Optional

/**
 * POJO / accessors for the job domain model.
 */

data class Job(
  val id: Long,
  val configType: ConfigType,
  val scope: String,
  val config: JobConfig,
  val attempts: List<Attempt>,
  val status: JobStatus,
  @field:Nullable @param:Nullable val startedAtInSecond: Long?,
  val createdAtInSecond: Long,
  val updatedAtInSecond: Long,
  val isScheduled: Boolean,
) {
  fun getAttemptsCount(): Int = attempts.size

  fun getSuccessfulAttempt(): Optional<Attempt> {
    val successfulAttempts =
      attempts
        .filter { a: Attempt -> a.status == AttemptStatus.SUCCEEDED }
        .toList()

    check(successfulAttempts.size <= 1) { "Job $id has multiple successful attempts." }
    return Optional.ofNullable(successfulAttempts.firstOrNull())
  }

  fun getLastFailedAttempt(): Optional<Attempt> =
    Optional.ofNullable(
      attempts
        .filter { a: Attempt -> a.status == AttemptStatus.FAILED }
        .maxByOrNull { obj: Attempt -> obj.createdAtInSecond },
    )

  fun getLastAttempt(): Optional<Attempt> = Optional.ofNullable(attempts.maxByOrNull { obj: Attempt -> obj.createdAtInSecond })

  fun getAttemptByNumber(attemptNumber: Int): Optional<Attempt> =
    Optional.ofNullable(
      attempts.firstOrNull { a: Attempt ->
        a.attemptNumber ==
          attemptNumber
      },
    )

  fun hasRunningAttempt(): Boolean = attempts.any { a: Attempt -> !Attempt.isAttemptInTerminalState(a) }

  fun isJobInTerminalState(): Boolean = JobStatus.TERMINAL_STATUSES.contains(status)

  fun validateStatusTransition(newStatus: JobStatus) {
    val validNewStatuses = JobStatus.VALID_STATUS_CHANGES[status]!!
    check(validNewStatuses.contains(newStatus)) {
      "Transitioning Job $id from JobStatus $status to $newStatus is not allowed. Valid transitions: $validNewStatuses"
    }
  }

  companion object {
    val REPLICATION_TYPES: Set<ConfigType> = setOf(ConfigType.SYNC, ConfigType.RESET_CONNECTION, ConfigType.REFRESH)
    val SYNC_REPLICATION_TYPES: Set<ConfigType> = setOf(ConfigType.SYNC, ConfigType.REFRESH)
  }
}
