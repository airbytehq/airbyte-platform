/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config

import com.google.common.base.Preconditions
import io.airbyte.config.JobConfig.ConfigType
import jakarta.annotation.Nullable
import java.util.EnumSet
import java.util.Optional
import java.util.stream.Collectors

/**
 * POJO / accessors for the job domain model.
 */
@JvmRecord
data class Job(
  @JvmField val id: Long,
  @JvmField val configType: ConfigType,
  @JvmField val scope: String,
  @JvmField val config: JobConfig,
  @JvmField val attempts: List<Attempt>,
  @JvmField val status: JobStatus,
  @JvmField @field:Nullable @param:Nullable val startedAtInSecond: Long?,
  @JvmField val createdAtInSecond: Long,
  @JvmField val updatedAtInSecond: Long,
  val isScheduled: Boolean,
) {
  fun getAttemptsCount(): Int = attempts.size

  fun startedAtInSecond(): Long? = startedAtInSecond

  fun getSuccessfulAttempt(): Optional<Attempt> {
    val successfulAttempts =
      attempts
        .stream()
        .filter { a: Attempt -> a.status == AttemptStatus.SUCCEEDED }
        .collect(Collectors.toList())

    Preconditions.checkState(
      successfulAttempts.size <= 1,
      String.format("Job %s has multiple successful attempts.", id),
    )
    return successfulAttempts.stream().findFirst()
  }

  fun getSuccessOutput(): Optional<JobOutput> = getSuccessfulAttempt().map { obj: Attempt -> obj.output }

  fun getLastFailedAttempt(): Optional<Attempt> =
    attempts
      .stream()
      .filter { a: Attempt -> a.status == AttemptStatus.FAILED }
      .max(Comparator.comparing { obj: Attempt -> obj.createdAtInSecond })

  fun getLastAttempt(): Optional<Attempt> =
    attempts
      .stream()
      .max(Comparator.comparing { obj: Attempt -> obj.createdAtInSecond })

  fun getAttemptByNumber(attemptNumber: Int): Optional<Attempt> =
    attempts
      .stream()
      .filter { a: Attempt -> a.attemptNumber == attemptNumber }
      .findFirst()

  fun hasRunningAttempt(): Boolean = attempts.stream().anyMatch { a: Attempt -> !Attempt.isAttemptInTerminalState(a) }

  fun isJobInTerminalState(): Boolean = JobStatus.TERMINAL_STATUSES.contains(status)

  fun validateStatusTransition(newStatus: JobStatus) {
    val validNewStatuses = JobStatus.VALID_STATUS_CHANGES[status]!!
    check(validNewStatuses.contains(newStatus)) {
      String.format(
        "Transitioning Job %d from JobStatus %s to %s is not allowed. Valid transitions: %s",
        id,
        status,
        newStatus,
        validNewStatuses,
      )
    }
  }

  companion object {
    @JvmField
    val REPLICATION_TYPES: Set<ConfigType> = EnumSet.of(ConfigType.SYNC, ConfigType.RESET_CONNECTION, ConfigType.REFRESH)

    @JvmField
    val SYNC_REPLICATION_TYPES: Set<ConfigType> = EnumSet.of(ConfigType.SYNC, ConfigType.REFRESH)
  }
}
