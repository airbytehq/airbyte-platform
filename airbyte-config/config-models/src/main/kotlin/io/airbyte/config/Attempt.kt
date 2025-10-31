/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config

import jakarta.annotation.Nonnull
import java.nio.file.Path
import java.util.Optional

/**
 * POJO / accessors for the attempt domain model.
 */

data class Attempt(
  val attemptNumber: Int,
  val jobId: Long,
  val logPath: Path?,
  val syncConfig: AttemptSyncConfig?,
  val output: JobOutput?,
  @field:Nonnull @param:Nonnull val status: AttemptStatus,
  val processingTaskQueue: String?,
  val failureSummary: AttemptFailureSummary?,
  val createdAtInSecond: Long,
  val updatedAtInSecond: Long,
  val endedAtInSecond: Long?,
) {
  companion object {
    fun isAttemptInTerminalState(attempt: Attempt): Boolean = AttemptStatus.TERMINAL_STATUSES.contains(attempt.status)
  }
}
