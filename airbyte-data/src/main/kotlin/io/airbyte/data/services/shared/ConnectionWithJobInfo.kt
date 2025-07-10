/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.shared

import io.airbyte.config.StandardSync
import io.airbyte.db.instance.jobs.jooq.generated.enums.JobStatus
import java.time.OffsetDateTime
import java.util.Optional

/**
 * Connection data along with its latest job information.
 */
data class ConnectionWithJobInfo(
  private val _connection: StandardSync,
  private val _sourceName: String,
  private val _destinationName: String,
  private val _latestJobStatus: Optional<JobStatus>,
  private val _latestJobCreatedAt: Optional<OffsetDateTime>,
) {
  fun connection(): StandardSync = _connection

  fun sourceName(): String = _sourceName

  fun destinationName(): String = _destinationName

  fun latestJobStatus(): Optional<JobStatus> = _latestJobStatus

  fun latestJobCreatedAt(): Optional<OffsetDateTime> = _latestJobCreatedAt

  companion object {
    @JvmStatic
    fun of(
      connection: StandardSync,
      sourceName: String,
      destinationName: String,
      latestJobStatus: JobStatus?,
      latestJobCreatedAt: OffsetDateTime?,
    ): ConnectionWithJobInfo =
      ConnectionWithJobInfo(connection, sourceName, destinationName, Optional.ofNullable(latestJobStatus), Optional.ofNullable(latestJobCreatedAt))
  }
}
