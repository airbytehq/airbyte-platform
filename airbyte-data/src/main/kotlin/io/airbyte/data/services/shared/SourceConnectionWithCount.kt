/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.shared

import io.airbyte.config.JobStatus
import io.airbyte.config.SourceConnection
import java.time.OffsetDateTime

/**
 * A pair of a source connection and its associated connection count.
 *
 * @param source Source connection.
 * @param sourceDefinitionName Name of the source definition
 * @param connectionCount Number of non-deprecated connections using this source.
 * @param lastSync Timestamp of the most recent sync for any connection using this source.
 * @param connectionJobStatuses Map of most recent job status to count of connections with that job
 * @param isActive Whether the source has any active connections
 * status.
 */
@JvmRecord
data class SourceConnectionWithCount(
  @JvmField val source: SourceConnection,
  @JvmField val sourceDefinitionName: String,
  @JvmField val connectionCount: Int,
  @JvmField val lastSync: OffsetDateTime?,
  @JvmField val connectionJobStatuses: Map<JobStatus, Int>,
  @JvmField val isActive: Boolean,
)
