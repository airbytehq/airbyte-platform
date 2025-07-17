/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.shared

import io.airbyte.config.DestinationConnection
import io.airbyte.config.JobStatus
import java.time.OffsetDateTime

/**
 * A pair of a destination connection and its associated connection count.
 *
 * @param destination Destination connection.
 * @param destinationDefinitionName Name of the destination definition
 * @param connectionCount Number of non-deprecated connections using this destination.
 * @param lastSync Timestamp of the most recent sync for any connection using this destination.
 * @param connectionJobStatuses Map of most recent job status to count of connections with that job
 * @param isActive Whether the destination has any active connections
 * status.
 */
@JvmRecord
data class DestinationConnectionWithCount(
  @JvmField val destination: DestinationConnection,
  @JvmField val destinationDefinitionName: String,
  @JvmField val connectionCount: Int,
  @JvmField val lastSync: OffsetDateTime?,
  @JvmField val connectionJobStatuses: Map<JobStatus, Int>,
  @JvmField val isActive: Boolean,
)
