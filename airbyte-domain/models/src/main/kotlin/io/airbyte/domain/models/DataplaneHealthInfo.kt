/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.domain.models

import java.time.OffsetDateTime
import java.util.UUID

/**
 * Health information for a dataplane based on heartbeat data.
 */
data class DataplaneHealthInfo(
  val dataplaneId: UUID,
  val status: HealthStatus,
  val lastHeartbeatTimestamp: OffsetDateTime?,
  val secondsSinceLastHeartbeat: Long?,
  val recentHeartbeats: List<HeartbeatData>,
  val controlPlaneVersion: String?,
  val dataplaneVersion: String?,
) {
  /**
   * Enum representing the health status of a dataplane.
   */
  enum class HealthStatus {
    /**
     * Dataplane is healthy (heartbeat within last 60 seconds).
     */
    HEALTHY,

    /**
     * Dataplane is degraded (heartbeat within last 5 minutes).
     */
    DEGRADED,

    /**
     * Dataplane is unhealthy (no heartbeat in last 5 minutes).
     */
    UNHEALTHY,

    /**
     * Dataplane health status is unknown (no heartbeat data available).
     */
    UNKNOWN,
  }
}

/**
 * Data for a single heartbeat record.
 */
data class HeartbeatData(
  val timestamp: OffsetDateTime,
)
