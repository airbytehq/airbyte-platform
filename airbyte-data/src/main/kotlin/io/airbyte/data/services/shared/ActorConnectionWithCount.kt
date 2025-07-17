/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.shared

import io.airbyte.config.DestinationConnection
import io.airbyte.config.JobStatus
import io.airbyte.config.SourceConnection
import java.time.OffsetDateTime

/**
 * A unified wrapper for actor connections with count that can hold either a source or destination connection.
 *
 * @param sourceConnection Source connection (if this is a source actor).
 * @param destinationConnection Destination connection (if this is a destination actor).
 * @param connectionCount Number of non-deprecated connections using this actor.
 * @param lastSync Timestamp of the most recent sync for any connection using this actor.
 * @param connectionJobStatuses Map of most recent job status to count of connections with that job status.
 */
@JvmRecord
data class ActorConnectionWithCount(
  @JvmField val actorDefinitionName: String,
  @JvmField val sourceConnection: SourceConnection?,
  @JvmField val destinationConnection: DestinationConnection?,
  @JvmField val connectionCount: Int,
  @JvmField val lastSync: OffsetDateTime?,
  @JvmField val connectionJobStatuses: Map<JobStatus, Int>,
  @JvmField val isActive: Boolean,
) {
  companion object {
    /**
     * Creates an ActorConnectionWithCount from a SourceConnectionWithCount.
     */
    fun fromSource(sourceConnectionWithCount: SourceConnectionWithCount): ActorConnectionWithCount =
      ActorConnectionWithCount(
        actorDefinitionName = sourceConnectionWithCount.sourceDefinitionName,
        sourceConnection = sourceConnectionWithCount.source,
        destinationConnection = null,
        connectionCount = sourceConnectionWithCount.connectionCount,
        lastSync = sourceConnectionWithCount.lastSync,
        connectionJobStatuses = sourceConnectionWithCount.connectionJobStatuses,
        isActive = sourceConnectionWithCount.isActive,
      )

    /**
     * Creates an ActorConnectionWithCount from a DestinationConnectionWithCount.
     */
    fun fromDestination(destinationConnectionWithCount: DestinationConnectionWithCount): ActorConnectionWithCount =
      ActorConnectionWithCount(
        actorDefinitionName = destinationConnectionWithCount.destinationDefinitionName,
        sourceConnection = null,
        destinationConnection = destinationConnectionWithCount.destination,
        connectionCount = destinationConnectionWithCount.connectionCount,
        lastSync = destinationConnectionWithCount.lastSync,
        connectionJobStatuses = destinationConnectionWithCount.connectionJobStatuses,
        isActive = destinationConnectionWithCount.isActive,
      )
  }
}
