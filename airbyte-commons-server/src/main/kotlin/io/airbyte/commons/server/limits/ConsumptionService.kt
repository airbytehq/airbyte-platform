/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.limits

import io.airbyte.data.services.ConnectionService
import io.airbyte.data.services.DestinationService
import io.airbyte.data.services.SourceService
import jakarta.inject.Singleton
import java.util.UUID

@Singleton
class ConsumptionService(
  private val connectionService: ConnectionService,
  private val destinationService: DestinationService,
  private val sourceService: SourceService,
) {
  class WorkspaceConsumption(
    val connections: Long,
    val sources: Long,
    val destinations: Long,
  )

  fun getForWorkspace(workspaceId: UUID): WorkspaceConsumption {
    val connectionCount = connectionService.listWorkspaceStandardSyncs(workspaceId, false).size.toLong()
    val destinationCount = destinationService.listWorkspaceDestinationConnection(workspaceId).size.toLong()
    val sourcesCount = sourceService.listWorkspaceSourceConnection(workspaceId).size.toLong()

    return WorkspaceConsumption(connectionCount, sourcesCount, destinationCount)
  }
}
