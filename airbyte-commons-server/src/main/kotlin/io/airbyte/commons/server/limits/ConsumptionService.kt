package io.airbyte.commons.server.limits

import io.airbyte.config.persistence.PermissionPersistence
import io.airbyte.config.persistence.WorkspacePersistence
import io.airbyte.data.services.ConnectionService
import io.airbyte.data.services.DestinationService
import io.airbyte.data.services.SourceService
import jakarta.inject.Singleton
import java.util.Optional
import java.util.UUID

@Singleton
class ConsumptionService(
  private val connectionService: ConnectionService,
  private val destinationService: DestinationService,
  private val sourceService: SourceService,
  private val workspacePersistence: WorkspacePersistence,
  private val permissionPersistence: PermissionPersistence,
) {
  class WorkspaceConsumption(
    val connections: Long,
    val sources: Long,
    val destinations: Long,
  )

  class OrganizationConsumption(
    val workspaces: Long,
    val users: Long,
  )

  fun getForWorkspace(workspaceId: UUID): ConsumptionService.WorkspaceConsumption {
    val connectionCount = connectionService.listWorkspaceStandardSyncs(workspaceId, false).size.toLong()
    val destinationCount = destinationService.listWorkspaceDestinationConnection(workspaceId).size.toLong()
    val sourcesCount = sourceService.listWorkspaceSourceConnection(workspaceId).size.toLong()

    return WorkspaceConsumption(connectionCount, sourcesCount, destinationCount)
  }

  fun getForOrganization(organizationId: UUID): ConsumptionService.OrganizationConsumption {
    val workspaceCount = workspacePersistence.listWorkspacesByOrganizationId(organizationId, false, Optional.empty()).size.toLong()
    val userCount = permissionPersistence.listUsersInOrganization(organizationId).size.toLong()
    return OrganizationConsumption(workspaceCount, userCount)
  }
}
