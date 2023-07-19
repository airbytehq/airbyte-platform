package io.airbyte.api.server.services

import io.airbyte.airbyte_api.model.generated.ConnectionResponse
import io.airbyte.airbyte_api.model.generated.ConnectionsResponse
import io.airbyte.api.model.generated.ConnectionCreate
import io.airbyte.api.model.generated.ConnectionUpdate
import java.util.Collections
import java.util.UUID

interface ConnectionService {
  fun createConnection(
    connectionCreate: ConnectionCreate,
    endpointUserInfo: String,
  ): ConnectionResponse

  fun deleteConnection(
    connectionId: UUID,
    endpointUserInfo: String,
  ): ConnectionResponse

  fun getConnection(
    connectionId: UUID,
    endpointUserInfo: String,
  ): ConnectionResponse

  fun updateConnection(
    connectionUpdate: ConnectionUpdate,
    endpointUserInfo: String,
  ): ConnectionResponse

  fun listConnectionsForWorkspaces(
    workspaceIds: MutableList<UUID> = Collections.emptyList(),
    limit: Int = 20,
    offset: Int = 0,
    includeDeleted: Boolean = false,
    endpointUserInfo: String,
  ): ConnectionsResponse
}
