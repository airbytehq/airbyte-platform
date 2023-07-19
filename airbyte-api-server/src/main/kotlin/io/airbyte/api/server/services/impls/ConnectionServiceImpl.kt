package io.airbyte.api.server.services.impls

import io.airbyte.airbyte_api.model.generated.ConnectionResponse
import io.airbyte.airbyte_api.model.generated.ConnectionsResponse
import io.airbyte.api.model.generated.ConnectionCreate
import io.airbyte.api.model.generated.ConnectionUpdate
import io.airbyte.api.server.services.ConnectionService
import io.micronaut.context.annotation.Secondary
import jakarta.inject.Singleton
import java.util.UUID

@Singleton
@Secondary
class ConnectionServiceImpl : ConnectionService {
  override fun createConnection(connectionCreate: ConnectionCreate, endpointUserInfo: String): ConnectionResponse {
    TODO("Not yet implemented")
  }

  override fun deleteConnection(connectionId: UUID, endpointUserInfo: String): ConnectionResponse {
    TODO("Not yet implemented")
  }

  override fun getConnection(connectionId: UUID, endpointUserInfo: String): ConnectionResponse {
    TODO("Not yet implemented")
  }

  override fun updateConnection(connectionUpdate: ConnectionUpdate, endpointUserInfo: String): ConnectionResponse {
    TODO("Not yet implemented")
  }

  override fun listConnectionsForWorkspaces(
    workspaceIds: MutableList<UUID>,
    limit: Int,
    offset: Int,
    includeDeleted: Boolean,
    endpointUserInfo: String,
  ): ConnectionsResponse {
    TODO("Not yet implemented")
  }
}
