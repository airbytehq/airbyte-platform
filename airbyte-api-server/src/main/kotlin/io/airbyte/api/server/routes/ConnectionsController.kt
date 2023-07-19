package io.airbyte.api.server.routes

import io.airbyte.airbyte_api.generated.ConnectionsApi
import io.airbyte.airbyte_api.model.generated.ConnectionCreateRequest
import io.airbyte.airbyte_api.model.generated.ConnectionPatchRequest
import io.airbyte.api.server.services.ConnectionService
import io.micronaut.http.annotation.Controller
import java.util.UUID
import javax.ws.rs.core.Response

@Controller("/v1/connections")
open class ConnectionsController(connectionService: ConnectionService) : ConnectionsApi {
  override fun createConnection(connectionCreateRequest: ConnectionCreateRequest?): Response {
    TODO("Not yet implemented")
  }

  override fun deleteConnection(connectionId: UUID?): Response {
    TODO("Not yet implemented")
  }

  override fun getConnection(connectionId: UUID?): Response {
    TODO("Not yet implemented")
  }

  override fun listConnections(
    workspaceIds: MutableList<UUID>?,
    includeDeleted: Boolean?,
    limit: Int?,
    offset: Int?,
  ): Response {
    TODO("Not yet implemented")
  }

  override fun patchConnection(connectionId: UUID?, connectionPatchRequest: ConnectionPatchRequest?): Response {
    TODO("Not yet implemented")
  }
}
