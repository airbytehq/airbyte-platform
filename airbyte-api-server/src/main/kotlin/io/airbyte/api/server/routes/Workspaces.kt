package io.airbyte.api.server.routes

import io.airbyte.airbyte_api.generated.WorkspacesApi
import io.airbyte.airbyte_api.model.generated.WorkspaceCreateRequest
import io.airbyte.airbyte_api.model.generated.WorkspaceOAuthCredentialsRequest
import io.airbyte.airbyte_api.model.generated.WorkspaceUpdateRequest
import io.airbyte.api.server.services.WorkspaceService
import io.micronaut.http.annotation.Controller
import java.util.UUID
import javax.ws.rs.core.Response

@Controller("/v1/workspaces")
class Workspaces(workspaceService: WorkspaceService) : WorkspacesApi {
  override fun createOrUpdateWorkspaceOAuthCredentials(
    workspaceId: UUID?,
    workspaceOAuthCredentialsRequest: WorkspaceOAuthCredentialsRequest?,
  ): Response {
    TODO("Not yet implemented")
  }

  override fun createWorkspace(workspaceCreateRequest: WorkspaceCreateRequest?): Response {
    TODO("Not yet implemented")
  }

  override fun deleteWorkspace(workspaceId: UUID?): Response {
    TODO("Not yet implemented")
  }

  override fun getWorkspace(workspaceId: UUID?): Response {
    TODO("Not yet implemented")
  }

  override fun listWorkspaces(workspaceIds: MutableList<UUID>?, includeDeleted: Boolean?, limit: Int?, offset: Int?): Response {
    TODO("Not yet implemented")
  }

  override fun updateWorkspace(workspaceId: UUID?, workspaceUpdateRequest: WorkspaceUpdateRequest?): Response {
    TODO("Not yet implemented")
  }
}
