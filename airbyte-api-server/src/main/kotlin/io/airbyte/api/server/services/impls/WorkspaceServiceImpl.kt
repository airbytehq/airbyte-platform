package io.airbyte.api.server.services.impls

import io.airbyte.airbyte_api.model.generated.WorkspaceCreateRequest
import io.airbyte.airbyte_api.model.generated.WorkspaceResponse
import io.airbyte.airbyte_api.model.generated.WorkspaceUpdateRequest
import io.airbyte.airbyte_api.model.generated.WorkspacesResponse
import io.airbyte.api.server.services.WorkspaceService
import java.util.UUID

class WorkspaceServiceImpl : WorkspaceService {
  override fun createCloudWorkspace(workspaceCreateRequest: WorkspaceCreateRequest?, authorization: String): WorkspaceResponse {
    TODO("Not yet implemented")
  }

  override fun updateCloudWorkspace(workspaceId: UUID, workspaceUpdateRequest: WorkspaceUpdateRequest, authorization: String): WorkspaceResponse {
    TODO("Not yet implemented")
  }

  override fun getWorkspace(workspaceId: UUID?, userInfo: String): WorkspaceResponse {
    TODO("Not yet implemented")
  }

  override fun deleteWorkspace(workspaceId: UUID?, authorization: String, userInfo: String) {
    TODO("Not yet implemented")
  }

  override fun listWorkspaces(
    workspaceIds: List<UUID?>,
    includeDeleted: Boolean,
    limit: Int,
    offset: Int,
    authorization: String,
    userInfo: String,
  ): WorkspacesResponse {
    TODO("Not yet implemented")
  }
}
