package io.airbyte.api.server.services

import io.airbyte.airbyte_api.model.generated.WorkspaceCreateRequest
import io.airbyte.airbyte_api.model.generated.WorkspaceResponse
import io.airbyte.airbyte_api.model.generated.WorkspaceUpdateRequest
import io.airbyte.airbyte_api.model.generated.WorkspacesResponse
import java.util.UUID
import javax.validation.constraints.NotBlank

interface WorkspaceService {
  fun createCloudWorkspace(workspaceCreateRequest: @NotBlank WorkspaceCreateRequest?, authorization: String): WorkspaceResponse

  fun updateCloudWorkspace(
    workspaceId: UUID,
    workspaceUpdateRequest: WorkspaceUpdateRequest,
    authorization: String,
  ): WorkspaceResponse

  fun getWorkspace(workspaceId: @NotBlank UUID?, userInfo: String): WorkspaceResponse

  fun deleteWorkspace(workspaceId: @NotBlank UUID?, authorization: String, userInfo: String)

  fun listWorkspaces(
    workspaceIds: List<UUID?>,
    includeDeleted: Boolean,
    limit: Int,
    offset: Int,
    authorization: String,
    userInfo: String,
  ): WorkspacesResponse
}
