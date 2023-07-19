package io.airbyte.api.server.services

import java.util.UUID

interface UserService {
  fun getAllWorkspaceIdsForUser(
    userId: UUID,
    authorization: String,
  ): List<UUID>

  fun getUserIdFromAuthToken(authToken: String): UUID
}
