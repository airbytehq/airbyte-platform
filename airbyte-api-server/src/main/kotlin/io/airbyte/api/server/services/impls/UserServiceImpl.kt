package io.airbyte.api.server.services.impls

import io.airbyte.api.server.services.UserService
import java.util.UUID

class UserServiceImpl : UserService {
  override fun getAllWorkspaceIdsForUser(userId: UUID, authorization: String): List<UUID> {
    TODO("Not yet implemented")
  }

  override fun getUserIdFromAuthToken(authToken: String): UUID {
    TODO("Not yet implemented")
  }
}
