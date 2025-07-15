/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.support

import io.airbyte.config.AuthenticatedUser
import java.util.Optional
import java.util.UUID

/**
 * Interface for retrieving the User associated with the current request.
 */
interface CurrentUserService {
  fun getCurrentUser(): AuthenticatedUser

  fun getCurrentUserIdIfExists(): Optional<UUID>
}
