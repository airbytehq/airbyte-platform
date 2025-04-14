/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.auth.support

import io.airbyte.config.AuthenticatedUser

/**
 * Interface for resolving user authentication attributes into an Airbyte User object.
 */
interface UserAuthenticationResolver {
  fun resolveUser(expectedAuthUserId: String): AuthenticatedUser

  fun resolveRealm(): String?
}
