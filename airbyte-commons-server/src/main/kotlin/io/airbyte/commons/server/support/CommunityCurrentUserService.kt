/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.support

import io.airbyte.config.AuthenticatedUser
import io.airbyte.config.persistence.UserPersistence
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.runtime.http.scope.RequestScope
import java.util.Optional
import java.util.UUID

/**
 * Implementation of [CurrentUserService] that uses the default user from the
 * [UserPersistence]. Community edition of Airbyte doesn't surface the concept of real users,
 * so this implementation simply returns the default user that ships with the application.
 * `@RequestScope` means one bean is created per request, so the default user is cached for any
 * later calls to getCurrentUser() within the same request.
 */
@RequestScope
open class CommunityCurrentUserService(
  private val userPersistence: UserPersistence?,
) : CurrentUserService {
  private var retrievedDefaultUser: AuthenticatedUser? = null

  override fun getCurrentUser(): AuthenticatedUser {
    if (this.retrievedDefaultUser == null) {
      try {
        this.retrievedDefaultUser = userPersistence?.getDefaultUser()?.orElseThrow()
        log.debug("Setting current user for request to retrieved default user: {}", retrievedDefaultUser)
      } catch (e: Exception) {
        throw RuntimeException("Could not get the current user due to an internal error.", e)
      }
    }
    return retrievedDefaultUser!!
  }

  override fun getCurrentUserIdIfExists(): Optional<UUID> {
    try {
      return Optional.of(getCurrentUser().userId)
    } catch (e: Exception) {
      log.error("Unable to get current user associated with the request", e)
      return Optional.empty()
    }
  }

  companion object {
    private val log = KotlinLogging.logger {}
  }
}
