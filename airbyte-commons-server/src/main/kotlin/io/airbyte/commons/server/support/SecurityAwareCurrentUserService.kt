/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.support

import io.airbyte.commons.server.errors.AuthException
import io.airbyte.config.AuthenticatedUser
import io.airbyte.config.persistence.UserPersistence
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.Replaces
import io.micronaut.context.annotation.Requires
import io.micronaut.runtime.http.scope.RequestScope
import io.micronaut.security.utils.SecurityService
import java.util.Optional
import java.util.UUID

/**
 * Interface for retrieving the current Airbyte User associated with the current request. Replaces
 * the [CommunityCurrentUserService] when micronaut.security is enabled, ie in Enterprise and
 * Cloud. `@RequestScope` means one bean is created per request, so the current user is cached for
 * any subsequent calls to getCurrentUser() within the same request.
 */
@RequestScope
@Requires(property = "micronaut.security.enabled", value = "true")
@Replaces(
  CommunityCurrentUserService::class,
)
open class SecurityAwareCurrentUserService(
  private val userPersistence: UserPersistence,
  private val securityService: SecurityService,
) : CurrentUserService {
  private var retrievedCurrentUser: AuthenticatedUser? = null

  override fun getCurrentUser(): AuthenticatedUser {
    if (this.retrievedCurrentUser == null) {
      try {
        val authUserId = securityService.username().orElseThrow()
        this.retrievedCurrentUser = userPersistence.getUserByAuthId(authUserId).orElseThrow()
        log.debug("Setting current user for request to: {}", retrievedCurrentUser)
      } catch (e: Exception) {
        throw AuthException("Could not get the current Airbyte user due to an internal error.", e)
      }
    }
    return retrievedCurrentUser!!
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
