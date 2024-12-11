package io.airbyte.commons.server.authorization

import io.airbyte.commons.auth.AuthRole
import io.airbyte.commons.server.support.RbacRoleHelper
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.core.annotation.Nullable
import io.micronaut.http.HttpRequest
import jakarta.inject.Singleton

private val logger = KotlinLogging.logger {}

interface TokenRoleResolver {
  fun resolveRoles(
    @Nullable authUserId: String?,
    httpRequest: HttpRequest<*>,
  ): Set<String>
}

@Singleton
class RbacTokenRoleResolver(
  private val rbacRoleHelper: RbacRoleHelper,
) : TokenRoleResolver {
  override fun resolveRoles(
    @Nullable authUserId: String?,
    httpRequest: HttpRequest<*>,
  ): Set<String> {
    logger.debug { "Resolving roles for authUserId $authUserId" }

    if (authUserId.isNullOrBlank()) {
      logger.debug { "Provided authUserId is null or blank, returning empty role set" }
      return setOf()
    }

    return mutableSetOf(AuthRole.AUTHENTICATED_USER.name).apply {
      try {
        addAll(rbacRoleHelper.getRbacRoles(authUserId, httpRequest))
      } catch (e: Exception) {
        logger.error(e) { "Failed to resolve roles for authUserId $authUserId" }
      }
    }
  }
}
