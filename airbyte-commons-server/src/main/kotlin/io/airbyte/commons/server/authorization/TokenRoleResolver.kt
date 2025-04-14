/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

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
        val headerMap: Map<String, String> = httpRequest.headers.asMap(String::class.java, String::class.java)
        addAll(rbacRoleHelper.getRbacRoles(authUserId, headerMap))
      } catch (e: Exception) {
        logger.error(e) { "Failed to resolve roles for authUserId $authUserId" }
      }
    }
  }
}
