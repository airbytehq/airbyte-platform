/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.authorization

import io.micronaut.http.HttpRequest
import jakarta.inject.Singleton

interface TokenRoleResolver {
  fun resolveRoles(
    authUserId: String?,
    httpRequest: HttpRequest<*>,
  ): Set<String>
}

@Singleton
class RbacTokenRoleResolver(
  private val roleResolver: RoleResolver,
) : TokenRoleResolver {
  override fun resolveRoles(
    authUserId: String?,
    httpRequest: HttpRequest<*>,
  ): Set<String> {
    if (authUserId == null) {
      return emptySet()
    }

    return roleResolver
      .Request()
      .withHttpRequest(httpRequest)
      .withAuthUserId(authUserId)
      .roles()
  }
}
