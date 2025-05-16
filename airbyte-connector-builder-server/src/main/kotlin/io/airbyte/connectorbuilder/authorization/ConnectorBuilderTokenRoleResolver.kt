/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.connectorbuilder.authorization

import io.airbyte.commons.auth.AuthRole
import io.airbyte.commons.server.authorization.TokenRoleResolver
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.Primary
import jakarta.inject.Singleton

private val logger = KotlinLogging.logger {}

/**
 * The Connector Builder Server's role resolver does not apply RBAC-specific roles, because they
 * are not needed and currently inaccessible in the Connector Builder Server, which is isolated
 * from other internal Airbyte applications (like the Config DB). If RBAC roles are needed in the
 * future, the Connector Builder Server will need to be updated such that it is able to determine
 * the RBAC roles of a user based on the Permissions stored in the Config DB.
 */
@Primary
@Singleton
class ConnectorBuilderTokenRoleResolver : TokenRoleResolver {
  override fun resolveRoles(
    authUserId: String?,
    httpRequest: io.micronaut.http.HttpRequest<*>,
  ): Set<String> {
    if (authUserId.isNullOrBlank()) {
      logger.debug { "Provided authUserId is null or blank, returning empty role set" }
      return setOf()
    }

    return setOf(AuthRole.AUTHENTICATED_USER.name)
  }
}
