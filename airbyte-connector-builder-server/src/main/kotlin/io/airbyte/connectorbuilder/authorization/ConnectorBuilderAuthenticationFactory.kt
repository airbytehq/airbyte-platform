/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.connectorbuilder.authorization

import com.nimbusds.jwt.JWT
import io.airbyte.commons.auth.roles.AuthRole
import io.airbyte.commons.server.authorization.AuthenticationFactory
import io.micronaut.context.annotation.Replaces
import io.micronaut.security.authentication.Authentication
import io.micronaut.security.token.jwt.validator.JwtAuthenticationFactory
import jakarta.inject.Singleton
import java.util.Optional

/**
 * The Connector Builder Server does not have access to the config database,
 * so it cannot determine roles for a given request, so it provides only a single,
 * hard-coded "authenticated user" role.
 */
@Replaces(AuthenticationFactory::class)
@Singleton
class ConnectorBuilderAuthenticationFactory : JwtAuthenticationFactory {
  override fun createAuthentication(token: JWT): Optional<Authentication> {
    val subject = token.jwtClaimsSet?.subject
    if (subject.isNullOrBlank()) {
      return Optional.empty()
    }

    return Optional.of(
      Authentication.build(
        token.jwtClaimsSet?.subject,
        setOf(AuthRole.AUTHENTICATED_USER.name),
        token.jwtClaimsSet?.claims,
      ),
    )
  }
}
