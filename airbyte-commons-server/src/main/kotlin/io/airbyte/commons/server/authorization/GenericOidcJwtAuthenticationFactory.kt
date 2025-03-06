/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.authorization

import com.nimbusds.jwt.JWT
import io.micronaut.context.annotation.Replaces
import io.micronaut.context.annotation.Requires
import io.micronaut.http.context.ServerRequestContext
import io.micronaut.security.authentication.Authentication
import io.micronaut.security.token.jwt.validator.DefaultJwtAuthenticationFactory
import io.micronaut.security.token.jwt.validator.JwtAuthenticationFactory
import jakarta.inject.Singleton
import java.util.Optional

@Singleton
@Replaces(DefaultJwtAuthenticationFactory::class)
@Requires(property = "airbyte.auth.identity-provider.type", value = "generic-oidc")
class GenericOidcJwtAuthenticationFactory(
  private val tokenRoleResolver: TokenRoleResolver,
) : JwtAuthenticationFactory {
  override fun createAuthentication(token: JWT?): Optional<Authentication> =
    Optional.of(
      Authentication.build(
        token?.jwtClaimsSet?.subject,
        tokenRoleResolver.resolveRoles(
          token?.jwtClaimsSet?.subject,
          ServerRequestContext.currentRequest<Any>().orElseThrow(),
        ),
        token?.jwtClaimsSet?.claims,
      ),
    )
}
