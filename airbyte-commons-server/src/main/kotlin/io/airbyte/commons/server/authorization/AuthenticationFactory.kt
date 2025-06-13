/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.authorization

import com.nimbusds.jwt.JWT
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.Replaces
import io.micronaut.security.authentication.Authentication
import io.micronaut.security.token.jwt.validator.DefaultJwtAuthenticationFactory
import io.micronaut.security.token.jwt.validator.JwtAuthenticationFactory
import jakarta.inject.Singleton
import java.util.Optional

private val logger = KotlinLogging.logger {}

@Singleton
@Replaces(DefaultJwtAuthenticationFactory::class)
class AuthenticationFactory(
  private val roleResolver: RoleResolver,
) : JwtAuthenticationFactory {
  override fun createAuthentication(token: JWT): Optional<Authentication> {
    val claimsSet = token.jwtClaimsSet
    if (claimsSet == null) {
      return Optional.empty()
    }
    return Optional.of(createAuth(claimsSet.subject, claimsSet.claims))
  }

  private fun createAuth(
    authUserId: String,
    attrs: Map<String, Any>,
  ): Authentication {
    // Some tokens already have roles assigned. If the token contains roles, use those,
    // otherwise resolve the roles for the current identity + request.
    val tokenRoles = (attrs["roles"] as? List<*>)?.filterIsInstance<String>()
    val roles = tokenRoles ?: resolveRoles(authUserId)

    return Authentication.build(authUserId, roles, attrs)
  }

  private fun resolveRoles(authUserId: String): Set<String> =
    roleResolver
      .Request()
      .withAuthUserId(authUserId)
      .withCurrentHttpRequest()
      .roles()
}
