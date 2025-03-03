/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.auth.resolvers

import io.airbyte.commons.auth.config.GenericOidcFieldMappingConfig
import io.airbyte.commons.auth.support.JwtUserAuthenticationResolver
import io.airbyte.commons.auth.support.UserAuthenticationResolver
import io.airbyte.config.AuthProvider
import io.airbyte.config.AuthenticatedUser
import io.micronaut.context.annotation.Replaces
import io.micronaut.context.annotation.Requires
import io.micronaut.security.authentication.Authentication
import io.micronaut.security.authentication.AuthenticationException
import io.micronaut.security.utils.SecurityService
import jakarta.inject.Singleton
import java.util.Optional

@Singleton
@Requires(property = "airbyte.auth.identity-provider.type", value = "generic-oidc")
@Replaces(JwtUserAuthenticationResolver::class)
class GenericOidcUserAuthenticationResolver(
  private val securityService: SecurityService,
  private val fieldMappings: GenericOidcFieldMappingConfig,
) : UserAuthenticationResolver {
  override fun resolveUser(unused: String): AuthenticatedUser {
    val authUserId: String =
      securityService
        .username()
        .orElseThrow {
          AuthenticationException("Unable to retrieve the authUserId")
        }
    val jwtMap: Authentication =
      securityService
        .authentication
        .orElseThrow {
          AuthenticationException("Unable to parse the jwt body")
        }
    val name: String = (jwtMap.attributes[fieldMappings.name] ?: authUserId).toString()
    val email: String = (jwtMap.attributes[fieldMappings.email] ?: authUserId).toString()
    return AuthenticatedUser()
      .withName(name)
      .withEmail(email)
      .withAuthUserId(authUserId)
      .withAuthProvider(AuthProvider.AIRBYTE)
  }

  override fun resolveRealm(): String? {
    val jwtMap: Optional<Authentication> = securityService.authentication
    if (jwtMap.isEmpty) {
      return null
    }
    val issuer: String = jwtMap.get().attributes[fieldMappings.issuer] as String
    if (issuer.isBlank()) {
      return null
    }
    return issuer
  }
}
