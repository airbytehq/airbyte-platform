/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.auth.support

import io.airbyte.commons.auth.support.JwtTokenParser.JWT_AUTH_PROVIDER
import io.airbyte.commons.auth.support.JwtTokenParser.JWT_SSO_REALM
import io.airbyte.commons.auth.support.JwtTokenParser.JWT_USER_EMAIL
import io.airbyte.commons.auth.support.JwtTokenParser.JWT_USER_NAME
import io.airbyte.config.AuthProvider
import io.airbyte.config.AuthenticatedUser
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.security.utils.SecurityService
import jakarta.inject.Singleton
import kotlin.jvm.optionals.getOrNull

private val log = KotlinLogging.logger {}

@Singleton
class JwtUserAuthenticationResolver(
  private val securityService: SecurityService?,
) : UserAuthenticationResolver {
  override fun resolveUser(expectedAuthUserId: String): AuthenticatedUser {
    if (securityService == null) {
      log.warn { "Security service is not available. Returning empty user." }
      return AuthenticatedUser()
    }

    if (expectedAuthUserId != securityService.username().getOrNull()) {
      throw IllegalArgumentException("JWT token doesn't match the expected auth user id.")
    }

    val jwtMap = securityService.authentication.getOrNull()?.attributes ?: throw IllegalStateException("JWT token is missing attributes")
    val email = jwtMap[JWT_USER_EMAIL] as String
    // Default name to email address if name is not found
    val name = jwtMap.getOrDefault(JWT_USER_NAME, email) as String
    // TODO: the default should maybe be OIDC?
    val authProvider = jwtMap.getOrDefault(JWT_AUTH_PROVIDER, AuthProvider.AIRBYTE) as AuthProvider

    return AuthenticatedUser()
      .withName(name)
      .withEmail(email)
      .withAuthUserId(expectedAuthUserId)
      .withAuthProvider(authProvider)
  }

  override fun resolveRealm(): String? {
    if (securityService == null) {
      log.warn { "Security service is not available. Returning empty realm." }
      return null
    }

    val jwtMap = securityService.authentication.getOrNull()?.attributes ?: throw IllegalStateException("JWT token is missing attributes")
    return jwtMap[JWT_SSO_REALM] as String?
  }
}
