/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.validator

import io.airbyte.commons.server.config.JwtIdentityProvidersConfig
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.Requires
import io.micronaut.security.token.Claims
import io.micronaut.security.token.jwt.validator.GenericJwtClaimsValidator
import jakarta.inject.Singleton

private val logger = KotlinLogging.logger {}

@Singleton
@Requires(property = "airbyte.auth.identity-provider.verify-audience", value = "true")
class AudiencesTokenValidator<T>(
  private val jwtIdentityProvidersConfig: JwtIdentityProvidersConfig,
) : GenericJwtClaimsValidator<T> {
  init {
    if (jwtIdentityProvidersConfig.verifyAudience) {
      logger.info { "Validating the configured audiences ${jwtIdentityProvidersConfig.audiences}" }
    } else {
      logger.info { "Not validating audiences, this is recommended for a production system" }
    }
  }

  override fun validate(
    claims: Claims?,
    request: T,
  ): Boolean {
    if (!jwtIdentityProvidersConfig.verifyAudience) {
      logger.debug { "Verifying the audience has been set to false, not verifying audiences" }
      return true
    }

    if (claims == null || claims["aud"] == null) {
      logger.debug { "The claims were null or there is no audience in the jwt claims" }
      return false
    }

    when (val audience = claims["aud"]) {
      is String -> {
        return jwtIdentityProvidersConfig.audiences.any {
          logger.debug { "Verifying the audience $it against the single aud claim: ${claims["aud"]}" }
          it == claims["aud"]
        }
      }
      is List<*> -> {
        return (audience)
          .filterIsInstance<String>()
          .any {
            logger.debug { "Verifying the audiences $it against the single the configured list of audiences ${jwtIdentityProvidersConfig.audiences}" }
            jwtIdentityProvidersConfig.audiences.contains(it)
          }
      }
      else -> {
        logger.error { "The tokens issuer was neither a string nor an array of strings, failing." }
        return false
      }
    }
  }
}
