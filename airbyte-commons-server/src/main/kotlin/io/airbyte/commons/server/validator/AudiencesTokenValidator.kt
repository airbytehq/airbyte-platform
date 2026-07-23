/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.validator

import io.airbyte.micronaut.runtime.AirbyteAuthConfig
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.Requires
import io.micronaut.security.token.Claims
import io.micronaut.security.token.jwt.validator.GenericJwtClaimsValidator
import jakarta.inject.Singleton

private val logger = KotlinLogging.logger {}

@Singleton
@Requires(property = "airbyte.auth.identity-provider.verify-audience", value = "true")
class AudiencesTokenValidator<T>(
  private val airbyteAuthConfig: AirbyteAuthConfig,
) : GenericJwtClaimsValidator<T> {
  init {
    if (airbyteAuthConfig.identityProvider.verifyAudience) {
      logger.info { "Validating the configured audiences ${airbyteAuthConfig.identityProvider.audiences}" }
    } else {
      logger.info { "Not validating audiences, this is recommended for a production system" }
    }
  }

  override fun validate(
    claims: Claims?,
    request: T,
  ): Boolean {
    if (!airbyteAuthConfig.identityProvider.verifyAudience) {
      logger.debug { "Verifying the audience has been set to false, not verifying audiences" }
      return true
    }

    if (claims == null || claims["aud"] == null) {
      logger.debug { "The claims were null or there is no audience in the jwt claims" }
      return false
    }

    when (val audience = claims["aud"]) {
      is String -> {
        return airbyteAuthConfig.identityProvider.audiences.any {
          logger.debug { "Verifying the audience $it against the single aud claim: ${claims["aud"]}" }
          it == claims["aud"]
        }
      }
      is List<*> -> {
        return (audience)
          .filterIsInstance<String>()
          .any {
            logger.debug {
              "Verifying the audiences $it against the single the configured list of audiences ${airbyteAuthConfig.identityProvider.audiences}"
            }
            airbyteAuthConfig.identityProvider.audiences.contains(it)
          }
      }
      else -> {
        logger.error { "The tokens issuer was neither a string nor an array of strings, failing." }
        return false
      }
    }
  }
}
