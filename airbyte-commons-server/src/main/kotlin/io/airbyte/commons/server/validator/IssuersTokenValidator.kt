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
@Requires(property = "airbyte.auth.identity-provider.verify-issuer", value = "true")
class IssuersTokenValidator<T>(
  private val airbyteAuthConfig: AirbyteAuthConfig,
) : GenericJwtClaimsValidator<T> {
  init {
    if (airbyteAuthConfig.identityProvider.verifyIssuer) {
      logger.info { "Validating issuer for the configured issuers ${airbyteAuthConfig.identityProvider.issuers}" }
    } else {
      logger.info { "Not validating issuers, this is recommended for a production system" }
    }
  }

  override fun validate(
    claims: Claims?,
    request: T,
  ): Boolean {
    if (!airbyteAuthConfig.identityProvider.verifyIssuer) {
      logger.debug { "Verifying the issuer has been set to false, not verifying issuer" }
      return true
    }

    if (claims == null || claims["iss"] == null) {
      logger.debug { "The claims were null or there is no issuer in the jwt claims" }
      return false
    }
    logger.debug { "Validating issuer for the configured issuers ${airbyteAuthConfig.identityProvider.issuers}" }

    return airbyteAuthConfig.identityProvider.issuers.any {
      logger.debug { "Verifying the issuer $it against the iss claim: ${claims["iss"]}" }
      it.trim() == claims["iss"].toString().trim()
    }
  }
}
