/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.data

import io.airbyte.commons.auth.config.TokenExpirationConfig
import io.airbyte.commons.auth.roles.AuthRole
import io.airbyte.data.auth.TokenType
import io.airbyte.data.services.DataplaneTokenService
import io.airbyte.data.services.ServiceAccountsService
import io.micronaut.context.annotation.Property
import io.micronaut.context.annotation.Replaces
import io.micronaut.context.annotation.Requires
import io.micronaut.security.token.jwt.generator.JwtTokenGenerator
import jakarta.inject.Singleton
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.UUID

@Singleton
@Requires(property = "micronaut.security.enabled", value = "true")
@Requires(property = "micronaut.security.token.jwt.enabled", value = "true")
@Replaces(DataplaneTokenServiceNoAuthImpl::class)
class DataplaneTokenServiceDataImpl(
  private val jwtTokenGenerator: JwtTokenGenerator,
  private val tokenExpirationConfig: TokenExpirationConfig,
  private val serviceAccountsService: ServiceAccountsService,
  @Property(name = "airbyte.auth.token-issuer") private val tokenIssuer: String,
) : DataplaneTokenService {
  /**
   * Gets a token
   * @param clientId The client ID for the dataplane
   * @param clientSecret The client secret for the dataplane
   * @return A JWT that authorizes the user to make requests
   */
  override fun getToken(
    clientId: String,
    clientSecret: String,
  ): String {
    val serviceAccount = serviceAccountsService.getAndVerify(UUID.fromString(clientId), clientSecret)

    return jwtTokenGenerator
      .generateToken(
        mapOf(
          "iss" to tokenIssuer,
          "sub" to serviceAccount.id,
          TokenType.DATAPLANE_V1.toClaim(),
          "roles" to AuthRole.getInstanceAdminRoles(),
          "exp" to Instant.now().plus(tokenExpirationConfig.dataplaneTokenExpirationInMinutes, ChronoUnit.MINUTES).epochSecond,
        ),
      ).orElseThrow {
        IllegalStateException(
          "Could not generate token",
        )
      }
  }
}
