/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.data

import io.airbyte.commons.auth.AuthRole
import io.airbyte.commons.auth.config.TokenExpirationConfig
import io.airbyte.data.helpers.DataplanePasswordEncoder
import io.airbyte.data.repositories.DataplaneClientCredentialsRepository
import io.airbyte.data.services.DataplaneTokenService
import io.micronaut.context.annotation.Property
import io.micronaut.context.annotation.Replaces
import io.micronaut.context.annotation.Requires
import io.micronaut.security.token.jwt.generator.JwtTokenGenerator
import jakarta.inject.Singleton
import java.time.Instant
import java.time.temporal.ChronoUnit

@Singleton
@Requires(property = "micronaut.security.enabled", value = "true")
@Requires(property = "micronaut.security.token.jwt.enabled", value = "true")
@Replaces(DataplaneTokenServiceNoAuthImpl::class)
class DataplaneTokenServiceDataImpl(
  private val dataplaneClientCredentialsRepository: DataplaneClientCredentialsRepository,
  private val jwtTokenGenerator: JwtTokenGenerator,
  private val dataplanePasswordEncoder: DataplanePasswordEncoder,
  private val tokenExpirationConfig: TokenExpirationConfig,
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
    val dataplaneClientCredentials =
      dataplaneClientCredentialsRepository.findByClientId(clientId)
        ?: throw IllegalArgumentException("Credentials were not found with the clientId provided. clientId: $clientId")

    if (!dataplanePasswordEncoder.matches(clientSecret, dataplaneClientCredentials.clientSecret)) {
      throw IllegalArgumentException("Invalid clientSecret provided.")
    }
    return jwtTokenGenerator
      .generateToken(
        mapOf(
          "iss" to tokenIssuer,
          "sub" to dataplaneClientCredentials.dataplaneId,
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
