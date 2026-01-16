/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.data

import io.airbyte.commons.auth.RequiresAuthMode
import io.airbyte.commons.auth.config.AuthMode
import io.airbyte.commons.auth.roles.AuthRole
import io.airbyte.config.Application
import io.airbyte.config.AuthenticatedUser
import io.airbyte.data.services.ApplicationService
import io.airbyte.micronaut.runtime.AirbyteAuthConfig
import io.micronaut.security.token.jwt.generator.JwtTokenGenerator
import jakarta.inject.Singleton
import jakarta.ws.rs.BadRequestException
import java.time.Instant
import java.time.OffsetDateTime
import java.time.temporal.ChronoUnit
import java.util.UUID

@Singleton
@RequiresAuthMode(AuthMode.SIMPLE)
class ApplicationServiceMicronautImpl(
  private val airbyteAuthConfig: AirbyteAuthConfig,
  private val jwtTokenGenerator: JwtTokenGenerator,
) : ApplicationService {
  override fun listApplicationsByUser(user: AuthenticatedUser): List<Application> =
    listOf(
      Application()
        .withName(user.name + " Application")
        .withId(UUID.randomUUID().toString())
        .withClientId(airbyteAuthConfig.instanceAdmin.clientId)
        .withClientSecret(airbyteAuthConfig.instanceAdmin.clientSecret)
        .withCreatedOn(OffsetDateTime.now().toString()),
    )

  override fun getToken(
    clientId: String,
    clientSecret: String,
  ): String {
    if (clientId != airbyteAuthConfig.instanceAdmin.clientId || clientSecret != airbyteAuthConfig.instanceAdmin.clientSecret) {
      throw BadRequestException("Invalid client id or token")
    }

    return jwtTokenGenerator
      .generateToken(
        mapOf(
          "iss" to airbyteAuthConfig.tokenIssuer,
          "aud" to "airbyte-server",
          "sub" to DEFAULT_AUTH_USER_ID,
          "roles" to AuthRole.getInstanceAdminRoles(),
          "exp" to
            Instant
              .now()
              .plus(
                airbyteAuthConfig.tokenExpiration.applicationTokenExpirationInMinutes,
                ChronoUnit.MINUTES,
              ).epochSecond,
        ),
      )
      // Necessary now that this is no longer optional, but I don't know under what conditions we could
      // end up here.
      .orElseThrow { BadRequestException("Could not generate token") }
  }

  override fun createApplication(
    user: AuthenticatedUser,
    name: String,
  ): Application = throw UnsupportedOperationException()

  override fun deleteApplication(
    user: AuthenticatedUser,
    applicationId: String,
  ): Application = throw UnsupportedOperationException()

  companion object {
    @JvmField
    val DEFAULT_AUTH_USER_ID: UUID = UUID.fromString("00000000-0000-0000-0000-000000000000")
  }
}
