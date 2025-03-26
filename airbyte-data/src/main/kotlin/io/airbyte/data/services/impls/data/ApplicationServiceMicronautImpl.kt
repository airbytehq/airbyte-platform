/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.data

import io.airbyte.commons.auth.AuthRole
import io.airbyte.commons.auth.OrganizationAuthRole
import io.airbyte.commons.auth.RequiresAuthMode
import io.airbyte.commons.auth.WorkspaceAuthRole
import io.airbyte.commons.auth.config.AuthMode
import io.airbyte.config.Application
import io.airbyte.config.AuthenticatedUser
import io.airbyte.data.config.InstanceAdminConfig
import io.airbyte.data.services.ApplicationService
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
  private val instanceAdminConfig: InstanceAdminConfig,
  private val jwtTokenGenerator: JwtTokenGenerator,
) : ApplicationService {
  override fun listApplicationsByUser(user: AuthenticatedUser): List<Application> =
    listOf(
      Application()
        .withName(user.name + " Application")
        .withId(UUID.randomUUID().toString())
        .withClientId(instanceAdminConfig.clientId)
        .withClientSecret(instanceAdminConfig.clientSecret)
        .withCreatedOn(OffsetDateTime.now().toString()),
    )

  override fun getToken(
    clientId: String,
    clientSecret: String,
  ): String {
    if (clientId != instanceAdminConfig.clientId || clientSecret != instanceAdminConfig.clientSecret) {
      throw BadRequestException("Invalid client id or token")
    }

    return jwtTokenGenerator
      .generateToken(
        mapOf(
          "iss" to "airbyte-server",
          "sub" to DEFAULT_AUTH_USER_ID,
          "roles" to
            buildSet {
              addAll(AuthRole.buildAuthRolesSet(AuthRole.ADMIN))
              addAll(WorkspaceAuthRole.buildWorkspaceAuthRolesSet(WorkspaceAuthRole.WORKSPACE_ADMIN))
              addAll(OrganizationAuthRole.buildOrganizationAuthRolesSet(OrganizationAuthRole.ORGANIZATION_ADMIN))
            },
          "exp" to Instant.now().plus(24, ChronoUnit.HOURS).epochSecond,
        ),
      ) // Necessary now that this is no longer optional, but I don't know under what conditions we could
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
