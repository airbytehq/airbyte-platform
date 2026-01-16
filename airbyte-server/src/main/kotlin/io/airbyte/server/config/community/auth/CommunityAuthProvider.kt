/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.config.community.auth

import io.airbyte.api.problems.model.generated.ProblemMessageData
import io.airbyte.api.problems.throwable.generated.ForbiddenProblem
import io.airbyte.commons.DEFAULT_USER_ID
import io.airbyte.commons.auth.RequiresAuthMode
import io.airbyte.commons.auth.config.AuthMode
import io.airbyte.commons.auth.roles.AuthRole
import io.airbyte.data.services.OrganizationService
import io.airbyte.micronaut.runtime.AirbyteAuthConfig
import io.micronaut.http.HttpRequest
import io.micronaut.security.authentication.AuthenticationRequest
import io.micronaut.security.authentication.AuthenticationResponse
import io.micronaut.security.authentication.provider.HttpRequestAuthenticationProvider
import jakarta.inject.Singleton
import java.util.UUID

const val SESSION_ID = "sessionId"

/**
 * This class is responsible for authenticating the user against the community provider.
 */
@Singleton
@RequiresAuthMode(AuthMode.SIMPLE)
class CommunityAuthProvider<B>(
  private val airbyteAuthConfig: AirbyteAuthConfig,
  private val organizationService: OrganizationService,
) : HttpRequestAuthenticationProvider<B> {
  override fun authenticate(
    requestContext: HttpRequest<B>?,
    authRequest: AuthenticationRequest<String, String>,
  ): AuthenticationResponse? {
    // The authRequest identity must match the default organization's email address that
    // was collected during the instanceConfiguration step.
    val defaultOrgEmail =
      organizationService
        .getDefaultOrganization()
        .orElseThrow {
          ForbiddenProblem(ProblemMessageData().message("Default organization not found. Cannot authenticate."))
        }.email

    if (authRequest.identity == defaultOrgEmail && authRequest.secret == airbyteAuthConfig.instanceAdmin.password) {
      val sessionId = UUID.randomUUID()
      val authenticationResponse =
        AuthenticationResponse.success(
          DEFAULT_USER_ID.toString(),
          AuthRole.getInstanceAdminRoles(),
          mapOf(SESSION_ID to sessionId.toString()),
        )
      return authenticationResponse
    }
    return AuthenticationResponse.failure("Invalid credentials")
  }
}
