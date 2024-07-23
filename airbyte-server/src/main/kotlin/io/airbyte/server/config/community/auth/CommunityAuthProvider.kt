/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */
package io.airbyte.server.config.community.auth

import io.airbyte.commons.auth.RequiresAuthMode
import io.airbyte.commons.auth.config.AuthMode
import io.airbyte.commons.server.support.RbacRoleHelper
import io.airbyte.config.persistence.UserPersistence
import io.airbyte.data.config.InstanceAdminConfig
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
  private val instanceAdminConfig: InstanceAdminConfig,
) : HttpRequestAuthenticationProvider<B> {
  override fun authenticate(
    requestContext: HttpRequest<B>?,
    authRequest: AuthenticationRequest<String, String>,
  ): AuthenticationResponse? {
    if (authRequest.identity == instanceAdminConfig.username && authRequest.secret == instanceAdminConfig.password) {
      val sessionId = UUID.randomUUID()
      val authenticationResponse =
        AuthenticationResponse.success(
          UserPersistence.DEFAULT_USER_ID.toString(),
          RbacRoleHelper.getInstanceAdminRoles(),
          mapOf(SESSION_ID to sessionId.toString()),
        )
      return authenticationResponse
    }
    return AuthenticationResponse.failure("Invalid credentials")
  }
}
