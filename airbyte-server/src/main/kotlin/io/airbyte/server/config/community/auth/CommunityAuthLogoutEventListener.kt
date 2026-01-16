/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.config.community.auth

import io.airbyte.api.problems.model.generated.ProblemMessageData
import io.airbyte.api.problems.throwable.generated.UnprocessableEntityProblem
import io.airbyte.data.services.AuthRefreshTokenService
import io.micronaut.context.event.ApplicationEventListener
import io.micronaut.security.authentication.Authentication
import io.micronaut.security.event.LogoutEvent
import jakarta.inject.Singleton

@Singleton
class CommunityAuthLogoutEventListener(
  private val refreshTokenService: AuthRefreshTokenService,
) : ApplicationEventListener<LogoutEvent> {
  override fun onApplicationEvent(event: LogoutEvent) {
    val sessionId =
      (event.source as? Authentication)
        ?.attributes
        ?.get(SESSION_ID)
        ?.toString()
        ?: throw UnprocessableEntityProblem(ProblemMessageData().message("Could not retrieve session ID from authentication context"))

    refreshTokenService.revokeAuthRefreshToken(sessionId)
  }
}
