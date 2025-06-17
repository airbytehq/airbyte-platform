/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.config.community.auth

import io.airbyte.api.problems.model.generated.ProblemMessageData
import io.airbyte.api.problems.throwable.generated.ForbiddenProblem
import io.airbyte.api.problems.throwable.generated.UnprocessableEntityProblem
import io.airbyte.commons.DEFAULT_USER_ID
import io.airbyte.commons.auth.roles.AuthRole
import io.airbyte.data.services.AuthRefreshTokenService
import io.micronaut.security.authentication.Authentication
import io.micronaut.security.token.event.RefreshTokenGeneratedEvent
import io.micronaut.security.token.refresh.RefreshTokenPersistence
import jakarta.inject.Singleton
import org.reactivestreams.Publisher
import reactor.core.publisher.Flux

@Singleton
class CommunityAuthRefreshTokenPersistence(
  val authRefreshTokenService: AuthRefreshTokenService,
) : RefreshTokenPersistence {
  override fun persistToken(event: RefreshTokenGeneratedEvent?) {
    val sessionId =
      event
        ?.authentication
        ?.attributes
        ?.get(SESSION_ID)
        ?.toString()
        ?: throw UnprocessableEntityProblem(ProblemMessageData().message("Session ID not found in authentication"))

    val refreshToken =
      event
        .refreshToken
        ?: throw UnprocessableEntityProblem(ProblemMessageData().message("Refresh token not found"))

    event.let { _ ->
      authRefreshTokenService.saveAuthRefreshToken(
        sessionId = sessionId,
        tokenValue = refreshToken,
      )
    }
  }

  override fun getAuthentication(refreshToken: String): Publisher<Authentication> =
    Flux.create {
      val token = authRefreshTokenService.getAuthRefreshToken(refreshToken)
      if (token == null) {
        it.error(ForbiddenProblem(ProblemMessageData().message("Refresh token not found: $refreshToken")))
      } else if (token.revoked) {
        it.error(ForbiddenProblem(ProblemMessageData().message("Refresh token revoked: $refreshToken")))
      } else {
        it
          .next(
            Authentication.build(
              DEFAULT_USER_ID.toString(),
              AuthRole.getInstanceAdminRoles(),
              mapOf(SESSION_ID to token.sessionId),
            ),
          ).complete()
      }
    }
}
