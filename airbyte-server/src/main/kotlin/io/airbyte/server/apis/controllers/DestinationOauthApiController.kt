/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.problems.model.generated.ProblemMessageData
import io.airbyte.api.problems.throwable.generated.BadRequestProblem
import io.airbyte.api.server.generated.apis.DestinationOauthApi
import io.airbyte.api.server.generated.models.CompleteDestinationOAuthRequest
import io.airbyte.api.server.generated.models.CompleteOAuthResponse
import io.airbyte.api.server.generated.models.DestinationOauthConsentRequest
import io.airbyte.api.server.generated.models.OAuthConsentRead
import io.airbyte.api.server.generated.models.SetInstancewideDestinationOauthParamsRequestBody
import io.airbyte.commons.auth.roles.AuthRoleConstants
import io.airbyte.commons.auth.route.AuthorizeOperation
import io.airbyte.commons.auth.route.RouteAccessPolicy
import io.airbyte.commons.auth.route.RouteMigration
import io.airbyte.commons.server.handlers.OAuthHandler
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors
import io.airbyte.server.apis.execute
import io.micronaut.context.annotation.Context
import io.micronaut.http.annotation.Body
import io.micronaut.http.annotation.Controller
import io.micronaut.scheduling.annotation.ExecuteOn
import io.micronaut.security.annotation.Secured
import io.micronaut.security.rules.SecurityRule

@Controller
@Context
@Secured(SecurityRule.IS_AUTHENTICATED)
class DestinationOauthApiController(
  private val oAuthHandler: OAuthHandler,
) : DestinationOauthApi {
  @Secured(AuthRoleConstants.WORKSPACE_EDITOR, AuthRoleConstants.WORKSPACE_DESTINATION_EDITOR, AuthRoleConstants.ORGANIZATION_EDITOR)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.LEGACY,
  )
  override fun completeDestinationOAuth(
    @Body completeDestinationOAuthRequest: CompleteDestinationOAuthRequest,
  ): CompleteOAuthResponse {
    // the spec leaves redirectUrl optional, but every OAuth flow needs it to finish
    if (completeDestinationOAuthRequest.redirectUrl == null) {
      throw BadRequestProblem(ProblemMessageData().message("redirectUrl is required"))
    }
    return execute { oAuthHandler.completeDestinationOAuth(completeDestinationOAuthRequest) }
  }

  @Secured(AuthRoleConstants.WORKSPACE_EDITOR, AuthRoleConstants.WORKSPACE_DESTINATION_EDITOR, AuthRoleConstants.ORGANIZATION_EDITOR)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.LEGACY,
  )
  override fun getDestinationOAuthConsent(
    @Body destinationOauthConsentRequest: DestinationOauthConsentRequest,
  ): OAuthConsentRead = execute { oAuthHandler.getDestinationOAuthConsent(destinationOauthConsentRequest) }

  @Secured(AuthRoleConstants.ADMIN)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.LEGACY,
  )
  override fun setInstancewideDestinationOauthParams(
    @Body setInstancewideDestinationOauthParamsRequestBody: SetInstancewideDestinationOauthParamsRequestBody,
  ) {
    execute<Any?> {
      oAuthHandler.setDestinationInstancewideOauthParams(setInstancewideDestinationOauthParamsRequestBody)
      null
    }
  }
}
