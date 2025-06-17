/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.generated.DestinationOauthApi
import io.airbyte.api.model.generated.CompleteDestinationOAuthRequest
import io.airbyte.api.model.generated.CompleteOAuthResponse
import io.airbyte.api.model.generated.DestinationOauthConsentRequest
import io.airbyte.api.model.generated.OAuthConsentRead
import io.airbyte.api.model.generated.SetInstancewideDestinationOauthParamsRequestBody
import io.airbyte.commons.auth.roles.AuthRoleConstants
import io.airbyte.commons.server.handlers.OAuthHandler
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors
import io.airbyte.server.apis.execute
import io.micronaut.context.annotation.Context
import io.micronaut.http.annotation.Body
import io.micronaut.http.annotation.Controller
import io.micronaut.http.annotation.Post
import io.micronaut.scheduling.annotation.ExecuteOn
import io.micronaut.security.annotation.Secured
import io.micronaut.security.rules.SecurityRule

@Controller("/api/v1/destination_oauths")
@Context
@Secured(SecurityRule.IS_AUTHENTICATED)
class DestinationOauthApiController(
  private val oAuthHandler: OAuthHandler,
) : DestinationOauthApi {
  @Post("/complete_oauth")
  @Secured(AuthRoleConstants.WORKSPACE_EDITOR, AuthRoleConstants.ORGANIZATION_EDITOR)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun completeDestinationOAuth(
    @Body completeDestinationOAuthRequest: CompleteDestinationOAuthRequest,
  ): CompleteOAuthResponse? = execute { oAuthHandler.completeDestinationOAuth(completeDestinationOAuthRequest) }

  @Post("/get_consent_url")
  @Secured(AuthRoleConstants.WORKSPACE_EDITOR, AuthRoleConstants.ORGANIZATION_EDITOR)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun getDestinationOAuthConsent(
    @Body destinationOauthConsentRequest: DestinationOauthConsentRequest,
  ): OAuthConsentRead? = execute { oAuthHandler.getDestinationOAuthConsent(destinationOauthConsentRequest) }

  @Post("/oauth_params/create")
  @Secured(AuthRoleConstants.ADMIN)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun setInstancewideDestinationOauthParams(
    @Body setInstancewideDestinationOauthParamsRequestBody: SetInstancewideDestinationOauthParamsRequestBody,
  ) {
    execute<Any?> {
      oAuthHandler.setDestinationInstancewideOauthParams(setInstancewideDestinationOauthParamsRequestBody)
      null
    }
  }
}
