/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.generated.SourceOauthApi
import io.airbyte.api.model.generated.CompleteOAuthResponse
import io.airbyte.api.model.generated.CompleteSourceOauthRequest
import io.airbyte.api.model.generated.OAuthConsentRead
import io.airbyte.api.model.generated.RevokeSourceOauthTokensRequest
import io.airbyte.api.model.generated.SetInstancewideSourceOauthParamsRequestBody
import io.airbyte.api.model.generated.SourceOauthConsentRequest
import io.airbyte.commons.auth.AuthRoleConstants
import io.airbyte.commons.auth.generated.Intent
import io.airbyte.commons.auth.permissions.RequiresIntent
import io.airbyte.commons.server.handlers.OAuthHandler
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors
import io.airbyte.server.apis.execute
import io.micronaut.http.annotation.Body
import io.micronaut.http.annotation.Controller
import io.micronaut.http.annotation.Post
import io.micronaut.scheduling.annotation.ExecuteOn
import io.micronaut.security.annotation.Secured

@Controller("/api/v1/source_oauths")
@RequiresIntent(Intent.RunOAuthFlow)
class SourceOauthApiController(
  private val oAuthHandler: OAuthHandler,
) : SourceOauthApi {
  @Post("/complete_oauth")
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun completeSourceOAuth(
    @Body completeSourceOauthRequest: CompleteSourceOauthRequest?,
  ): CompleteOAuthResponse? =
    execute {
      oAuthHandler.completeSourceOAuthHandleReturnSecret(
        completeSourceOauthRequest,
      )
    }

  @Post("/get_consent_url")
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun getSourceOAuthConsent(
    @Body sourceOauthConsentRequest: SourceOauthConsentRequest,
  ): OAuthConsentRead? = execute { oAuthHandler.getSourceOAuthConsent(sourceOauthConsentRequest) }

  @Post("/revoke")
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun revokeSourceOAuthTokens(
    @Body revokeSourceOauthTokensRequest: RevokeSourceOauthTokensRequest,
  ) {
    execute<Any?> {
      oAuthHandler.revokeSourceOauthTokens(revokeSourceOauthTokensRequest)
      null
    }
  }

  @Post("/oauth_params/create")
  @Secured(AuthRoleConstants.ADMIN)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun setInstancewideSourceOauthParams(
    @Body setInstancewideSourceOauthParamsRequestBody: SetInstancewideSourceOauthParamsRequestBody,
  ) {
    execute<Any?> {
      oAuthHandler.setSourceInstancewideOauthParams(setInstancewideSourceOauthParamsRequestBody)
      null
    }
  }
}
