/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.problems.model.generated.ProblemMessageData
import io.airbyte.api.problems.throwable.generated.BadRequestProblem
import io.airbyte.api.server.generated.apis.SourceOauthApi
import io.airbyte.api.server.generated.models.CompleteOAuthResponse
import io.airbyte.api.server.generated.models.CompleteSourceOauthRequest
import io.airbyte.api.server.generated.models.EmbeddedSourceOauthConsentRequest
import io.airbyte.api.server.generated.models.OAuthConsentRead
import io.airbyte.api.server.generated.models.RevokeSourceOauthTokensRequest
import io.airbyte.api.server.generated.models.SetInstancewideSourceOauthParamsRequestBody
import io.airbyte.api.server.generated.models.SourceOAuthScopesRead
import io.airbyte.api.server.generated.models.SourceOAuthScopesRequest
import io.airbyte.api.server.generated.models.SourceOauthConsentRequest
import io.airbyte.commons.auth.generated.Intent
import io.airbyte.commons.auth.permissions.RequiresIntent
import io.airbyte.commons.auth.route.AuthorizeOperation
import io.airbyte.commons.auth.route.RouteAccessPolicy
import io.airbyte.commons.auth.route.RouteMigration
import io.airbyte.commons.json.Jsons
import io.airbyte.commons.server.handlers.OAuthHandler
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors
import io.airbyte.server.apis.execute
import io.micronaut.http.annotation.Body
import io.micronaut.http.annotation.Controller
import io.micronaut.scheduling.annotation.ExecuteOn
import io.micronaut.security.annotation.Secured
import io.micronaut.security.rules.SecurityRule

@Controller
@RequiresIntent(Intent.RunOAuthFlow)
class SourceOauthApiController(
  private val oAuthHandler: OAuthHandler,
) : SourceOauthApi {
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Secured(SecurityRule.IS_AUTHENTICATED)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = io.airbyte.server.auth.SourceOAuthFlowRule::class,
  )
  override fun completeSourceOAuth(
    @Body completeSourceOauthRequest: CompleteSourceOauthRequest,
  ): CompleteOAuthResponse {
    // the spec leaves redirectUrl optional, but every OAuth flow needs it to finish
    if (completeSourceOauthRequest.redirectUrl == null) {
      throw BadRequestProblem(ProblemMessageData().message("redirectUrl is required"))
    }
    return execute {
      oAuthHandler.completeSourceOAuthHandleReturnSecret(
        completeSourceOauthRequest,
      )
    }
  }

  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Secured(SecurityRule.IS_AUTHENTICATED)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = io.airbyte.server.auth.SourceOAuthFlowRule::class,
  )
  override fun getEmbeddedSourceOAuthConsent(
    @Body embeddedSourceOauthConsentRequest: EmbeddedSourceOauthConsentRequest,
  ): OAuthConsentRead {
    val sourceOauthConsentRequest =
      SourceOauthConsentRequest(
        sourceDefinitionId = embeddedSourceOauthConsentRequest.sourceDefinitionId,
        workspaceId = embeddedSourceOauthConsentRequest.workspaceId,
        redirectUrl = embeddedSourceOauthConsentRequest.redirectUrl,
        sourceId = embeddedSourceOauthConsentRequest.sourceId,
        oAuthInputConfiguration = Jsons.emptyObject(),
      )
    return execute { oAuthHandler.getSourceOAuthConsent(sourceOauthConsentRequest) }
  }

  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Secured(SecurityRule.IS_AUTHENTICATED)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = io.airbyte.server.auth.SourceOAuthFlowRule::class,
  )
  override fun getSourceOAuthScopes(
    @Body request: SourceOAuthScopesRequest,
  ): SourceOAuthScopesRead = execute { oAuthHandler.getSourceOAuthScopes(request) }

  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Secured(SecurityRule.IS_AUTHENTICATED)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = io.airbyte.server.auth.SourceOAuthFlowRule::class,
  )
  override fun getSourceOAuthConsent(
    @Body sourceOauthConsentRequest: SourceOauthConsentRequest,
  ): OAuthConsentRead = execute { oAuthHandler.getSourceOAuthConsent(sourceOauthConsentRequest) }

  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Secured(SecurityRule.IS_AUTHENTICATED)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = io.airbyte.server.auth.SourceOAuthFlowRule::class,
  )
  override fun revokeSourceOAuthTokens(
    @Body revokeSourceOauthTokensRequest: RevokeSourceOauthTokensRequest,
  ) {
    execute<Any?> {
      oAuthHandler.revokeSourceOauthTokens(revokeSourceOauthTokensRequest)
      null
    }
  }

  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Secured(SecurityRule.IS_AUTHENTICATED)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = io.airbyte.server.auth.SourceOAuthInstanceRule::class,
  )
  override fun setInstancewideSourceOauthParams(
    @Body setInstancewideSourceOauthParamsRequestBody: SetInstancewideSourceOauthParamsRequestBody,
  ) {
    execute<Any?> {
      oAuthHandler.setSourceInstancewideOauthParams(setInstancewideSourceOauthParamsRequestBody)
      null
    }
  }
}
