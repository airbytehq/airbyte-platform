/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.oauth

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.protocol.models.v0.OAuthConfigSpecification
import io.airbyte.validation.json.JsonValidationException
import java.io.IOException
import java.util.UUID

/**
 * OAuth flow impl.
 */
interface OAuthFlowImplementation {
  fun getSourceConsentUrl(
    workspaceId: UUID,
    sourceDefinitionId: UUID?,
    redirectUrl: String,
    inputOAuthConfiguration: JsonNode,
    oauthConfigSpecification: OAuthConfigSpecification?,
    sourceOAuthParamConfig: JsonNode?,
  ): String

  fun getDestinationConsentUrl(
    workspaceId: UUID,
    destinationDefinitionId: UUID?,
    redirectUrl: String,
    inputOAuthConfiguration: JsonNode,
    oauthConfigSpecification: OAuthConfigSpecification?,
    destinationOAuthParamConfig: JsonNode?,
  ): String

  @Deprecated("")
  fun completeSourceOAuth(
    workspaceId: UUID,
    sourceDefinitionId: UUID?,
    queryParams: Map<String, Any>,
    redirectUrl: String,
    oauthParamConfig: JsonNode,
  ): Map<String, Any>

  fun completeSourceOAuth(
    workspaceId: UUID,
    sourceDefinitionId: UUID?,
    queryParams: Map<String, Any>,
    redirectUrl: String,
    inputOAuthConfiguration: JsonNode,
    oauthConfigSpecification: OAuthConfigSpecification,
    oauthParamConfig: JsonNode,
  ): Map<String, Any>

  @Deprecated("")
  fun completeDestinationOAuth(
    workspaceId: UUID,
    destinationDefinitionId: UUID?,
    queryParams: Map<String, Any>,
    redirectUrl: String,
    oauthParamConfig: JsonNode,
  ): Map<String, Any>

  fun completeDestinationOAuth(
    workspaceId: UUID,
    destinationDefinitionId: UUID?,
    queryParams: Map<String, Any>,
    redirectUrl: String,
    inputOAuthConfiguration: JsonNode,
    oauthConfigSpecification: OAuthConfigSpecification,
    oauthParamConfig: JsonNode,
  ): Map<String, Any>

  fun revokeSourceOauth(
    workspaceId: UUID,
    sourceDefinitionId: UUID,
    hydratedSourceConnectionConfiguration: JsonNode,
    oauthParamConfig: JsonNode,
  ) {
  }
}
