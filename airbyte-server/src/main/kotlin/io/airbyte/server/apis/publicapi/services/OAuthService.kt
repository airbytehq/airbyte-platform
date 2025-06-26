/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.publicapi.services

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.api.model.generated.CompleteOAuthResponse
import io.airbyte.api.model.generated.OAuthConsentRead
import io.airbyte.publicApi.server.generated.models.ActorTypeEnum
import io.micronaut.http.HttpResponse
import jakarta.validation.constraints.NotBlank
import java.util.UUID

interface OAuthService {
  fun getSourceConsentUrl(
    workspaceId: @NotBlank UUID,
    definitionId: @NotBlank UUID?,
    redirectUrl: @NotBlank String,
    oauthInputConfiguration: JsonNode,
  ): OAuthConsentRead

  fun completeSourceOAuthReturnSecret(
    workspaceId: @NotBlank UUID,
    definitionId: @NotBlank UUID,
    redirectUrl: @NotBlank String,
    queryParameters: @NotBlank Map<String, String>,
    oauthInputConfiguration: JsonNode,
  ): HttpResponse<CompleteOAuthResponse>

  fun setWorkspaceOverrideOAuthParams(
    workspaceId: UUID,
    actorType: ActorTypeEnum,
    definitionId: UUID,
    oauthCredentialsConfiguration: JsonNode,
  )
}
