/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.api.server.services

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.airbyte_api.model.generated.ActorTypeEnum
import io.airbyte.api.client.model.generated.CompleteOAuthResponse
import io.airbyte.api.client.model.generated.OAuthConsentRead
import io.micronaut.http.HttpResponse
import java.util.UUID
import javax.validation.constraints.NotBlank

interface OAuthService {
  fun getSourceConsentUrl(
    workspaceId: @NotBlank UUID,
    definitionId: @NotBlank UUID,
    redirectUrl: @NotBlank String,
    oauthInputConfiguration: JsonNode,
    authorization: String?,
    userInfo: String?,
  ): OAuthConsentRead

  fun completeSourceOAuthReturnSecret(
    workspaceId: @NotBlank UUID,
    definitionId: @NotBlank UUID,
    redirectUrl: @NotBlank String,
    queryParameters: @NotBlank Map<String, String>,
    oauthInputConfiguration: JsonNode,
    authorization: String?,
    userInfo: String?,
  ): HttpResponse<CompleteOAuthResponse>

  fun setWorkspaceOverrideOAuthParams(
    workspaceId: UUID,
    actorType: ActorTypeEnum,
    definitionId: UUID,
    oauthCredentialsConfiguration: JsonNode,
    authorization: String?,
    userInfo: String?,
  )
}
