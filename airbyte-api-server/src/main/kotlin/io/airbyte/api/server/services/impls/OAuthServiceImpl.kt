package io.airbyte.api.server.services.impls

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.airbyte_api.model.generated.ActorTypeEnum
import io.airbyte.api.client.model.generated.CompleteOAuthResponse
import io.airbyte.api.client.model.generated.OAuthConsentRead
import io.airbyte.api.server.services.OAuthService
import java.util.UUID

class OAuthServiceImpl : OAuthService {
  override fun getSourceConsentUrl(
    workspaceId: UUID,
    definitionId: UUID,
    redirectUrl: String,
    oauthInputConfiguration: JsonNode,
    userInfo: String,
  ): OAuthConsentRead {
    TODO("Not yet implemented")
  }

  override fun completeSourceOAuthReturnSecret(
    workspaceId: UUID,
    definitionId: UUID,
    redirectUrl: String,
    queryParameters: MutableMap<String, String>,
    oauthInputConfiguration: JsonNode,
    userInfo: String,
  ): CompleteOAuthResponse {
    TODO("Not yet implemented")
  }

  override fun setWorkspaceOverrideOAuthParams(
    workspaceId: UUID,
    actorType: ActorTypeEnum,
    definitionId: UUID,
    oauthCredentialsConfiguration: JsonNode,
    userInfo: String,
  ) {
    TODO("Not yet implemented")
  }
}
