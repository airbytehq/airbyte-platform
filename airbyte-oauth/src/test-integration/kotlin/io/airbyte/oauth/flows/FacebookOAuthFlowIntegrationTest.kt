/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.oauth.flows

import io.airbyte.commons.json.Jsons
import io.airbyte.config.SourceOAuthParameter
import io.airbyte.data.services.OAuthService
import io.airbyte.oauth.AUTH_CODE_KEY
import io.airbyte.oauth.CLIENT_ID_KEY
import io.airbyte.oauth.CLIENT_SECRET_KEY
import io.airbyte.oauth.OAuthFlowImplementation
import io.airbyte.oauth.flows.facebook.FacebookMarketingOAuthFlow
import io.github.oshai.kotlinlogging.KotlinLogging
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.mockito.ArgumentMatchers
import org.mockito.Mockito
import java.net.http.HttpClient
import java.nio.file.Files
import java.nio.file.Path
import java.util.Optional
import java.util.UUID

private val log = KotlinLogging.logger {}

@Tag("oauth")
class FacebookOAuthFlowIntegrationTest : OAuthFlowIntegrationTest() {
  override fun getCredentialsPath(): Path = CREDENTIALS_PATH

  override fun getFlowImplementation(
    oauthService: OAuthService,
    httpClient: HttpClient,
  ): OAuthFlowImplementation = FacebookMarketingOAuthFlow(httpClient)

  @BeforeEach
  override fun setup() {
    super.setup()
  }

  override fun getServerListeningPort(): Int = 9000

  @Test
  fun testFullFacebookOAuthFlow() {
    val workspaceId = UUID.randomUUID()
    val definitionId = UUID.randomUUID()
    val fullConfigAsString = Files.readString(CREDENTIALS_PATH)
    val credentialsJson = Jsons.deserialize(fullConfigAsString)
    val param =
      SourceOAuthParameter()
        .withOauthParameterId(UUID.randomUUID())
        .withSourceDefinitionId(definitionId)
        .withWorkspaceId(workspaceId)
        .withConfiguration(
          Jsons.jsonNode(
            mapOf(
              CLIENT_ID_KEY to credentialsJson[CLIENT_ID_KEY].asText(),
              CLIENT_SECRET_KEY to credentialsJson[CLIENT_SECRET_KEY].asText(),
            ),
          ),
        )
    Mockito.`when`(oauthService.getSourceOAuthParameterOptional(ArgumentMatchers.any(), ArgumentMatchers.any())).thenReturn(Optional.of(param))
    val url =
      flow.getSourceConsentUrl(workspaceId, definitionId, REDIRECT_URL, Jsons.emptyObject(), null, param.configuration)
    log.info { "Waiting for user consent at: $url" }
    waitForResponse(20)
    assertTrue(serverHandler.isSucceeded, "Failed to get User consent on time")
    val params =
      flow.completeSourceOAuth(
        workspaceId,
        definitionId,
        mapOf(AUTH_CODE_KEY to serverHandler.paramValue),
        REDIRECT_URL,
        param.configuration,
      )
    log.info { "Response from completing OAuth Flow is: $params" }
    assertTrue(params.containsKey("access_token"))
    assertTrue(params["access_token"].toString().isNotEmpty())
  }

  companion object {
    protected val CREDENTIALS_PATH: Path = Path.of("secrets/facebook_marketing.json")
    protected const val REDIRECT_URL: String = "http://localhost:9000/auth_flow"
  }
}
