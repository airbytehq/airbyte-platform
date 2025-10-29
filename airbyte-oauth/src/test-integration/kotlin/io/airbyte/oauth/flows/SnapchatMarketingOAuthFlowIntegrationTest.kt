/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.oauth.flows

import io.airbyte.commons.json.Jsons
import io.airbyte.config.SourceOAuthParameter
import io.airbyte.config.persistence.ConfigNotFoundException
import io.airbyte.data.services.OAuthService
import io.airbyte.oauth.AUTH_CODE_KEY
import io.airbyte.oauth.CLIENT_ID_KEY
import io.airbyte.oauth.CLIENT_SECRET_KEY
import io.airbyte.oauth.OAuthFlowImplementation
import io.airbyte.oauth.REFRESH_TOKEN_KEY
import io.airbyte.validation.json.JsonValidationException
import io.github.oshai.kotlinlogging.KotlinLogging
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.mockito.ArgumentMatchers
import org.mockito.Mockito
import java.io.IOException
import java.net.http.HttpClient
import java.nio.file.Files
import java.nio.file.Path
import java.util.Optional
import java.util.UUID

private val log = KotlinLogging.logger {}

@Tag("oauth")
class SnapchatMarketingOAuthFlowIntegrationTest : OAuthFlowIntegrationTest() {
  override fun getCredentialsPath(): Path = Path.of("secrets/snapchat.json")

  override fun getRedirectUrl(): String = "https://f215-195-114-147-152.ngrok.io/auth_flow"

  override fun getServerListeningPort(): Int = 3000

  override fun getFlowImplementation(
    oauthService: OAuthService,
    httpClient: HttpClient,
  ): OAuthFlowImplementation = SnapchatMarketingOAuthFlow(httpClient)

  @Test
  fun testFullSnapchatMarketingOAuthFlow() {
    val workspaceId = UUID.randomUUID()
    val definitionId = UUID.randomUUID()
    val fullConfigAsString = Files.readString(getCredentialsPath())
    val credentialsJson = Jsons.deserialize(fullConfigAsString)
    val sourceOAuthParameter =
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
    Mockito
      .`when`(oauthService.getSourceOAuthParameterOptional(ArgumentMatchers.any(), ArgumentMatchers.any()))
      .thenReturn(Optional.of(sourceOAuthParameter))
    val url =
      getFlowImplementation(oauthService, httpClient).getSourceConsentUrl(
        workspaceId,
        definitionId,
        getRedirectUrl(),
        Jsons.emptyObject(),
        null,
        sourceOAuthParameter.configuration,
      )
    log.info { "Waiting for user consent at: $url" }
    waitForResponse(20)
    assertTrue(serverHandler.isSucceeded, "Failed to get User consent on time")
    val params =
      flow.completeSourceOAuth(
        workspaceId,
        definitionId,
        mapOf(AUTH_CODE_KEY to serverHandler.paramValue),
        getRedirectUrl(),
        sourceOAuthParameter.configuration,
      )
    log.info { "Response from completing OAuth Flow is: $params" }
    assertTrue(params.containsKey(REFRESH_TOKEN_KEY))
    assertTrue(params[REFRESH_TOKEN_KEY].toString().isNotEmpty())
  }
}
