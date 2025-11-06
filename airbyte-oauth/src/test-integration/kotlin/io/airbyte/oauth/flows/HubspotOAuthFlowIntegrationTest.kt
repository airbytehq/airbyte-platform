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
import io.airbyte.oauth.REFRESH_TOKEN_KEY
import io.github.oshai.kotlinlogging.KotlinLogging
import org.junit.jupiter.api.Assertions.assertTrue
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
class HubspotOAuthFlowIntegrationTest : OAuthFlowIntegrationTest() {
  override fun getCredentialsPath(): Path = Path.of("secrets/hubspot.json")

  internal val flowObject: OAuthFlowImplementation
    get() = HubspotOAuthFlow(httpClient)

  override fun getFlowImplementation(
    oauthService: OAuthService,
    httpClient: HttpClient,
  ): OAuthFlowImplementation = HubspotOAuthFlow(httpClient)

  @Test
  fun testFullOAuthFlow() {
    var limit = 100
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
              CLIENT_ID_KEY to credentialsJson["credentials"][CLIENT_ID_KEY].asText(),
              CLIENT_SECRET_KEY to credentialsJson["credentials"][CLIENT_SECRET_KEY].asText(),
            ),
          ),
        )
    Mockito
      .`when`(oauthService.getSourceOAuthParameterOptional(ArgumentMatchers.any(), ArgumentMatchers.any()))
      .thenReturn(Optional.of(sourceOAuthParameter))
    val flowObject = getFlowImplementation(oauthService, httpClient)
    val url =
      flowObject.getSourceConsentUrl(
        workspaceId,
        definitionId,
        REDIRECT_URL,
        Jsons.emptyObject(),
        null,
        sourceOAuthParameter.configuration,
      )
    log.info { "Waiting for user consent at: $url" }
    // TODO: To automate, start a selenium job to navigate to the Consent URL and click on allowing
    // access...
    while (!serverHandler.isSucceeded && limit > 0) {
      Thread.sleep(1000)
      limit -= 1
    }
    assertTrue(serverHandler.isSucceeded, "Failed to get User consent on time")
    val params =
      flowObject.completeSourceOAuth(
        workspaceId,
        definitionId,
        java.util.Map.of(AUTH_CODE_KEY, serverHandler.paramValue),
        REDIRECT_URL,
        sourceOAuthParameter.configuration,
      )
    log.info { "Response from completing OAuth Flow is: $params" }
    assertTrue(params.containsKey("credentials"))
    val credentials = params["credentials"] as Map<String, Any>?
    assertTrue(credentials!!.containsKey(REFRESH_TOKEN_KEY))
    assertTrue(credentials[REFRESH_TOKEN_KEY].toString().isNotEmpty())
    assertTrue(credentials.containsKey("access_token"))
    assertTrue(credentials["access_token"].toString().isNotEmpty())
  }
}
