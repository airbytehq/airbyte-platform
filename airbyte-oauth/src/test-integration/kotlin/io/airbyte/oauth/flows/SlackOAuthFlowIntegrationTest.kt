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
class SlackOAuthFlowIntegrationTest : OAuthFlowIntegrationTest() {
  override fun getCredentialsPath(): Path = Path.of("secrets/slack.json")

  override fun getRedirectUrl(): String = "https://27b0-2804-14d-2a76-9a9a-fdbb-adee-9e5d-6c.ngrok.io/auth_flow"

  override fun getFlowImplementation(
    oauthService: OAuthService,
    httpClient: HttpClient,
  ): OAuthFlowImplementation = SlackOAuthFlow(httpClient)

  @Test
  fun testFullSlackOAuthFlow() {
    var limit = 20
    val workspaceId = UUID.randomUUID()
    val definitionId = UUID.randomUUID()
    val fullConfigAsString = Files.readString(getCredentialsPath())
    val credentialsJson = Jsons.deserialize(fullConfigAsString)["credentials"]
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
    // TODO: To automate, start a selenium job to navigate to the Consent URL and click on allowing
    // access...
    while (!serverHandler.isSucceeded && limit > 0) {
      Thread.sleep(1000)
      limit -= 1
    }
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
    assertTrue(params.containsKey("credentials"))
    assertTrue((params["credentials"] as Map<*, *>).containsKey("access_token"))
    assertTrue((params["credentials"] as Map<*, *>)["access_token"].toString().isNotEmpty())
  }
}
