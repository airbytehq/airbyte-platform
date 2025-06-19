/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.oauth.flows

import com.google.common.collect.ImmutableMap
import io.airbyte.commons.json.Jsons
import io.airbyte.config.SourceOAuthParameter
import io.airbyte.config.persistence.ConfigNotFoundException
import io.airbyte.data.services.OAuthService
import io.airbyte.oauth.OAuthFlowImplementation
import io.airbyte.validation.json.JsonValidationException
import io.github.oshai.kotlinlogging.KotlinLogging
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.mockito.ArgumentMatchers
import org.mockito.Mockito
import java.io.IOException
import java.net.http.HttpClient
import java.nio.file.Files
import java.nio.file.Path
import java.util.Collections
import java.util.Optional
import java.util.UUID

private val log = KotlinLogging.logger {}

@Tag("oauth")
class QuickbooksOAuthFlowIntegrationTest : OAuthFlowIntegrationTest() {
  override fun getServerListeningPort(): Int = 3000

  override fun getCredentialsPath(): Path = CREDENTIALS_PATH

  override fun getFlowImplementation(
    oauthService: OAuthService,
    httpClient: HttpClient,
  ): OAuthFlowImplementation = QuickbooksOAuthFlow(httpClient)

  @Test
  @Throws(
    InterruptedException::class,
    ConfigNotFoundException::class,
    IOException::class,
    JsonValidationException::class,
  )
  fun testFullOAuthFlow() {
    var limit = 20
    val workspaceId = UUID.randomUUID()
    val definitionId = UUID.randomUUID()
    val fullConfigAsString = Files.readString(CREDENTIALS_PATH)
    val credentialsJson = Jsons.deserialize(fullConfigAsString)
    val sourceOAuthParameter =
      SourceOAuthParameter()
        .withOauthParameterId(UUID.randomUUID())
        .withSourceDefinitionId(definitionId)
        .withWorkspaceId(workspaceId)
        .withConfiguration(
          Jsons.jsonNode(
            java.util.Map.of(
              "credentials",
              ImmutableMap
                .builder<Any, Any>()
                .put("client_id", credentialsJson["client_id"].asText())
                .put("client_secret", credentialsJson["client_secret"].asText())
                .build(),
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
        REDIRECT_URL,
        Jsons.emptyObject(),
        null,
        sourceOAuthParameter.configuration,
      )
    log.info("Waiting for user consent at: {}", url)
    // TODO: To automate, start a selenium job to navigate to the Consent URL and click on allowing
    // access...
    while (!serverHandler.isSucceeded && limit > 0) {
      Thread.sleep(1000)
      limit -= 1
    }
    Assertions.assertTrue(serverHandler.isSucceeded, "Failed to get User consent on time")
    val params =
      flow.completeSourceOAuth(
        workspaceId,
        definitionId,
        mapOf("code" to serverHandler.paramValue),
        REDIRECT_URL,
        sourceOAuthParameter.configuration,
      )

    log.info("Response from completing OAuth Flow is: {}", params.toString())
    Assertions.assertTrue(params.containsKey("credentials"))
    val credentials =
      Collections.unmodifiableMap(params["credentials"] as Map<String, Any>?)
    Assertions.assertTrue(credentials.containsKey("refresh_token"))
    Assertions.assertTrue(credentials["refresh_token"].toString().length > 0)
    Assertions.assertTrue(credentials.containsKey("access_token"))
    Assertions.assertTrue(credentials["access_token"].toString().length > 0)
    Assertions.assertTrue(credentials.containsKey("token_expiry_date"))
    Assertions.assertTrue(credentials["token_expiry_date"].toString().length > 0)
    Assertions.assertTrue(credentials.containsKey("realm_id"))
    Assertions.assertTrue(credentials["realm_id"].toString().length > 0)
  }

  companion object {
    protected val CREDENTIALS_PATH: Path = Path.of("secrets/quickbooks.json")
    protected const val REDIRECT_URL: String = "http://localhost:3000/auth_flow"
  }
}
