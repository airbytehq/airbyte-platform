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
import io.airbyte.oauth.flows.facebook.FacebookMarketingOAuthFlow
import io.airbyte.validation.json.JsonValidationException
import io.github.oshai.kotlinlogging.KotlinLogging
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.mockito.ArgumentMatchers
import org.mockito.Mockito
import java.io.IOException
import java.net.http.HttpClient
import java.nio.file.Files
import java.nio.file.Path
import java.util.Map
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
  @Throws(IOException::class)
  override fun setup() {
    super.setup()
  }

  override fun getServerListeningPort(): Int = 9000

  @Test
  @Throws(
    InterruptedException::class,
    ConfigNotFoundException::class,
    IOException::class,
    JsonValidationException::class,
  )
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
            ImmutableMap
              .builder<Any, Any>()
              .put("client_id", credentialsJson["client_id"].asText())
              .put("client_secret", credentialsJson["client_secret"].asText())
              .build(),
          ),
        )
    Mockito.`when`(oauthService.getSourceOAuthParameterOptional(ArgumentMatchers.any(), ArgumentMatchers.any())).thenReturn(Optional.of(param))
    val url =
      flow.getSourceConsentUrl(workspaceId, definitionId, REDIRECT_URL, Jsons.emptyObject(), null, param.configuration)
    log.info("Waiting for user consent at: {}", url)
    waitForResponse(20)
    Assertions.assertTrue(serverHandler.isSucceeded, "Failed to get User consent on time")
    val params =
      flow.completeSourceOAuth(
        workspaceId,
        definitionId,
        Map.of("code", serverHandler.paramValue),
        REDIRECT_URL,
        param.configuration,
      )
    log.info("Response from completing OAuth Flow is: {}", params.toString())
    Assertions.assertTrue(params.containsKey("access_token"))
    Assertions.assertTrue(params["access_token"].toString().length > 0)
  }

  companion object {
    protected val CREDENTIALS_PATH: Path = Path.of("secrets/facebook_marketing.json")
    protected const val REDIRECT_URL: String = "http://localhost:9000/auth_flow"
  }
}
