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
import java.util.Optional
import java.util.UUID

private val log = KotlinLogging.logger {}

@Tag("oauth")
class PipeDriveOAuthFlowIntegrationTest : OAuthFlowIntegrationTest() {
  override fun getCredentialsPath(): Path = Path.of("secrets/pipedrive.json")

  override fun getRedirectUrl(): String = "http://localhost:3000/auth_flow"

  override fun getServerListeningPort(): Int = 3000

  override fun getFlowImplementation(
    oauthService: OAuthService,
    httpClient: HttpClient,
  ): OAuthFlowImplementation = PipeDriveOAuthFlow(httpClient)

  @Test
  @Throws(
    InterruptedException::class,
    ConfigNotFoundException::class,
    IOException::class,
    JsonValidationException::class,
  )
  fun testFullPipeDriveOAuthFlow() {
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
            java.util.Map.of(
              "authorization",
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
        getRedirectUrl(),
        Jsons.emptyObject(),
        null,
        sourceOAuthParameter.configuration,
      )
    log.info("Waiting for user consent at: {}", url)
    waitForResponse(20)
    Assertions.assertTrue(serverHandler.isSucceeded, "Failed to get User consent on time")
    val params =
      flow.completeSourceOAuth(
        workspaceId,
        definitionId,
        java.util.Map.of("code", serverHandler.paramValue),
        getRedirectUrl(),
        sourceOAuthParameter.configuration,
      )
    log.info("Response from completing OAuth Flow is: {}", params.toString())
    Assertions.assertTrue(params.containsKey("authorization"))
    val creds = params["authorization"] as Map<String, String>?
    Assertions.assertTrue(creds!!["refresh_token"].toString().length > 0)
  }
}
