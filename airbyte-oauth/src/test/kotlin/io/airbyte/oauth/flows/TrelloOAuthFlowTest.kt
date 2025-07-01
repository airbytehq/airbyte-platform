/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.oauth.flows

import com.google.api.client.http.HttpTransport
import com.google.api.client.http.LowLevelHttpRequest
import com.google.api.client.http.LowLevelHttpResponse
import com.google.api.client.testing.http.MockHttpTransport
import com.google.api.client.testing.http.MockLowLevelHttpRequest
import com.google.api.client.testing.http.MockLowLevelHttpResponse
import com.google.common.collect.ImmutableMap
import io.airbyte.commons.json.Jsons
import io.airbyte.config.SourceOAuthParameter
import io.airbyte.config.persistence.ConfigNotFoundException
import io.airbyte.data.services.OAuthService
import io.airbyte.oauth.MoreOAuthParameters
import io.airbyte.validation.json.JsonValidationException
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.ArgumentMatchers
import org.mockito.Mockito
import java.io.IOException
import java.util.Map
import java.util.Optional
import java.util.UUID

internal class TrelloOAuthFlowTest {
  private var workspaceId: UUID? = null
  private var definitionId: UUID? = null
  private var trelloOAuthFlow: TrelloOAuthFlow? = null
  private var transport: HttpTransport? = null
  private var sourceOAuthParameter: SourceOAuthParameter? = null
  private var oauthService: OAuthService? = null

  @BeforeEach
  @Throws(IOException::class, JsonValidationException::class)
  fun setup() {
    workspaceId = UUID.randomUUID()
    definitionId = UUID.randomUUID()

    transport =
      object : MockHttpTransport() {
        @Throws(IOException::class)
        override fun buildRequest(
          method: String,
          url: String,
        ): LowLevelHttpRequest {
          return object : MockLowLevelHttpRequest() {
            @Throws(IOException::class)
            override fun execute(): LowLevelHttpResponse {
              val response = MockLowLevelHttpResponse()
              response.setStatusCode(200)
              response.setContentType("application/x-www-form-urlencoded")
              response.setContent("oauth_token=test_token&oauth_token_secret=test_secret&oauth_callback_confirmed=true")
              return response
            }
          }
        }
      }
    oauthService = Mockito.mock(OAuthService::class.java)
    sourceOAuthParameter =
      SourceOAuthParameter()
        .withSourceDefinitionId(definitionId)
        .withConfiguration(
          Jsons.jsonNode(
            ImmutableMap
              .builder<Any, Any>()
              .put("client_id", "test_client_id")
              .put("client_secret", "test_client_secret")
              .build(),
          ),
        )
    Mockito
      .`when`(oauthService!!.getSourceOAuthParameterOptional(ArgumentMatchers.any(), ArgumentMatchers.any()))
      .thenReturn(Optional.of(sourceOAuthParameter!!))
    trelloOAuthFlow = TrelloOAuthFlow(transport!!)
  }

  @Test
  @Throws(IOException::class, InterruptedException::class, ConfigNotFoundException::class)
  fun testGetSourceConsentUrl() {
    val consentUrl =
      trelloOAuthFlow!!.getSourceConsentUrl(
        workspaceId!!,
        definitionId,
        REDIRECT_URL,
        Jsons.emptyObject(),
        null,
        sourceOAuthParameter!!.configuration,
      )
    Assertions.assertEquals("https://trello.com/1/OAuthAuthorizeToken?oauth_token=test_token&expiration=never", consentUrl)
  }

  @Test
  @Throws(IOException::class, InterruptedException::class, ConfigNotFoundException::class)
  fun testCompleteSourceAuth() {
    val expectedParams =
      Map.of(
        "key",
        "test_client_id",
        "token",
        "test_token",
        "client_id",
        MoreOAuthParameters.SECRET_MASK,
        "client_secret",
        MoreOAuthParameters.SECRET_MASK,
      )
    val queryParams = Map.of<String, Any>("oauth_token", "token", "oauth_verifier", "verifier")
    val actualParams =
      trelloOAuthFlow!!.completeSourceOAuth(workspaceId!!, definitionId, queryParams, REDIRECT_URL, sourceOAuthParameter!!.configuration)
    Assertions.assertEquals(actualParams, expectedParams)
    Assertions.assertEquals(
      expectedParams.size,
      actualParams.size,
      String.format("Expected %s values but got %s", expectedParams.size, actualParams),
    )
    expectedParams.forEach { (key: String?, value: String?) ->
      Assertions.assertEquals(
        value,
        actualParams[key],
      )
    }
  }

  companion object {
    private const val REDIRECT_URL = "https://airbyte.io"
  }
}
