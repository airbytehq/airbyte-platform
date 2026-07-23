/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.oauth.flows

import com.google.api.client.http.HttpTransport
import com.google.api.client.http.LowLevelHttpRequest
import com.google.api.client.http.LowLevelHttpResponse
import com.google.api.client.testing.http.MockHttpTransport
import com.google.api.client.testing.http.MockLowLevelHttpRequest
import com.google.api.client.testing.http.MockLowLevelHttpResponse
import io.airbyte.commons.json.Jsons
import io.airbyte.config.SourceOAuthParameter
import io.airbyte.data.services.OAuthService
import io.airbyte.oauth.CLIENT_ID_KEY
import io.airbyte.oauth.CLIENT_SECRET_KEY
import io.airbyte.oauth.MoreOAuthParameters
import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
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
  fun setup() {
    workspaceId = UUID.randomUUID()
    definitionId = UUID.randomUUID()

    transport =
      object : MockHttpTransport() {
        override fun buildRequest(
          method: String,
          url: String,
        ): LowLevelHttpRequest {
          return object : MockLowLevelHttpRequest() {
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
    oauthService = mockk()
    sourceOAuthParameter =
      SourceOAuthParameter()
        .withSourceDefinitionId(definitionId)
        .withConfiguration(
          Jsons.jsonNode(
            mapOf(
              CLIENT_ID_KEY to "test_client_id",
              CLIENT_SECRET_KEY to "test_client_secret",
            ),
          ),
        )
    every {
      oauthService!!.getSourceOAuthParameterOptional(any(), any())
    } returns Optional.of(sourceOAuthParameter!!)
    trelloOAuthFlow = TrelloOAuthFlow(transport!!)
  }

  @Test
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
    assertEquals("https://trello.com/1/OAuthAuthorizeToken?oauth_token=test_token&expiration=never", consentUrl)
  }

  @Test
  fun testCompleteSourceAuth() {
    val expectedParams =
      mapOf(
        "key" to "test_client_id",
        "token" to "test_token",
        CLIENT_ID_KEY to MoreOAuthParameters.SECRET_MASK,
        CLIENT_SECRET_KEY to MoreOAuthParameters.SECRET_MASK,
      )
    val queryParams = mapOf("oauth_token" to "token", "oauth_verifier" to "verifier")
    val actualParams =
      trelloOAuthFlow!!.completeSourceOAuth(workspaceId!!, definitionId, queryParams, REDIRECT_URL, sourceOAuthParameter!!.configuration)
    assertEquals(actualParams, expectedParams)
    assertEquals(
      expectedParams.size,
      actualParams.size,
      "Expected ${expectedParams.size} values but got $actualParams",
    )
    expectedParams.forEach { (key: String?, value: String?) ->
      assertEquals(
        value,
        actualParams[key],
      )
    }
  }

  companion object {
    private const val REDIRECT_URL = "https://airbyte.io"
  }
}
