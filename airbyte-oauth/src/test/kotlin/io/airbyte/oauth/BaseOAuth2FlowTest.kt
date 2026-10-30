/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.oauth

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.commons.json.Jsons
import io.airbyte.protocol.models.v0.OAuthConfigSpecification
import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.io.IOException
import java.net.http.HttpClient
import java.net.http.HttpResponse
import java.util.UUID

/**
 * Tests the token exchange behavior shared by every OAuth2 connector, exercised through a minimal
 * concrete flow so the assertions cover [BaseOAuth2Flow] itself rather than any one connector.
 */
internal class BaseOAuth2FlowTest {
  private lateinit var httpClient: HttpClient
  private lateinit var oauthFlow: BaseOAuth2Flow

  @BeforeEach
  fun setup() {
    httpClient = mockk()
    oauthFlow = TestingOAuth2Flow(httpClient)
  }

  @Test
  fun testCompleteOAuthSurfacesHttpErrorStatusAndBody() {
    val errorBody =
      """{"error":"invalid_grant","error_description":"AADSTS54005: OAuth2 Authorization code was already redeemed"}"""
    mockHttpResponse(400, errorBody)

    val exception = assertThrows(IOException::class.java) { completeSourceOAuth() }
    val message = exception.message!!

    assertTrue(message.contains("400"), "Expected the HTTP status in the error message but got: $message")
    assertTrue(message.contains(errorBody), "Expected the response body in the error message but got: $message")
    assertTrue(message.contains(ACCESS_TOKEN_URL), "Expected the token endpoint in the error message but got: $message")
    assertFalse(
      message.contains("Missing '$REFRESH_TOKEN_KEY'"),
      "The provider error should surface instead of a missing-refresh_token error, but got: $message",
    )
  }

  @Test
  fun testCompleteOAuthSurfacesServerErrorWithNonJsonBody() {
    mockHttpResponse(500, "Internal Server Error")

    val exception = assertThrows(IOException::class.java) { completeSourceOAuth() }
    val message = exception.message!!

    assertTrue(message.contains("500"), "Expected the HTTP status in the error message but got: $message")
    assertTrue(message.contains("Internal Server Error"), "Expected the raw body in the error message but got: $message")
  }

  @Test
  fun testCompleteOAuthSucceedsOnSuccessfulResponse() {
    mockHttpResponse(200, Jsons.serialize(mapOf(REFRESH_TOKEN_KEY to REFRESH_TOKEN_RESPONSE)))

    val output = completeSourceOAuth()

    assertEquals(REFRESH_TOKEN_RESPONSE, output[REFRESH_TOKEN_KEY])
  }

  private fun mockHttpResponse(
    statusCode: Int,
    body: String,
  ) {
    val response = mockk<HttpResponse<String>>()
    every { response.statusCode() } returns statusCode
    every { response.body() } returns body
    every { httpClient.send(any(), any<HttpResponse.BodyHandler<String>>()) } returns response
  }

  private fun completeSourceOAuth(): Map<String, Any> =
    oauthFlow.completeSourceOAuth(
      UUID.randomUUID(),
      UUID.randomUUID(),
      mapOf(AUTH_CODE_KEY to "test_code"),
      REDIRECT_URL,
      Jsons.emptyObject(),
      oauthConfigSpecification,
      oAuthParamConfig,
    )

  /**
   * A minimal OAuth2 flow that keeps the base class behavior intact, so these tests observe the
   * shared token exchange rather than a connector specific override.
   */
  private class TestingOAuth2Flow(
    httpClient: HttpClient,
  ) : BaseOAuth2Flow(httpClient) {
    override fun formatConsentUrl(
      definitionId: UUID?,
      clientId: String,
      redirectUrl: String,
      inputOAuthConfiguration: JsonNode,
    ): String = "https://airbyte.io/consent"

    override fun getAccessTokenUrl(inputOAuthConfiguration: JsonNode): String = ACCESS_TOKEN_URL
  }

  companion object {
    private const val ACCESS_TOKEN_URL = "https://login.microsoftonline.com/common/oauth2/v2.0/token"
    private const val REDIRECT_URL = "https://airbyte.io"
    private const val REFRESH_TOKEN_RESPONSE = "refresh_token_response"
    private const val TYPE = "type"

    private val oAuthParamConfig: JsonNode =
      Jsons.jsonNode(
        mapOf(
          CLIENT_ID_KEY to "test_client_id",
          CLIENT_SECRET_KEY to "test_client_secret",
        ),
      )

    private val oauthConfigSpecification: OAuthConfigSpecification =
      OAuthConfigSpecification()
        .withCompleteOauthOutputSpecification(
          getJsonSchema(mapOf(REFRESH_TOKEN_KEY to mapOf(TYPE to "string"))),
        ).withCompleteOauthServerOutputSpecification(
          getJsonSchema(mapOf(CLIENT_ID_KEY to mapOf(TYPE to "string"))),
        )

    private fun getJsonSchema(properties: Map<String, Any>): JsonNode =
      Jsons.jsonNode(
        mapOf(
          TYPE to "object",
          "additionalProperties" to "false",
          "properties" to properties,
        ),
      )
  }
}
