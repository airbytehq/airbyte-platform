/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.oauth.flows

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.JsonNodeFactory
import io.airbyte.commons.json.Jsons
import io.airbyte.oauth.AUTH_CODE_KEY
import io.airbyte.oauth.BaseOAuthFlow
import io.airbyte.oauth.CLIENT_ID_KEY
import io.airbyte.oauth.CLIENT_SECRET_KEY
import io.airbyte.oauth.MoreOAuthParameters
import io.airbyte.oauth.REFRESH_TOKEN_KEY
import io.airbyte.oauth.SCOPE_KEY
import io.airbyte.oauth.declarative.DeclarativeOAuthFlow
import org.junit.jupiter.api.Test
import java.time.Clock
import java.time.Instant
import java.time.ZoneId

internal class DeclarativeOAuthFlowTest : BaseOAuthFlowTest() {
  override val oAuthFlow: BaseOAuthFlow
    get() {
      val oauthFlow =
        DeclarativeOAuthFlow(httpClient) { this.constantState }
      oauthFlow.specHandler.setClock(Clock.fixed(Instant.parse("2024-01-01T00:00:00Z"), ZoneId.of("UTC")))
      return oauthFlow
    }

  @Suppress("ktlint:standard:max-line-length")
  override val expectedConsentUrl: String
    get() {
      val expectedClientId = "test_client_id"
      val expectedRedirectUri = "https%3A%2F%2Fairbyte.io"
      val expectedScope = "test_scope_1+test_scope_2+test_scope_3"
      val expectedState = "state"
      val expectedSubdomain = "test_subdomain"
      // Base64 from (String) of `state`, using `SHA-256`.
      val expectedCodeChallenge = "S6aXNcpTdl7WpwnttWxuoja3GTo7KaazkMNG8PQ0Dk4="

      return "https://some.domain.com/oauth2/authorize?my_client_id_key=$expectedClientId&callback_uri=$expectedRedirectUri&scope=$expectedScope&my_state_key=$expectedState&subdomain=$expectedSubdomain&code_challenge=$expectedCodeChallenge"
    }

  override val inputOAuthConfiguration: JsonNode
    get() =
      Jsons.jsonNode(
        mapOf(
          "subdomain" to "test_subdomain", // these are the part of the spec,
          "consent_url" to
            "https://some.domain.com/oauth2/authorize?{{ client_id_key }}={{ client_id_value }}&{{ redirect_uri_key }}={{ redirect_uri_value | urlencode }}&{{ scope_param }}&{{ state_key }}={{ state_value }}&subdomain={{ subdomain }}&code_challenge={{ state_value | codechallengeS256 }}",
          SCOPE_KEY to "test_scope_1 test_scope_2 test_scope_3",
          "access_token_url" to "https://some.domain.com/oauth2/token/",
          "access_token_headers" to Jsons.jsonNode(mapOf("test_header" to "test_value")),
          "state_key" to "my_state_key",
          "client_id_key" to "my_client_id_key",
          "client_secret_key" to "my_client_secret_key",
          "auth_code_key" to "my_auth_code_key",
          "redirect_uri_key" to "callback_uri",
          "extract_output" to Jsons.jsonNode(listOf(ACCESS_TOKEN, REFRESH_TOKEN_KEY, EXPIRES_IN)),
        ),
      )

  override val userInputFromConnectorConfigSpecification: JsonNode
    get() =
      getJsonSchema(
        mapOf(
          "subdomain" to STRING_TYPE,
          "consent_url" to STRING_TYPE,
          SCOPE_KEY to STRING_TYPE,
          "state" to OBJECT_TYPE,
          "access_token_url" to STRING_TYPE,
          "extract_output" to ARRAY_TYPE,
          "access_token_headers" to OBJECT_TYPE,
          "client_id_key" to STRING_TYPE,
          "client_secret_key" to STRING_TYPE,
          "scope_key" to STRING_TYPE,
          "state_key" to STRING_TYPE,
          "auth_code_key" to STRING_TYPE,
          "redirect_uri_key" to STRING_TYPE,
        ),
      )

  override val expectedOutput: Map<String, String>
    get() =
      mapOf(
        EXPIRES_IN to "7200",
        ACCESS_TOKEN to "access_token_response",
        REFRESH_TOKEN_KEY to "refresh_token_response",
        CLIENT_ID_KEY to MoreOAuthParameters.SECRET_MASK,
        CLIENT_SECRET_KEY to MoreOAuthParameters.SECRET_MASK,
      )

  override val completeOAuthOutputSpecification: JsonNode
    get() =
      getJsonSchema(
        mapOf(
          TOKEN_EXPIRY_DATE to STRING_TYPE,
          ACCESS_TOKEN to STRING_TYPE,
          REFRESH_TOKEN_KEY to STRING_TYPE,
          CLIENT_ID_KEY to STRING_TYPE,
        ),
      )

  override val expectedFilteredOutput: Map<String, String>
    get() =
      mapOf(
        TOKEN_EXPIRY_DATE to "2024-01-01T02:00:00Z",
        ACCESS_TOKEN to "access_token_response",
        REFRESH_TOKEN_KEY to "refresh_token_response",
        CLIENT_ID_KEY to MoreOAuthParameters.SECRET_MASK,
      )

  override val queryParams: Map<String, Any>
    get() =
      mapOf( // keys override
        "my_auth_code_key" to "test_code",
        "my_state_key" to constantState, // default test values
        AUTH_CODE_KEY to "test_code",
        "state" to constantState,
      )

  @Test
  override fun testGetSourceConsentUrlEmptyOAuthSpec() {
  }

  @Test
  override fun testGetDestinationConsentUrlEmptyOAuthSpec() {
  }

  @Test
  override fun testDeprecatedCompleteDestinationOAuth() {
  }

  @Test
  override fun testDeprecatedCompleteSourceOAuth() {
  }

  @Test
  override fun testEmptyInputCompleteSourceOAuth() {
  }

  @Test
  override fun testEmptyInputCompleteDestinationOAuth() {
  }

  companion object {
    private const val ACCESS_TOKEN = "access_token"
    private const val EXPIRES_IN = "expires_in"
    private const val TOKEN_EXPIRY_DATE = "token_expiry_date"

    // JsonSchema Types
    private val STRING_TYPE: JsonNode = JsonNodeFactory.instance.objectNode().put("type", "string")
    private val ARRAY_TYPE: JsonNode =
      JsonNodeFactory.instance
        .objectNode()
        .put("type", "array")
        .set("items", STRING_TYPE)
    private val OBJECT_TYPE: JsonNode = JsonNodeFactory.instance.objectNode().put("type", "object")
  }
}
