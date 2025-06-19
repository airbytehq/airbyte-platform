/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.oauth.flows

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.JsonNodeFactory
import io.airbyte.commons.json.Jsons
import io.airbyte.oauth.BaseOAuthFlow
import io.airbyte.oauth.MoreOAuthParameters
import io.airbyte.oauth.declarative.DeclarativeOAuthFlow
import org.junit.jupiter.api.Test
import java.time.Clock
import java.time.Instant
import java.time.ZoneId
import java.util.List

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

      return String.format(
        "https://some.domain.com/oauth2/authorize?my_client_id_key=%s&callback_uri=%s&scope=%s&my_state_key=%s&subdomain=%s&code_challenge=%s",
        expectedClientId,
        expectedRedirectUri,
        expectedScope,
        expectedState,
        expectedSubdomain,
        expectedCodeChallenge,
      )
    }

  override val inputOAuthConfiguration: JsonNode
    get() =
      Jsons.jsonNode(
        java.util.Map.ofEntries( // the `subdomain` is a custom property passed by the user (test)
          java.util.Map.entry("subdomain", "test_subdomain"), // these are the part of the spec,
          // not all spec properties are provided, since they provide an override to the default values.
          java.util.Map.entry(
            "consent_url",
            "https://some.domain.com/oauth2/authorize?{{ client_id_key }}={{ client_id_value }}&{{ redirect_uri_key }}={{ redirect_uri_value | urlencode }}&{{ scope_param }}&{{ state_key }}={{ state_value }}&subdomain={{ subdomain }}&code_challenge={{ state_value | codechallengeS256 }}",
          ),
          java.util.Map.entry("scope", "test_scope_1 test_scope_2 test_scope_3"),
          java.util.Map.entry("access_token_url", "https://some.domain.com/oauth2/token/"),
          java.util.Map.entry(
            "access_token_headers",
            Jsons.jsonNode(
              java.util.Map.of(
                "test_header",
                "test_value",
              ),
            ),
          ), // Map.entry("state", Jsons.jsonNode(Map.of("min", 43, "max", 128))),
          java.util.Map.entry("state_key", "my_state_key"),
          java.util.Map.entry("client_id_key", "my_client_id_key"),
          java.util.Map.entry("client_secret_key", "my_client_secret_key"),
          java.util.Map.entry("auth_code_key", "my_auth_code_key"),
          java.util.Map.entry("redirect_uri_key", "callback_uri"),
          java.util.Map.entry(
            "extract_output",
            Jsons.jsonNode(
              List.of(
                ACCESS_TOKEN,
                REFRESH_TOKEN,
                EXPIRES_IN,
              ),
            ),
          ),
        ),
      )

  override val userInputFromConnectorConfigSpecification: JsonNode
    get() =
      BaseOAuthFlowTest.Companion.getJsonSchema(
        java.util.Map.ofEntries<String, Any>( // the `subdomain` is a custom property passed by the user (test)
          java.util.Map.entry<String, JsonNode>(
            "subdomain",
            STRING_TYPE,
          ), // these are the part of the spec
          java.util.Map.entry<String, JsonNode>(
            "consent_url",
            STRING_TYPE,
          ),
          java.util.Map.entry<String, JsonNode>("scope", STRING_TYPE),
          java.util.Map.entry<String, JsonNode>("state", OBJECT_TYPE),
          java.util.Map.entry<String, JsonNode>(
            "access_token_url",
            STRING_TYPE,
          ),
          java.util.Map.entry<String, JsonNode>(
            "extract_output",
            ARRAY_TYPE,
          ),
          java.util.Map.entry<String, JsonNode>(
            "access_token_headers",
            OBJECT_TYPE,
          ),
          java.util.Map.entry<String, JsonNode>(
            "client_id_key",
            STRING_TYPE,
          ),
          java.util.Map.entry<String, JsonNode>(
            "client_secret_key",
            STRING_TYPE,
          ),
          java.util.Map.entry<String, JsonNode>(
            "scope_key",
            STRING_TYPE,
          ),
          java.util.Map.entry<String, JsonNode>(
            "state_key",
            STRING_TYPE,
          ),
          java.util.Map.entry<String, JsonNode>(
            "auth_code_key",
            STRING_TYPE,
          ),
          java.util.Map.entry<String, JsonNode>(
            "redirect_uri_key",
            STRING_TYPE,
          ),
        ),
      )

  override val expectedOutput: Map<String, String>
    get() =
      java.util.Map.of(
        EXPIRES_IN,
        "7200",
        ACCESS_TOKEN,
        "access_token_response",
        REFRESH_TOKEN,
        "refresh_token_response",
        CLIENT_ID,
        MoreOAuthParameters.SECRET_MASK,
        CLIENT_SECRET,
        MoreOAuthParameters.SECRET_MASK,
      )

  override val completeOAuthOutputSpecification: JsonNode
    get() =
      BaseOAuthFlowTest.Companion.getJsonSchema(
        java.util.Map.of<String, Any>(
          TOKEN_EXPIRY_DATE,
          STRING_TYPE,
          ACCESS_TOKEN,
          STRING_TYPE,
          REFRESH_TOKEN,
          STRING_TYPE,
          CLIENT_ID,
          STRING_TYPE,
        ),
      )

  override val expectedFilteredOutput: Map<String, String>
    get() =
      java.util.Map.of(
        TOKEN_EXPIRY_DATE,
        "2024-01-01T02:00:00Z",
        ACCESS_TOKEN,
        "access_token_response",
        REFRESH_TOKEN,
        "refresh_token_response",
        CLIENT_ID,
        MoreOAuthParameters.SECRET_MASK,
      )

  override val queryParams: Map<String, Any>
    get() =
      java.util.Map.of<String, Any>( // keys override
        "my_auth_code_key",
        "test_code",
        "my_state_key",
        constantState, // default test values
        "code",
        "test_code",
        "state",
        constantState,
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
    private const val REFRESH_TOKEN = "refresh_token"
    private const val CLIENT_ID = "client_id"
    private const val CLIENT_SECRET = "client_secret"
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
