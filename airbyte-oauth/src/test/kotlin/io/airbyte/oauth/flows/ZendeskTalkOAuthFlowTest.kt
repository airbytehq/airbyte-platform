/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.oauth.flows

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.commons.json.Jsons
import io.airbyte.oauth.BaseOAuthFlow
import io.airbyte.oauth.CLIENT_ID_KEY
import io.airbyte.oauth.CLIENT_SECRET_KEY
import io.airbyte.oauth.MoreOAuthParameters
import org.junit.jupiter.api.Test

internal class ZendeskTalkOAuthFlowTest : BaseOAuthFlowTest() {
  override val oAuthFlow: BaseOAuthFlow
    get() = ZendeskTalkOAuthFlow(httpClient) { this.constantState }

  @Suppress("ktlint:standard:max-line-length")
  override val expectedConsentUrl: String
    get() = "https://test_subdomain.zendesk.com/oauth/authorizations/new?client_id=test_client_id&redirect_uri=https%3A%2F%2Fairbyte.io&response_type=code&scope=read&state=state"

  override val inputOAuthConfiguration: JsonNode
    get() =
      Jsons.jsonNode(
        mapOf(
          "subdomain" to "test_subdomain",
        ),
      )

  override val userInputFromConnectorConfigSpecification: JsonNode
    get() =
      getJsonSchema(
        mapOf(
          "subdomain" to mapOf("type" to "string"),
        ),
      )

  @Test
  override fun testEmptyOutputCompleteSourceOAuth() {
  }

  @Test
  override fun testGetSourceConsentUrlEmptyOAuthSpec() {
  }

  @Test
  override fun testValidateOAuthOutputFailure() {
  }

  @Test
  override fun testCompleteSourceOAuth() {
  }

  @Test
  override fun testEmptyInputCompleteDestinationOAuth() {
  }

  @Test
  override fun testDeprecatedCompleteDestinationOAuth() {
  }

  @Test
  override fun testDeprecatedCompleteSourceOAuth() {
  }

  @Test
  override fun testEmptyOutputCompleteDestinationOAuth() {
  }

  @Test
  override fun testCompleteDestinationOAuth() {
  }

  @Test
  override fun testGetDestinationConsentUrlEmptyOAuthSpec() {
  }

  @Test
  override fun testEmptyInputCompleteSourceOAuth() {
  }

  override val expectedOutput: Map<String, String>
    get() =
      mapOf(
        "access_token" to "access_token_response",
        CLIENT_ID_KEY to MoreOAuthParameters.SECRET_MASK,
        CLIENT_SECRET_KEY to MoreOAuthParameters.SECRET_MASK,
      )

  override val completeOAuthOutputSpecification: JsonNode
    get() =
      getJsonSchema(
        mapOf(
          "access_token" to mapOf("type" to "string"),
        ),
      )

  override val expectedFilteredOutput: Map<String, String>
    get() =
      mapOf(
        "access_token" to "access_token_response",
        CLIENT_ID_KEY to MoreOAuthParameters.SECRET_MASK,
      )
}
