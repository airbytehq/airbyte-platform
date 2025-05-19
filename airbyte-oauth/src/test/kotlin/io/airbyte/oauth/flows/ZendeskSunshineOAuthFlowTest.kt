/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.oauth.flows

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.commons.json.Jsons
import io.airbyte.oauth.BaseOAuthFlow
import io.airbyte.oauth.MoreOAuthParameters
import org.junit.jupiter.api.Test

internal class ZendeskSunshineOAuthFlowTest : BaseOAuthFlowTest() {
  override val oAuthFlow: BaseOAuthFlow
    get() = ZendeskSunshineOAuthFlow(httpClient) { this.constantState }

  @Suppress("ktlint:standard:max-line-length")
  override val expectedConsentUrl: String
    get() = "https://test_subdomain.zendesk.com/oauth/authorizations/new?response_type=code&redirect_uri=https%3A%2F%2Fairbyte.io&client_id=test_client_id&scope=read&state=state"

  override val inputOAuthConfiguration: JsonNode
    get() =
      Jsons.jsonNode(
        java.util.Map.of(
          "subdomain",
          "test_subdomain",
        ),
      )

  override val userInputFromConnectorConfigSpecification: JsonNode
    get() =
      BaseOAuthFlowTest.Companion.getJsonSchema(
        java.util.Map.of<String, Any>(
          "subdomain",
          java.util.Map.of<String, String>("type", "string"),
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
      java.util.Map.of(
        "access_token",
        "access_token_response",
        "client_id",
        MoreOAuthParameters.SECRET_MASK,
        "client_secret",
        MoreOAuthParameters.SECRET_MASK,
      )

  override val completeOAuthOutputSpecification: JsonNode
    get() =
      BaseOAuthFlowTest.Companion.getJsonSchema(
        java.util.Map.of<String, Any>(
          "access_token",
          java.util.Map.of<String, String>("type", "string"),
        ),
      )

  override val expectedFilteredOutput: Map<String, String>
    get() =
      java.util.Map.of(
        "access_token",
        "access_token_response",
        "client_id",
        MoreOAuthParameters.SECRET_MASK,
      )
}
