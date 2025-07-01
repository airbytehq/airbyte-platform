/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.oauth.flows

import com.fasterxml.jackson.databind.JsonNode
import com.google.common.collect.ImmutableMap
import io.airbyte.commons.json.Jsons
import io.airbyte.oauth.BaseOAuthFlow
import io.airbyte.oauth.MoreOAuthParameters
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test

internal class SurveymonkeyOAuthFlowTest : BaseOAuthFlowTest() {
  override val oAuthFlow: BaseOAuthFlow
    get() = SurveymonkeyOAuthFlow(httpClient) { this.constantState }

  @Suppress("ktlint:standard:max-line-length")
  override val expectedConsentUrl: String
    get() = "https://api.surveymonkey.com/oauth/authorize?client_id=test_client_id&redirect_uri=https%3A%2F%2Fairbyte.io&response_type=code&state=state"

  override val expectedOutputPath: List<String?>
    get() = listOf<String>()

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

  override val inputOAuthConfiguration: JsonNode
    get() =
      Jsons.jsonNode(
        ImmutableMap
          .builder<Any, Any>()
          .put("origin", "USA")
          .build(),
      )

  override val userInputFromConnectorConfigSpecification: JsonNode
    get() = BaseOAuthFlowTest.Companion.getJsonSchema(java.util.Map.of<String, Any>("origin", "USA"))

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

  @Test
  fun testGetAccessTokenUrl() {
    val oauthFlow = oAuthFlow as SurveymonkeyOAuthFlow
    val expectedAccessTokenUrl = "https://api.surveymonkey.com/oauth/token"

    val accessTokenUrl = oauthFlow.getAccessTokenUrl(inputOAuthConfiguration)
    Assertions.assertEquals(accessTokenUrl, expectedAccessTokenUrl)
  }

  @Test
  override fun testGetDestinationConsentUrlEmptyOAuthSpec() {
  }

  @Test
  override fun testGetDestinationConsentUrl() {
  }

  @Test
  override fun testGetSourceConsentUrlEmptyOAuthSpec() {
  }

  @Test
  override fun testGetSourceConsentUrl() {
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
  override fun testEmptyInputCompleteSourceOAuth() {
  }

  companion object {
    const val STRING: String = "string"
    const val TYPE: String = "type"
  }
}
