/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.oauth.flows

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.commons.json.Jsons
import io.airbyte.oauth.BaseOAuthFlow
import io.airbyte.oauth.CLIENT_ID_KEY
import io.airbyte.oauth.CLIENT_SECRET_KEY
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
      mapOf(
        "access_token" to "access_token_response",
        CLIENT_ID_KEY to MoreOAuthParameters.SECRET_MASK,
        CLIENT_SECRET_KEY to MoreOAuthParameters.SECRET_MASK,
      )

  override val inputOAuthConfiguration: JsonNode
    get() =
      Jsons.jsonNode(
        mapOf(
          "origin" to "USA",
        ),
      )

  override val userInputFromConnectorConfigSpecification: JsonNode
    get() = getJsonSchema(mapOf("origin" to "USA"))

  override val completeOAuthOutputSpecification: JsonNode
    get() =
      getJsonSchema(
        mapOf(
          "access_token" to mapOf(TYPE to STRING),
        ),
      )

  override val expectedFilteredOutput: Map<String, String>
    get() =
      mapOf(
        "access_token" to "access_token_response",
        CLIENT_ID_KEY to MoreOAuthParameters.SECRET_MASK,
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
