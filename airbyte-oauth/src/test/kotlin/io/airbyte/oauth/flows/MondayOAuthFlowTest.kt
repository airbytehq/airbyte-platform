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

internal class MondayOAuthFlowTest : BaseOAuthFlowTest() {
  override val oAuthFlow: BaseOAuthFlow
    get() = MondayOAuthFlow(httpClient) { this.constantState }

  @Suppress("ktlint:standard:max-line-length")
  override val expectedConsentUrl: String
    get() = "https://auth.monday.com/oauth2/authorize?client_id=test_client_id&redirect_uri=https%3A%2F%2Fairbyte.io&scope=me%3Aread+boards%3Aread+workspaces%3Aread+users%3Aread+account%3Aread+updates%3Aread+assets%3Aread+tags%3Aread+teams%3Aread&state=state&subdomain=test_subdomain"

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
