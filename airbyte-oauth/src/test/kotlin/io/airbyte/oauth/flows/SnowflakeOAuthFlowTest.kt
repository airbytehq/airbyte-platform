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
import io.airbyte.oauth.REFRESH_TOKEN_KEY
import org.junit.jupiter.api.Test

internal class SnowflakeOAuthFlowTest : BaseOAuthFlowTest() {
  override val oAuthFlow: BaseOAuthFlow
    get() = SourceSnowflakeOAuthFlow(httpClient) { this.constantState }

  @Suppress("ktlint:standard:max-line-length")
  override val expectedConsentUrl: String
    get() = "https://account.aws.snowflakecomputing.com/oauth/authorize?client_id=test_client_id&redirect_uri=https%3A%2F%2Fairbyte.io&response_type=code&state=state&scope=session%3Arole%3Asome_role"

  override val expectedOutput: Map<String, String>
    get() =
      mapOf(
        "access_token" to "access_token_response",
        REFRESH_TOKEN_KEY to "refresh_token_response",
        "username" to "username",
      )

  override val completeOAuthOutputSpecification: JsonNode
    get() =
      getJsonSchema(
        mapOf(
          "access_token" to mapOf(TYPE to STRING),
          REFRESH_TOKEN_KEY to mapOf(TYPE to STRING),
        ),
      )

  override val expectedFilteredOutput: Map<String, String>
    get() =
      mapOf(
        "access_token" to "access_token_response",
        REFRESH_TOKEN_KEY to "refresh_token_response",
        CLIENT_ID_KEY to MoreOAuthParameters.SECRET_MASK,
      )

  override val oAuthParamConfig: JsonNode?
    get() =
      Jsons.jsonNode(
        mapOf(
          CLIENT_ID_KEY to "test_client_id",
          CLIENT_SECRET_KEY to "test_client_secret",
        ),
      )

  override val inputOAuthConfiguration: JsonNode
    get() =
      Jsons.jsonNode(
        mapOf(
          "host" to "account.aws.snowflakecomputing.com",
          "role" to "some_role",
        ),
      )

  override val userInputFromConnectorConfigSpecification: JsonNode
    get() =
      getJsonSchema(
        mapOf(
          "host" to mapOf(TYPE to STRING),
          "role" to mapOf(TYPE to STRING),
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

  companion object {
    const val STRING: String = "string"
    const val TYPE: String = "type"
  }
}
