/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.oauth.flows

import com.fasterxml.jackson.databind.JsonNode
import com.google.common.collect.ImmutableMap
import io.airbyte.commons.json.Jsons
import io.airbyte.oauth.BaseOAuthFlow
import io.airbyte.oauth.MoreOAuthParameters
import org.junit.jupiter.api.Test

internal class SnowflakeOAuthFlowTest : BaseOAuthFlowTest() {
  override val oAuthFlow: BaseOAuthFlow
    get() = SourceSnowflakeOAuthFlow(httpClient) { this.constantState }

  @Suppress("ktlint:standard:max-line-length")
  override val expectedConsentUrl: String
    get() = "https://account.aws.snowflakecomputing.com/oauth/authorize?client_id=test_client_id&redirect_uri=https%3A%2F%2Fairbyte.io&response_type=code&state=state&scope=session%3Arole%3Asome_role"

  override val expectedOutput: Map<String, String>
    get() =
      java.util.Map.of(
        "access_token",
        "access_token_response",
        "refresh_token",
        "refresh_token_response",
        "username",
        "username",
      )

  override val completeOAuthOutputSpecification: JsonNode
    get() =
      BaseOAuthFlowTest.Companion.getJsonSchema(
        java.util.Map.of<String, Any>(
          "access_token",
          java.util.Map.of<String, String>(TYPE, STRING),
          "refresh_token",
          java.util.Map.of<String, String>(TYPE, STRING),
        ),
      )

  override val expectedFilteredOutput: Map<String, String>
    get() =
      java.util.Map.of(
        "access_token",
        "access_token_response",
        "refresh_token",
        "refresh_token_response",
        "client_id",
        MoreOAuthParameters.SECRET_MASK,
      )

  override val oAuthParamConfig: JsonNode?
    get() =
      Jsons.jsonNode(
        ImmutableMap
          .builder<Any, Any>()
          .put("client_id", "test_client_id")
          .put("client_secret", "test_client_secret")
          .build(),
      )

  override val inputOAuthConfiguration: JsonNode
    get() =
      Jsons.jsonNode(
        ImmutableMap
          .builder<Any, Any>()
          .put("host", "account.aws.snowflakecomputing.com")
          .put("role", "some_role")
          .build(),
      )

  override val userInputFromConnectorConfigSpecification: JsonNode
    get() =
      BaseOAuthFlowTest.Companion.getJsonSchema(
        java.util.Map.of<String, Any>(
          "host",
          java.util.Map.of<String, String>(TYPE, STRING),
          "role",
          java.util.Map.of<String, String>(TYPE, STRING),
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
