/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.oauth.flows

import com.fasterxml.jackson.databind.JsonNode
import com.google.common.collect.ImmutableMap
import io.airbyte.commons.json.Jsons
import io.airbyte.oauth.BaseOAuthFlow
import io.airbyte.protocol.models.v0.OAuthConfigSpecification
import org.junit.jupiter.api.Test

internal class TikTokMarketingOAuthFlowTest : BaseOAuthFlowTest() {
  override val oAuthFlow: BaseOAuthFlow
    get() = TikTokMarketingOAuthFlow(httpClient) { this.constantState }

  @Suppress("ktlint:standard:max-line-length")
  override val expectedConsentUrl: String
    get() = (
      "https://ads.tiktok.com/marketing_api/auth?app_id=app_id" +
        "&redirect_uri=https%3A%2F%2Fairbyte.io" +
        "&state=state"
    )

  override val oAuthParamConfig: JsonNode?
    get() =
      Jsons.jsonNode(
        ImmutableMap
          .builder<Any, Any>()
          .put("app_id", "app_id")
          .put("secret", "secret")
          .build(),
      )

  override val oAuthConfigSpecification: OAuthConfigSpecification
    get() =
      getoAuthConfigSpecification() // change property types to induce json validation errors.
        .withCompleteOauthServerOutputSpecification(
          BaseOAuthFlowTest.Companion.getJsonSchema(
            java.util.Map.of<String, Any>(
              "app_id",
              java.util.Map.of<String, String>("type", "integer"),
            ),
          ),
        ).withCompleteOauthOutputSpecification(
          BaseOAuthFlowTest.Companion.getJsonSchema(
            java.util.Map.of<String, Any>(
              "access_token",
              java.util.Map.of<String, String>("type", "integer"),
            ),
          ),
        )

  override val mockedResponse: String
    get() = (
      """{
   "data":{
      "access_token":"access_token_response"
   }
}"""
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
    get() = java.util.Map.of("access_token", "access_token_response")

  @Test
  override fun testDeprecatedCompleteDestinationOAuth() {
  }

  @Test
  override fun testDeprecatedCompleteSourceOAuth() {
  }
}
