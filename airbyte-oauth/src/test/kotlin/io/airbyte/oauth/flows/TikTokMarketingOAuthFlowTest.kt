/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.oauth.flows

import com.fasterxml.jackson.databind.JsonNode
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
        mapOf(
          "app_id" to "app_id",
          "secret" to "secret",
        ),
      )

  override val oAuthConfigSpecification: OAuthConfigSpecification
    get() =
      getOauthConfigSpecification() // change property types to induce json validation errors.
        .withCompleteOauthServerOutputSpecification(
          getJsonSchema(
            mapOf(
              "app_id" to mapOf("type" to "integer"),
            ),
          ),
        ).withCompleteOauthOutputSpecification(
          getJsonSchema(
            mapOf(
              "access_token" to mapOf("type" to "integer"),
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
      getJsonSchema(
        mapOf(
          "access_token" to mapOf("type" to "string"),
        ),
      )

  override val expectedFilteredOutput: Map<String, String>
    get() = mapOf("access_token" to "access_token_response")

  @Test
  override fun testDeprecatedCompleteDestinationOAuth() {
  }

  @Test
  override fun testDeprecatedCompleteSourceOAuth() {
  }
}
