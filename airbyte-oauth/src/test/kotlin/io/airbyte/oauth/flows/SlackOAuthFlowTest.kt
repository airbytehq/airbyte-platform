/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.oauth.flows

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.oauth.BaseOAuthFlow
import io.airbyte.oauth.MoreOAuthParameters

class SlackOAuthFlowTest : BaseOAuthFlowTest() {
  override val oAuthFlow: BaseOAuthFlow
    get() = SlackOAuthFlow(httpClient) { this.constantState }

  @Suppress("ktlint:standard:max-line-length")
  override val expectedConsentUrl: String
    get() = "https://slack.com/oauth/authorize?client_id=test_client_id&redirect_uri=https%3A%2F%2Fairbyte.io&state=state&scope=read"

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
