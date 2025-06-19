/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.oauth.flows.facebook

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.oauth.BaseOAuthFlow
import io.airbyte.oauth.MoreOAuthParameters
import io.airbyte.oauth.flows.BaseOAuthFlowTest

class FacebookPagesOAuthFlowTest : BaseOAuthFlowTest() {
  override val oAuthFlow: BaseOAuthFlow
    get() = FacebookPagesOAuthFlow(httpClient, this::constantState)

  @Suppress("ktlint:standard:max-line-length")
  override val expectedConsentUrl: String
    get() = "https://www.facebook.com/v21.0/dialog/oauth?client_id=test_client_id&redirect_uri=https%3A%2F%2Fairbyte.io&state=state&scope=pages_manage_ads%2Cpages_manage_metadata%2Cpages_read_engagement%2Cpages_read_user_content"

  override val expectedOutputPath: List<String>
    get() = listOf()

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
    get() = getJsonSchema(mapOf("access_token" to mapOf("type" to "string")))

  override val expectedFilteredOutput: Map<String, String>
    get() =
      java.util.Map.of(
        "access_token",
        "access_token_response",
        "client_id",
        MoreOAuthParameters.SECRET_MASK,
      )
}
