/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.oauth.flows

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.oauth.BaseOAuthFlow
import io.airbyte.oauth.CLIENT_ID_KEY
import io.airbyte.oauth.CLIENT_SECRET_KEY
import io.airbyte.oauth.MoreOAuthParameters

class IntercomOAuthFlowTest : BaseOAuthFlowTest() {
  override val oAuthFlow: BaseOAuthFlow
    get() = IntercomOAuthFlow(httpClient) { this.constantState }

  override val expectedOutputPath: List<String?>
    get() = listOf<String>()

  @Suppress("ktlint:standard:max-line-length")
  override val expectedConsentUrl: String
    get() = "https://app.intercom.com/a/oauth/connect?client_id=test_client_id&redirect_uri=https%3A%2F%2Fairbyte.io&response_type=code&state=state"

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
