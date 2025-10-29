/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.oauth.flows

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.oauth.BaseOAuthFlow
import io.airbyte.oauth.CLIENT_ID_KEY
import io.airbyte.oauth.CLIENT_SECRET_KEY
import io.airbyte.oauth.MoreOAuthParameters

class SlackOAuthFlowTest : BaseOAuthFlowTest() {
  override val oAuthFlow: BaseOAuthFlow
    get() = SlackOAuthFlow(httpClient) { this.constantState }

  @Suppress("ktlint:standard:max-line-length")
  override val expectedConsentUrl: String
    get() = "https://slack.com/oauth/v2/authorize?client_id=test_client_id&redirect_uri=https%3A%2F%2Fairbyte.io&state=state&scope=channels%3Ahistory%2Cchannels%3Ajoin%2Cchannels%3Aread%2Cgroups%3Aread%2Cgroups%3Ahistory%2Cusers%3Aread%2Cim%3Ahistory%2Cmpim%3Ahistory%2Cim%3Aread%2Cmpim%3Aread"

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
