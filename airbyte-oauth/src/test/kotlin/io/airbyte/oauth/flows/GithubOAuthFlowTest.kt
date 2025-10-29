/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.oauth.flows

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.oauth.BaseOAuthFlow
import io.airbyte.oauth.CLIENT_ID_KEY
import io.airbyte.oauth.CLIENT_SECRET_KEY
import io.airbyte.oauth.MoreOAuthParameters

class GithubOAuthFlowTest : BaseOAuthFlowTest() {
  override val oAuthFlow: BaseOAuthFlow
    get() = GithubOAuthFlow(httpClient) { this.constantState }

  @Suppress("ktlint:standard:max-line-length")
  override val expectedConsentUrl: String
    get() = "https://github.com/login/oauth/authorize?client_id=test_client_id&redirect_uri=https%3A%2F%2Fairbyte.io&scope=repo%20read:org%20read:repo_hook%20read:user%20read:project%20read:discussion%20workflow&state=state"

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
