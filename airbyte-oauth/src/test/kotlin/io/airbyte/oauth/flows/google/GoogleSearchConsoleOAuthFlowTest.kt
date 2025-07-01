/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.oauth.flows.google

import io.airbyte.oauth.BaseOAuthFlow
import io.airbyte.oauth.flows.BaseOAuthFlowTest

class GoogleSearchConsoleOAuthFlowTest : BaseOAuthFlowTest() {
  override val oAuthFlow: BaseOAuthFlow
    get() = GoogleSearchConsoleOAuthFlow(httpClient, this::constantState)

  @Suppress("ktlint:standard:max-line-length")
  override val expectedConsentUrl: String
    get() = "https://accounts.google.com/o/oauth2/v2/auth?client_id=test_client_id&redirect_uri=https%3A%2F%2Fairbyte.io&response_type=code&scope=https%3A%2F%2Fwww.googleapis.com%2Fauth%2Fwebmasters.readonly&access_type=offline&state=state&include_granted_scopes=true&prompt=consent"

  override val expectedOutputPath: List<String>
    get() = listOf("authorization")
}
