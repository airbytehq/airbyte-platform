/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.oauth.flows

import io.airbyte.oauth.BaseOAuthFlow

class RetentlyOAuthFlowTest : BaseOAuthFlowTest() {
  override val oAuthFlow: BaseOAuthFlow
    get() = RetentlyOAuthFlow(httpClient) { this.constantState }

  @Suppress("ktlint:standard:max-line-length")
  override val expectedConsentUrl: String
    get() = "https://app.retently.com/api/oauth/authorize?client_id=test_client_id&response_type=code&redirect_uri=https%3A%2F%2Fairbyte.io&state=state"
}
