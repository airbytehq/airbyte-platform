/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.oauth.flows

import io.airbyte.oauth.BaseOAuthFlow

class AmazonAdsOAuthFlowTest : BaseOAuthFlowTest() {
  override val oAuthFlow: BaseOAuthFlow
    get() = AmazonAdsOAuthFlow(httpClient) { this.constantState }

  @Suppress("ktlint:standard:max-line-length")
  override val expectedConsentUrl: String
    get() = "https://www.amazon.com/ap/oa?client_id=test_client_id&scope=advertising%3A%3Acampaign_management&response_type=code&redirect_uri=https%3A%2F%2Fairbyte.io&state=state"
}
