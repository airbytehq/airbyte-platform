/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.oauth.flows

import io.airbyte.oauth.BaseOAuthFlow

class LinkedinAdsOAuthFlowTest : BaseOAuthFlowTest() {
  override val oAuthFlow: BaseOAuthFlow
    get() = LinkedinAdsOAuthFlow(httpClient) { this.constantState }

  @Suppress("ktlint:standard:max-line-length")
  override val expectedConsentUrl: String
    get() = "https://www.linkedin.com/oauth/v2/authorization?client_id=test_client_id&redirect_uri=https%3A%2F%2Fairbyte.io&response_type=code&scope=r_ads_reporting+r_emailaddress+r_liteprofile+r_ads+r_basicprofile+r_organization_social&state=state"
}
