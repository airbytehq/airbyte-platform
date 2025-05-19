/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.oauth.flows

import io.airbyte.oauth.BaseOAuthFlow

class SquareOAuthFlowTest : BaseOAuthFlowTest() {
  override val oAuthFlow: BaseOAuthFlow
    get() = SquareOAuthFlow(httpClient) { this.constantState }

  @Suppress("ktlint:standard:max-line-length")
  override val expectedConsentUrl: String
    get() = (
      "https://connect.squareup.com/oauth2/authorize?client_id=test_client_id" +
        "&scope=CUSTOMERS_READ+EMPLOYEES_READ+ITEMS_READ+MERCHANT_PROFILE_READ+ORDERS_READ+PAYMENTS_READ+TIMECARDS_READ" +
        "&session=False&state=state"
    )
}
