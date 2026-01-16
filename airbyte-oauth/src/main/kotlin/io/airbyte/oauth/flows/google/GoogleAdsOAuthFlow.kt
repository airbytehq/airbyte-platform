/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.oauth.flows.google

import io.airbyte.commons.annotation.InternalForTesting
import java.net.http.HttpClient
import java.util.function.Supplier

/**
 * Google Ads OAuth.
 */
class GoogleAdsOAuthFlow : GoogleOAuthFlow {
  constructor(httpClient: HttpClient) : super(httpClient)

  @InternalForTesting
  internal constructor(httpClient: HttpClient, stateSupplier: Supplier<String>) : super(httpClient, stateSupplier)

  override fun getScope(): String = SCOPE_URL

  companion object {
    @InternalForTesting
    const val SCOPE_URL: String = "https://www.googleapis.com/auth/adwords"
  }
}
