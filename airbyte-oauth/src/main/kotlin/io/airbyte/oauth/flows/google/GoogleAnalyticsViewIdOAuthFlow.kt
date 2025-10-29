/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.oauth.flows.google

import io.airbyte.commons.annotation.InternalForTesting
import java.net.http.HttpClient
import java.util.function.Supplier

/**
 * Google Analytics OAuth.
 */
class GoogleAnalyticsViewIdOAuthFlow : GoogleOAuthFlow {
  constructor(httpClient: HttpClient) : super(httpClient)

  @InternalForTesting
  internal constructor(httpClient: HttpClient, stateSupplier: Supplier<String>) : super(httpClient, stateSupplier)

  override fun getScope(): String = SCOPE_URL

  companion object {
    const val SCOPE_URL: String = "https://www.googleapis.com/auth/analytics.readonly"
  }
}
