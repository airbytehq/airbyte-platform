/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.oauth.flows.google

import com.google.common.annotations.VisibleForTesting
import java.net.http.HttpClient
import java.util.function.Supplier

/**
 * Google Search Console OAuth.
 */
class GoogleSearchConsoleOAuthFlow : GoogleOAuthFlow {
  constructor(httpClient: HttpClient) : super(httpClient)

  @VisibleForTesting
  internal constructor(httpClient: HttpClient, stateSupplier: Supplier<String>) : super(httpClient, stateSupplier)

  override fun getScope(): String = SCOPE_URL

  @Deprecated("")
  override fun getDefaultOAuthOutputPath(): List<String> = listOf("authorization")

  companion object {
    @VisibleForTesting
    const val SCOPE_URL: String = "https://www.googleapis.com/auth/webmasters.readonly"
  }
}
