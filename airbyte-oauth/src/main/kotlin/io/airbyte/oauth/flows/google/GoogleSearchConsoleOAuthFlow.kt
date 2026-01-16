/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.oauth.flows.google

import io.airbyte.commons.annotation.InternalForTesting
import java.net.http.HttpClient
import java.util.function.Supplier

/**
 * Google Search Console OAuth.
 */
class GoogleSearchConsoleOAuthFlow : GoogleOAuthFlow {
  constructor(httpClient: HttpClient) : super(httpClient)

  @InternalForTesting
  internal constructor(httpClient: HttpClient, stateSupplier: Supplier<String>) : super(httpClient, stateSupplier)

  override fun getScope(): String = SCOPE_URL

  @Deprecated("")
  override fun getDefaultOAuthOutputPath(): List<String> = listOf("authorization")

  companion object {
    @InternalForTesting
    const val SCOPE_URL: String = "https://www.googleapis.com/auth/webmasters.readonly"
  }
}
