/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.oauth.flows.google

import com.google.common.annotations.VisibleForTesting
import java.net.http.HttpClient
import java.util.function.Supplier

/**
 * Google Sheets OAuth.
 */
class GoogleSheetsOAuthFlow : GoogleOAuthFlow {
  constructor(httpClient: HttpClient) : super(httpClient)

  @VisibleForTesting
  internal constructor(httpClient: HttpClient, stateSupplier: Supplier<String>) : super(httpClient, stateSupplier)

  override fun getScope(): String = SCOPE_URL

  companion object {
    // space-delimited string for multiple scopes, see:
    // https://datatracker.ietf.org/doc/html/rfc6749#section-3.3
    @VisibleForTesting
    const val SCOPE_URL: String = "https://www.googleapis.com/auth/spreadsheets.readonly https://www.googleapis.com/auth/drive.readonly"
  }
}
