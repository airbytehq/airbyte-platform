/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.oauth.flows.google

import io.airbyte.commons.annotation.InternalForTesting
import java.net.http.HttpClient
import java.util.function.Supplier

/**
 * Google Sheets Destination OAuth.
 */
class DestinationGoogleSheetsOAuthFlow : GoogleOAuthFlow {
  constructor(httpClient: HttpClient) : super(httpClient)

  @InternalForTesting
  constructor(httpClient: HttpClient, stateSupplier: Supplier<String>) : super(httpClient, stateSupplier)

  override fun getScope(): String = SCOPE_URL

  companion object {
    @InternalForTesting
    const val SCOPE_URL = "https://www.googleapis.com/auth/spreadsheets https://www.googleapis.com/auth/drive"
  }
}
