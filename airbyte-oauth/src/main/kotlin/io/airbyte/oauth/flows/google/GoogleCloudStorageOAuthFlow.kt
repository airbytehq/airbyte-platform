/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.oauth.flows.google

import com.google.common.annotations.VisibleForTesting
import java.net.http.HttpClient
import java.util.function.Supplier

/**
 * Google Cloud Storage OAuth.
 */
class GoogleCloudStorageOAuthFlow : GoogleOAuthFlow {
  constructor(httpClient: HttpClient) : super(httpClient)

  @VisibleForTesting
  internal constructor(httpClient: HttpClient, stateSupplier: Supplier<String>) : super(httpClient, stateSupplier)

  override fun getScope(): String = SCOPE_URL

  companion object {
    @VisibleForTesting
    const val SCOPE_URL: String = "https://www.googleapis.com/auth/devstorage.read_only"
  }
}
