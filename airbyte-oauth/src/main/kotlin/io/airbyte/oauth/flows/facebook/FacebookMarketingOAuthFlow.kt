/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.oauth.flows.facebook

import com.google.common.annotations.VisibleForTesting
import java.net.http.HttpClient
import java.util.function.Supplier

/**
 * Facebook Marketing OAuth.
 */
class FacebookMarketingOAuthFlow : FacebookOAuthFlow {
  constructor(httpClient: HttpClient) : super(httpClient)

  @VisibleForTesting
  internal constructor(httpClient: HttpClient, stateSupplier: Supplier<String>) : super(httpClient, stateSupplier)

  override fun getScopes(): String = SCOPES

  override fun getDefaultOAuthOutputPath(): List<String> = listOf("credentials")

  companion object {
    private const val SCOPES = "ads_management,ads_read,read_insights,business_management"
  }
}
