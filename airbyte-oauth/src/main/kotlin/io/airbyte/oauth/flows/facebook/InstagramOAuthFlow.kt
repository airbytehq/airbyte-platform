/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.oauth.flows.facebook

import com.google.common.annotations.VisibleForTesting
import java.net.http.HttpClient
import java.util.function.Supplier

/**
 * Instagram OAuth.
 *
 * Instagram Graph API require Facebook API User token
 */

class InstagramOAuthFlow : FacebookOAuthFlow {
  constructor(httpClient: HttpClient) : super(httpClient)

  @VisibleForTesting
  internal constructor(httpClient: HttpClient, stateSupplier: Supplier<String>) : super(httpClient, stateSupplier)

  override fun getScopes(): String = SCOPES

  companion object {
    private const val SCOPES =
      "ads_management,business_management,instagram_basic,instagram_manage_insights,pages_show_list,pages_read_engagement"
  }
}
