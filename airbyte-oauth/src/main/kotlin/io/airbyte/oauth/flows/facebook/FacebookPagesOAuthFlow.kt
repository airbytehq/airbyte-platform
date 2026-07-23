/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.oauth.flows.facebook

import io.airbyte.commons.annotation.InternalForTesting
import java.net.http.HttpClient
import java.util.function.Supplier

/**
 * Facebook Pages OAuth.
 */
class FacebookPagesOAuthFlow : FacebookOAuthFlow {
  constructor(httpClient: HttpClient) : super(httpClient)

  @InternalForTesting
  internal constructor(httpClient: HttpClient, stateSupplier: Supplier<String>) : super(httpClient, stateSupplier)

  override fun getScopes(): String = SCOPES

  companion object {
    private const val SCOPES = "pages_manage_ads,pages_manage_metadata,pages_read_engagement,pages_read_user_content,read_insights,catalog_management"
  }
}
