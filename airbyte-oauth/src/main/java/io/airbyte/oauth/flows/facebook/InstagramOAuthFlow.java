/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.oauth.flows.facebook;

import com.google.common.annotations.VisibleForTesting;
import java.net.http.HttpClient;
import java.util.function.Supplier;

/**
 * Instagram OAuth.
 */
// Instagram Graph API require Facebook API User token
public class InstagramOAuthFlow extends FacebookOAuthFlow {

  private static final String SCOPES =
      "ads_management,business_management,instagram_basic,instagram_manage_insights,pages_show_list,pages_read_engagement";

  public InstagramOAuthFlow(final HttpClient httpClient) {
    super(httpClient);
  }

  @VisibleForTesting
  InstagramOAuthFlow(final HttpClient httpClient, final Supplier<String> stateSupplier) {
    super(httpClient, stateSupplier);
  }

  @Override
  protected String getScopes() {
    return SCOPES;
  }

}
