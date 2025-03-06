/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.oauth.flows.facebook;

import com.google.common.annotations.VisibleForTesting;
import java.net.http.HttpClient;
import java.util.function.Supplier;

/**
 * Facebook Pages OAuth.
 */
public class FacebookPagesOAuthFlow extends FacebookOAuthFlow {

  private static final String SCOPES = "pages_manage_ads,pages_manage_metadata,pages_read_engagement,pages_read_user_content";

  public FacebookPagesOAuthFlow(final HttpClient httpClient) {
    super(httpClient);
  }

  @VisibleForTesting
  FacebookPagesOAuthFlow(final HttpClient httpClient, final Supplier<String> stateSupplier) {
    super(httpClient, stateSupplier);
  }

  @Override
  protected String getScopes() {
    return SCOPES;
  }

}
