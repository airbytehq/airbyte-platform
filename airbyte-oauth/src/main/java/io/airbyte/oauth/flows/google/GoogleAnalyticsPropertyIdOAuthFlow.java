/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.oauth.flows.google;

import com.google.common.annotations.VisibleForTesting;
import java.net.http.HttpClient;
import java.util.function.Supplier;

/**
 * Google Analytics Property Id OAuth.
 */
public class GoogleAnalyticsPropertyIdOAuthFlow extends GoogleOAuthFlow {

  public static final String SCOPE_URL = "https://www.googleapis.com/auth/analytics.readonly";

  public GoogleAnalyticsPropertyIdOAuthFlow(final HttpClient httpClient) {
    super(httpClient);
  }

  @VisibleForTesting
  GoogleAnalyticsPropertyIdOAuthFlow(final HttpClient httpClient, final Supplier<String> stateSupplier) {
    super(httpClient, stateSupplier);
  }

  @Override
  protected String getScope() {
    return SCOPE_URL;
  }

}
