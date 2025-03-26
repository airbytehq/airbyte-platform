/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.oauth.flows.google;

import com.google.common.annotations.VisibleForTesting;
import java.net.http.HttpClient;
import java.util.function.Supplier;

/**
 * Google Analytics OAuth.
 */
public class GoogleAnalyticsViewIdOAuthFlow extends GoogleOAuthFlow {

  public static final String SCOPE_URL = "https://www.googleapis.com/auth/analytics.readonly";

  public GoogleAnalyticsViewIdOAuthFlow(final HttpClient httpClient) {
    super(httpClient);
  }

  @VisibleForTesting
  GoogleAnalyticsViewIdOAuthFlow(final HttpClient httpClient, final Supplier<String> stateSupplier) {
    super(httpClient, stateSupplier);
  }

  @Override
  protected String getScope() {
    return SCOPE_URL;
  }

}
