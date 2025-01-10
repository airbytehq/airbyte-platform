/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.oauth.flows.google;

import com.google.common.annotations.VisibleForTesting;
import java.net.http.HttpClient;
import java.util.function.Supplier;

/**
 * YouTube Analytics OAuth.
 */
public class YouTubeAnalyticsOAuthFlow extends GoogleOAuthFlow {

  private static final String SCOPE_URL =
      "https://www.googleapis.com/auth/yt-analytics.readonly https://www.googleapis.com/auth/yt-analytics-monetary.readonly";

  public YouTubeAnalyticsOAuthFlow(final HttpClient httpClient) {
    super(httpClient);
  }

  @VisibleForTesting
  YouTubeAnalyticsOAuthFlow(final HttpClient httpClient, final Supplier<String> stateSupplier) {
    super(httpClient, stateSupplier);
  }

  @Override
  protected String getScope() {
    return SCOPE_URL;
  }

}
