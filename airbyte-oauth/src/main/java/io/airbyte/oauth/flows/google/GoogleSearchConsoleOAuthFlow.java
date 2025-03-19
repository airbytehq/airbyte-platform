/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.oauth.flows.google;

import com.google.common.annotations.VisibleForTesting;
import java.net.http.HttpClient;
import java.util.List;
import java.util.function.Supplier;

/**
 * Google Search Console OAuth.
 */
public class GoogleSearchConsoleOAuthFlow extends GoogleOAuthFlow {

  @VisibleForTesting
  static final String SCOPE_URL = "https://www.googleapis.com/auth/webmasters.readonly";

  public GoogleSearchConsoleOAuthFlow(final HttpClient httpClient) {
    super(httpClient);
  }

  @VisibleForTesting
  GoogleSearchConsoleOAuthFlow(final HttpClient httpClient, final Supplier<String> stateSupplier) {
    super(httpClient, stateSupplier);
  }

  @Override
  protected String getScope() {
    return SCOPE_URL;
  }

  @Override
  @Deprecated
  public List<String> getDefaultOAuthOutputPath() {
    return List.of("authorization");
  }

}
