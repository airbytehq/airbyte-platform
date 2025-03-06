/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.oauth.flows.facebook;

import com.google.common.annotations.VisibleForTesting;
import java.net.http.HttpClient;
import java.util.List;
import java.util.function.Supplier;

/**
 * Facebook Marketing OAuth.
 */
public class FacebookMarketingOAuthFlow extends FacebookOAuthFlow {

  private static final String SCOPES = "ads_management,ads_read,read_insights,business_management";

  public FacebookMarketingOAuthFlow(final HttpClient httpClient) {
    super(httpClient);
  }

  @VisibleForTesting
  FacebookMarketingOAuthFlow(final HttpClient httpClient, final Supplier<String> stateSupplier) {
    super(httpClient, stateSupplier);
  }

  @Override
  protected String getScopes() {
    return SCOPES;
  }

  @Override
  public List<String> getDefaultOAuthOutputPath() {
    return List.of("credentials");
  }

}
