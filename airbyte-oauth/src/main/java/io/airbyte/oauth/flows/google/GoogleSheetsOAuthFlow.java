/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.oauth.flows.google;

import com.google.common.annotations.VisibleForTesting;
import java.net.http.HttpClient;
import java.util.function.Supplier;

/**
 * Google Sheets OAuth.
 */
public class GoogleSheetsOAuthFlow extends GoogleOAuthFlow {

  // space-delimited string for multiple scopes, see:
  // https://datatracker.ietf.org/doc/html/rfc6749#section-3.3
  @VisibleForTesting
  static final String SCOPE_URL = "https://www.googleapis.com/auth/spreadsheets.readonly https://www.googleapis.com/auth/drive.readonly";

  public GoogleSheetsOAuthFlow(final HttpClient httpClient) {
    super(httpClient);
  }

  @VisibleForTesting
  GoogleSheetsOAuthFlow(final HttpClient httpClient, final Supplier<String> stateSupplier) {
    super(httpClient, stateSupplier);
  }

  @Override
  protected String getScope() {
    return SCOPE_URL;
  }

}
