/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.test.acceptance;

import static io.airbyte.commons.auth.AirbyteAuthConstants.X_AIRBYTE_AUTH_HEADER;
import static io.airbyte.test.acceptance.AcceptanceTestConstants.X_AIRBYTE_AUTH_HEADER_TEST_CLIENT_VALUE;

import java.io.IOException;
import okhttp3.Interceptor;
import okhttp3.Request;
import org.jetbrains.annotations.NotNull;

/**
 * Interceptor that adds the X-Airbyte-Auth header to requests. This is useful for testing
 * Enterprise features without needing to set up a valid Keycloak token. Used for setting up
 * `AirbyteApiClient2` instances specifically.
 */
public class AcceptanceTestAuthHeaderInterceptor implements Interceptor {

  @NotNull
  @Override
  public okhttp3.Response intercept(final Chain chain) throws IOException {
    final Request original = chain.request();
    final Request request = original.newBuilder()
        .header(X_AIRBYTE_AUTH_HEADER, X_AIRBYTE_AUTH_HEADER_TEST_CLIENT_VALUE)
        .method(original.method(), original.body())
        .build();
    return chain.proceed(request);
  }

}
