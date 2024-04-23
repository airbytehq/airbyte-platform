/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.test.acceptance;

import static io.airbyte.commons.auth.AirbyteAuthConstants.X_AIRBYTE_AUTH_HEADER_INTERNAL_PREFIX;

public class AcceptanceTestConstants {

  public static final String IS_ENTERPRISE = "IS_ENTERPRISE";

  /**
   * test-client is a valid internal service name according to the AirbyteAuthInternalTokenValidator.
   * This header value can be used to set up Acceptance Test clients that can authorize as an instance
   * admin. This is useful for testing Enterprise features without needing to set up a valid Keycloak
   * token.
   */
  public static final String X_AIRBYTE_AUTH_HEADER_TEST_CLIENT_VALUE = X_AIRBYTE_AUTH_HEADER_INTERNAL_PREFIX + " test-client";

  /**
   * This is a flag that can be used to enable/disable enterprise-only features in acceptance tests.
   */
  public static final boolean IS_ENTERPRISE_TRUE = System.getenv().getOrDefault(IS_ENTERPRISE, "false").equals("true");

}
