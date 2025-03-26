/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.auth;

import java.util.Set;

/**
 * Constants that relate to Airbyte instance authentication. For now, this primarily relates to
 * auth-related features of Airbyte Pro, but may expand to include all authentication-related
 * constants across Airbyte products.
 */
public class AirbyteAuthConstants {

  /**
   * Header used for internal service authentication. This header is dropped by the webapp proxy so
   * that external requests cannot use it to authenticate as an internal service.
   */
  public static final String X_AIRBYTE_AUTH_HEADER = "X-Airbyte-Auth";

  /**
   * Header prefix used to identify internal service authentication. For now, this is the only use
   * case for the X-Airbyte-Auth header, but in the future, we may add new prefixes for other use
   * cases.
   */
  public static final String X_AIRBYTE_AUTH_HEADER_INTERNAL_PREFIX = "Internal";

  /**
   * Set of valid internal service names that are able to use the X-Airbyte-Auth: Internal header.
   */
  public static final Set<String> VALID_INTERNAL_SERVICE_NAMES = Set.of("worker", "test-client");

}
