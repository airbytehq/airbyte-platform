/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.auth

/**
 * Constants that relate to Airbyte instance authentication. For now, this primarily relates to
 * auth-related features of Airbyte Enterprise, but may expand to include all authentication-related
 * constants across Airbyte products.
 */
object AirbyteAuthConstants {
  /**
   * Header used for internal service authentication. This header is dropped by the webapp proxy so
   * that external requests cannot use it to authenticate as an internal service.
   */
  const val X_AIRBYTE_AUTH_HEADER: String = "X-Airbyte-Auth"

  /**
   * Header prefix used to identify internal service authentication. For now, this is the only use
   * case for the X-Airbyte-Auth header, but in the future, we may add new prefixes for other use
   * cases.
   */
  const val X_AIRBYTE_AUTH_HEADER_INTERNAL_PREFIX: String = "Internal"

  /**
   * Set of valid internal service names that are able to use the X-Airbyte-Auth: Internal header.
   */
  val VALID_INTERNAL_SERVICE_NAMES = setOf("worker", "test-client")
}
