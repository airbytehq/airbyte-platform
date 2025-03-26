/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.pro

import io.airbyte.commons.auth.AirbyteAuthConstants
import io.micronaut.context.annotation.Requires
import io.micronaut.security.token.reader.HttpHeaderTokenReader
import jakarta.inject.Singleton

/**
 * Token reader for internal Airbyte auth. This is used to authenticate internal requests between
 * Airbyte services. Traffic between internal services is assumed to be trusted, so this is not a
 * means of security, but rather a mechanism for identifying and granting roles to the service that
 * is making the internal request. The webapp proxy unsets the X-Airbyte-Auth header, so this header
 * will only be present on internal requests.
 */
@Singleton
@Requires(property = "micronaut.security.enabled", value = "true")
@Requires(property = "airbyte.edition", pattern = "(?i)^community|enterprise$")
class AirbyteAuthInternalTokenReader : HttpHeaderTokenReader() {
  override fun getPrefix(): String = AirbyteAuthConstants.X_AIRBYTE_AUTH_HEADER_INTERNAL_PREFIX

  override fun getHeaderName(): String = AirbyteAuthConstants.X_AIRBYTE_AUTH_HEADER

  override fun getOrder(): Int = ORDER

  companion object {
    // This is set higher than other token readers so that it is checked last.
    // The BearerTokenReader, for instance, should take precedence over this one.
    private const val ORDER = 100
  }
}
