/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.pro;

import io.airbyte.commons.license.annotation.RequiresAirbyteProEnabled;
import io.micronaut.security.token.reader.HttpHeaderTokenReader;
import io.micronaut.security.token.reader.TokenReader;
import jakarta.inject.Singleton;

/**
 * Token reader for internal Airbyte auth. This is used to authenticate internal requests between
 * Airbyte services. Traffic between internal services is assumed to be trusted, so this is not a
 * means of security, but rather a mechanism for identifying and granting roles to the service that
 * is making the internal request. The webapp proxy unsets the X-Airbyte-Auth header, so this header
 * will only be present on internal requests.
 */
@Singleton
@RequiresAirbyteProEnabled
public class AirbyteAuthInternalTokenReader extends HttpHeaderTokenReader implements TokenReader {

  // This is set higher than other token readers so that it is checked last.
  // The BearerTokenReader, for instance, should take precedence over this one.
  private static final int ORDER = 100;

  @Override
  protected String getPrefix() {
    return AirbyteAuthConstants.AIRBYTE_AUTH_HEADER_INTERNAL_PREFIX;
  }

  @Override
  protected String getHeaderName() {
    return AirbyteAuthConstants.AIRBYTE_AUTH_HEADER;
  }

  @Override
  public int getOrder() {
    return ORDER;
  }

}
