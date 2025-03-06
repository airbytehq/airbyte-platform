/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.micronaut.security;

import io.micronaut.http.HttpHeaders;
import io.micronaut.security.token.reader.HttpHeaderTokenReader;
import jakarta.inject.Singleton;

/**
 * This is essentially a re-implementation of Micronaut's <a href=
 * "https://github.com/micronaut-projects/micronaut-security/blob/3.11.x/security-jwt/src/main/java/io/micronaut/security/token/jwt/bearer/BearerTokenReader.java">BearerTokenReader</a>.
 * Micronaut's implementation is only active when the 'jwt' config is enabled, but we want to leave
 * that disabled for now because it makes many assumptions that don't align with our Keycloak-based
 * auth flow.
 */
@SuppressWarnings("PMD.CommentSize")
@Singleton
public class BearerTokenReader extends HttpHeaderTokenReader {

  private static final String AUTHORIZATION_HEADER_NAME = HttpHeaders.AUTHORIZATION;
  private static final String BEARER_PREFIX = "Bearer";

  // copied from Micronaut's BearerTokenReader
  private static final int ORDER = -100;

  @Override
  protected String getPrefix() {
    return BEARER_PREFIX;
  }

  @Override
  protected String getHeaderName() {
    return AUTHORIZATION_HEADER_NAME;
  }

  @Override
  public int getOrder() {
    return ORDER;
  }

}
