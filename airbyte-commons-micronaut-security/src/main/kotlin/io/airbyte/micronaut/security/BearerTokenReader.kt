/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.micronaut.security

import io.micronaut.http.HttpHeaders
import io.micronaut.security.token.reader.HttpHeaderTokenReader
import jakarta.inject.Singleton

// copied from Micronaut's BearerTokenReader
internal const val ORDER = -100
internal const val AUTHORIZATION_HEADER_NAME = HttpHeaders.AUTHORIZATION
internal const val BEARER_PREFIX = "Bearer"

/**
 * This is essentially a re-implementation of Micronaut's <a href=
 * "https://github.com/micronaut-projects/micronaut-security/blob/3.11.x/security-jwt/src/main/java/io/micronaut/security/token/jwt/bearer/BearerTokenReader.java">BearerTokenReader</a>.
 *
 * Micronaut's implementation is only active when the 'jwt' config is enabled, but we want to leave that disabled for now
 * because it makes many assumptions that don't align with our Keycloak-based auth flow.
 */
@Singleton
class BearerTokenReader : HttpHeaderTokenReader() {
  override fun getPrefix(): String = BEARER_PREFIX

  override fun getHeaderName(): String = AUTHORIZATION_HEADER_NAME

  override fun getOrder(): Int = ORDER
}
