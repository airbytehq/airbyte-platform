/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.auth

import io.micronaut.http.HttpHeaders
import io.micronaut.security.token.reader.HttpHeaderTokenReader
import jakarta.inject.Singleton

/**
 * Custom {@link HttpHeaderTokenReader} that looks for a bearer token authentication header in HTTP requests.
 */
@Singleton
class WorkloadBearerTokenReader : HttpHeaderTokenReader() {
  override fun getPrefix(): String {
    return BEARER_PREFIX
  }

  override fun getHeaderName(): String {
    return HttpHeaders.AUTHORIZATION
  }

  companion object {
    const val BEARER_PREFIX: String = "Bearer"
  }
}
