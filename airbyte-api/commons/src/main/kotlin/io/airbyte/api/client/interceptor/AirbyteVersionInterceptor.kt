/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.api.client.interceptor

import io.airbyte.commons.constants.ApiConstants.AIRBYTE_VERSION_HEADER
import okhttp3.Interceptor
import okhttp3.Response

class AirbyteVersionInterceptor(
  private val airbyteVersion: String,
) : Interceptor {
  override fun intercept(chain: Interceptor.Chain): Response =
    chain
      .request()
      .newBuilder()
      .header(AIRBYTE_VERSION_HEADER, airbyteVersion)
      .build()
      .let { chain.proceed(it) }
}
