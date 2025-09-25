/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.api.client.interceptor

import io.airbyte.commons.constants.ApiConstants.AIRBYTE_VERSION_HEADER
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import okhttp3.Interceptor
import okhttp3.Request
import okhttp3.Response
import org.junit.jupiter.api.Test

internal class AirbyteVersionInterceptorTest {
  @Test
  internal fun `test that the airbyte version header is added to the request`() {
    val airbyteVersion = "1.2.3"
    val interceptor = AirbyteVersionInterceptor(airbyteVersion = airbyteVersion)
    val chain: Interceptor.Chain = mockk()
    val builder: Request.Builder = mockk()
    val request: Request = mockk()
    val builtRequest: Request = mockk()

    every { builder.header(any(), any()) } returns builder
    every { builder.build() } returns builtRequest
    every { request.newBuilder() } returns builder
    every { chain.request() } returns request
    every { chain.proceed(any()) } returns mockk<Response>()

    interceptor.intercept(chain)

    verify(exactly = 1) { builder.header(AIRBYTE_VERSION_HEADER, airbyteVersion) }
    verify(exactly = 1) { chain.proceed(builtRequest) }
  }
}
