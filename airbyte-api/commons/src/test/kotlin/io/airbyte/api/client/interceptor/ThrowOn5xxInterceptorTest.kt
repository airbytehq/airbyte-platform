/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.api.client.interceptor

import io.micronaut.http.HttpStatus
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import okhttp3.Interceptor
import okhttp3.Request
import okhttp3.Response
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertDoesNotThrow
import org.junit.jupiter.api.assertThrows
import java.io.IOException

internal class ThrowOn5xxInterceptorTest {
  @Test
  internal fun `test that when the response has an error status code, an exception is thrown`() {
    val statusCode = HttpStatus.INTERNAL_SERVER_ERROR.code
    val responseMessage = "error"
    val chain: Interceptor.Chain =
      mockk {
        every { request() } returns mockk<Request>()
        every { proceed(any()) } returns
          mockk<Response> {
            every { code } returns statusCode
            every { message } returns responseMessage
          }
      }

    val interceptor = ThrowOn5xxInterceptor()

    val e =
      assertThrows<IOException> {
        interceptor.intercept(chain)
      }
    assertEquals("HTTP error: $statusCode $responseMessage", e.message)
    verify(exactly = 1) { chain.proceed(any()) }
  }

  @Test
  internal fun `test that when the response is not an error, an exception is not thrown`() {
    val chain: Interceptor.Chain =
      mockk {
        every { request() } returns mockk<Request>()
        every { proceed(any()) } returns
          mockk<Response> {
            every { code } returns HttpStatus.OK.code
          }
      }

    val interceptor = ThrowOn5xxInterceptor()

    assertDoesNotThrow {
      interceptor.intercept(chain)
    }
    verify(exactly = 1) { chain.proceed(any()) }
  }
}
