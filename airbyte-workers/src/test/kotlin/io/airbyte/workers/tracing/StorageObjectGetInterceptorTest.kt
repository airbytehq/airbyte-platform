/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.tracing

import datadog.trace.api.interceptor.MutableSpan
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test

internal class StorageObjectGetInterceptorTest {
  @Test
  fun testOnTraceComplete() {
    val simple = DummySpan()

    val unmodifiedError = DummySpan()
    unmodifiedError.setError(true)
    unmodifiedError.setTag("unmodified", true)

    val statusCodeError = DummySpan()
    statusCodeError.setError(true)
    statusCodeError.setTag("peer.hostname", "storage.googleapis.com")
    statusCodeError.setTag("http.status_code", 404)

    val errorMsgError = DummySpan()
    errorMsgError.setError(true)
    errorMsgError.setTag("peer.hostname", "storage.googleapis.com")
    errorMsgError.setTag("error.msg", "404 Not Found and is still missing!")

    val spans =
      listOf<MutableSpan>(
        simple,
        unmodifiedError,
        statusCodeError,
        errorMsgError,
      )

    val interceptor = StorageObjectGetInterceptor()
    val actual: Collection<MutableSpan> = interceptor.onTraceComplete(spans)

    Assertions.assertEquals(spans, actual)
    Assertions.assertTrue(unmodifiedError.isError())
    Assertions.assertFalse(statusCodeError.isError())
    Assertions.assertFalse(errorMsgError.isError())
  }
}
