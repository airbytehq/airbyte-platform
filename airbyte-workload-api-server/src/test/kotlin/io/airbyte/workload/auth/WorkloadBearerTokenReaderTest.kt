/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.auth

import io.airbyte.workload.auth.WorkloadBearerTokenReader.Companion.BEARER_PREFIX
import io.micronaut.http.HttpHeaders
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test

class WorkloadBearerTokenReaderTest {
  @Test
  internal fun `test that the correct header is used by the reader`() {
    val reader = WorkloadBearerTokenReader()
    val method = reader.javaClass.getDeclaredMethod("getHeaderName")
    method.isAccessible = true
    val headerName = method.invoke(reader)
    Assertions.assertEquals(HttpHeaders.AUTHORIZATION, headerName)
  }

  @Test
  internal fun `test that the correct header value prefix is used by the reader`() {
    val reader = WorkloadBearerTokenReader()
    val method = reader.javaClass.getDeclaredMethod("getPrefix")
    method.isAccessible = true
    val prefix = method.invoke(reader)
    Assertions.assertEquals(BEARER_PREFIX, prefix)
  }
}
