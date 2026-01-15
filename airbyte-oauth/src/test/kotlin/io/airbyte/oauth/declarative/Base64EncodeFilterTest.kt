/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.oauth.declarative

import com.hubspot.jinjava.Jinjava
import com.hubspot.jinjava.JinjavaConfig
import com.hubspot.jinjava.interpret.Context
import com.hubspot.jinjava.interpret.JinjavaInterpreter
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

internal class Base64EncodeFilterTest {
  private var filter: Base64EncodeFilter? = null
  private var interpreter: JinjavaInterpreter? = null

  @BeforeEach
  fun setUp() {
    filter = Base64EncodeFilter()
    val config = JinjavaConfig.newBuilder().build()
    interpreter = JinjavaInterpreter(Jinjava(), Context(), config)
  }

  @Test
  fun testFilterWithValidString() {
    val input = "testValue"
    val expectedBase64 = "dGVzdFZhbHVl"
    val result = filter!!.filter(input, interpreter!!)
    Assertions.assertEquals(expectedBase64, result)
  }

  @Test
  fun testFilterWithBasicAuthCredentials() {
    val input = "client_id:client_secret"
    val expectedBase64 = "Y2xpZW50X2lkOmNsaWVudF9zZWNyZXQ="
    val result = filter!!.filter(input, interpreter!!)
    Assertions.assertEquals(expectedBase64, result)
  }

  @Test
  fun testFilterWithNonString() {
    val input: Any = 12345
    val result = filter!!.filter(input, interpreter!!)
    Assertions.assertEquals(input, result)
  }

  @Test
  fun testFilterWithException() {
    val input = "testValue"
    val brokenFilter: Base64EncodeFilter =
      object : Base64EncodeFilter() {
        override fun encode(value: String): String = throw RuntimeException("Encoding failed")
      }
    val exception =
      Assertions.assertThrows(
        RuntimeException::class.java,
      ) {
        brokenFilter.filter(input, interpreter!!)
      }
    Assertions.assertEquals("Failed to encode value to Base64: `testValue`", exception.message)
  }

  @Test
  fun testFilterName() {
    Assertions.assertEquals("b64encode", filter!!.name)
  }
}
