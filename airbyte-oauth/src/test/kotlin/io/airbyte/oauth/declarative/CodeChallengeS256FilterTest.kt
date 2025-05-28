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

internal class CodeChallengeS256FilterTest {
  private var filter: CodeChallengeS256Filter? = null
  private var interpreter: JinjavaInterpreter? = null

  @BeforeEach
  fun setUp() {
    filter = CodeChallengeS256Filter()
    val config = JinjavaConfig.newBuilder().build()
    interpreter = JinjavaInterpreter(Jinjava(), Context(), config)
  }

  @Test
  @Throws(Exception::class)
  fun testFilterWithValidString() {
    val input = "testValue"
    val expectedHash = "gv4Mg0y+oGkBPF63go5Zmmk+DSQRiH4qsnMnFmKXMII="
    val result = filter!!.filter(input, interpreter!!)
    Assertions.assertEquals(expectedHash, result)
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
    val brokenFilter: CodeChallengeS256Filter =
      object : CodeChallengeS256Filter() {
        override fun getCodeChallenge(value: String): String = throw RuntimeException("SHA-256 algorithm not available")
      }
    val exception =
      Assertions.assertThrows(
        RuntimeException::class.java,
      ) {
        brokenFilter.filter(input, interpreter!!)
      }
    Assertions.assertEquals("Failed to get `codechallengeS256` from: `testValue`", exception.message)
  }
}
