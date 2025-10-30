/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.security

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.nio.charset.StandardCharsets
import java.util.Collections
import kotlin.concurrent.thread

private val hexadecimalPattern = "[0-9a-fA-F]+".toRegex()

internal class HashFunctionsTest {
  @Test
  fun `should produce 32 character hexadecimal MD5 hash`() {
    val text = "some test to hash"
    val hashed = text.toByteArray(StandardCharsets.UTF_8).md5()
    assertNotEquals(text, hashed)
    assertTrue(hashed.matches(hexadecimalPattern))
    assertEquals(32, hashed.length, "MD5 hash should be 32 characters")
  }

  @Test
  fun `should produce 8 character hexadecimal Murmur3 hash`() {
    val text = "some test to hash"
    val hashed = text.toByteArray(StandardCharsets.UTF_8).murmur332()
    assertNotEquals(text, hashed)
    assertTrue(hashed.matches(hexadecimalPattern))
    assertEquals(8, hashed.length, "Murmur3 hash should be 8 characters")
  }

  @Test
  fun `should produce 64 character hexadecimal SHA-256 hash`() {
    val text = "some test to hash"
    val hashed = text.toByteArray(StandardCharsets.UTF_8).sha256()
    assertNotEquals(text, hashed)
    assertTrue(hashed.matches(hexadecimalPattern))
    assertEquals(64, hashed.length, "SHA-256 hash should be 64 characters")
  }

  @Test
  fun `hash functions are thread-safe`() {
    val testData = "concurrent test".toByteArray(StandardCharsets.UTF_8)
    val results = Collections.synchronizedSet(mutableSetOf<String>())

    val threads =
      (1..100).map {
        thread {
          repeat(100) {
            results.add(testData.md5())
            results.add(testData.murmur332())
            results.add(testData.sha256())
          }
        }
      }
    threads.forEach { it.join() }

    // All threads should produce the same hashes
    assertEquals(3, results.size, "Should have exactly 3 unique hashes")
  }

  @Test
  fun `produces correct MD5 hash values`() {
    assertEquals(
      "d41d8cd98f00b204e9800998ecf8427e",
      "".toByteArray(StandardCharsets.UTF_8).md5(),
    )
    assertEquals(
      "098f6bcd4621d373cade4e832627b4f6",
      "test".toByteArray(StandardCharsets.UTF_8).md5(),
    )
  }
}
