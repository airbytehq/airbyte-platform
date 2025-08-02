/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.version

import io.airbyte.commons.json.Jsons
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test

internal class VersionTest {
  @Test
  fun testJsonSerializationDeserialization() {
    val jsonString =
      """
      {"version": "1.2.3"}
      
      """.trimIndent()
    val expectedVersion = Version("1.2.3")

    val deserializedVersion: Version? = Jsons.deserialize(jsonString, Version::class.java)
    Assertions.assertEquals(expectedVersion, deserializedVersion)

    val deserializedVersionLoop: Version? = Jsons.deserialize(Jsons.serialize(deserializedVersion), Version::class.java)
    Assertions.assertEquals(expectedVersion, deserializedVersionLoop)
  }

  @Test
  fun testGreaterThanOrEqualTo() {
    Assertions.assertTrue(LOWER_VERSION.greaterThanOrEqualTo(LOWER_VERSION))
    Assertions.assertTrue(HIGHER_VERSION.greaterThanOrEqualTo(LOWER_VERSION))
    Assertions.assertFalse(LOWER_VERSION.greaterThanOrEqualTo(HIGHER_VERSION))
  }

  @Test
  fun testGreaterThan() {
    Assertions.assertFalse(LOWER_VERSION.greaterThan(LOWER_VERSION))
    Assertions.assertTrue(HIGHER_VERSION.greaterThan(LOWER_VERSION))
    Assertions.assertFalse(LOWER_VERSION.greaterThan(HIGHER_VERSION))
  }

  @Test
  fun testLessThan() {
    Assertions.assertFalse(LOWER_VERSION.lessThan(LOWER_VERSION))
    Assertions.assertFalse(HIGHER_VERSION.lessThan(LOWER_VERSION))
    Assertions.assertTrue(LOWER_VERSION.lessThan(HIGHER_VERSION))
  }

  @Test
  fun testLessThanOrEqualTo() {
    Assertions.assertTrue(LOWER_VERSION.lessThanOrEqualTo(LOWER_VERSION))
    Assertions.assertFalse(HIGHER_VERSION.lessThanOrEqualTo(LOWER_VERSION))
    Assertions.assertTrue(LOWER_VERSION.lessThanOrEqualTo(HIGHER_VERSION))
  }

  companion object {
    private val LOWER_VERSION = Version("1.0.0")
    private val HIGHER_VERSION = Version("1.2.3")
  }
}
