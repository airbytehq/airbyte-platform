/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.api.client.infrastructure

import io.airbyte.commons.json.Jsons
import io.airbyte.config.FailureReason
import io.airbyte.config.FailureReason.FailureType
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.openapitools.client.infrastructure.Serializer

internal class FailureTypeAdapterTest {
  private lateinit var adapter: FailureTypeAdapter
  private lateinit var failureType: FailureType

  @BeforeEach
  internal fun setUp() {
    adapter = FailureTypeAdapter()
    failureType = FailureType.CONFIG_ERROR
  }

  @Test
  internal fun testToJson() {
    assertEquals(failureType.value(), adapter.toJson(failureType))
  }

  @Test
  internal fun testFromJson() {
    assertEquals(failureType, adapter.fromJson(failureType.value()))
  }

  @Test
  @OptIn(ExperimentalStdlibApi::class)
  internal fun testSerialization() {
    assertEquals("\"${failureType.value()}\"", Serializer.moshi.adapter(FailureType::class.java).toJson(failureType))
  }

  @Test
  @OptIn(ExperimentalStdlibApi::class)
  internal fun testDeserialization() {
    val failureReason = FailureReason()
    failureReason.failureType = failureType
    assertEquals(failureReason, Serializer.moshi.adapter(FailureReason::class.java).fromJson(Jsons.serialize(failureReason)))
  }
}
