/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.api.client.infrastructure

import io.airbyte.commons.json.Jsons
import io.airbyte.config.FailureReason
import io.airbyte.config.FailureReason.FailureOrigin
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.openapitools.client.infrastructure.Serializer

internal class FailureOriginAdapterTest {
  private lateinit var adapter: FailureOriginAdapter
  private lateinit var failureOrigin: FailureOrigin

  @BeforeEach
  internal fun setUp() {
    adapter = FailureOriginAdapter()
    failureOrigin = FailureOrigin.SOURCE
  }

  @Test
  internal fun testToJson() {
    assertEquals(failureOrigin.value(), adapter.toJson(failureOrigin))
  }

  @Test
  internal fun testFromJson() {
    assertEquals(failureOrigin, adapter.fromJson(failureOrigin.value()))
  }

  @Test
  internal fun testSerialization() {
    assertEquals("\"${failureOrigin.value()}\"", Serializer.moshi.adapter(FailureOrigin::class.java).toJson(failureOrigin))
  }

  @Test
  internal fun testDeserialization() {
    val failureReason = FailureReason()
    failureReason.failureOrigin = failureOrigin
    assertEquals(failureReason, Serializer.moshi.adapter(FailureReason::class.java).fromJson(Jsons.serialize(failureReason)))
  }
}
