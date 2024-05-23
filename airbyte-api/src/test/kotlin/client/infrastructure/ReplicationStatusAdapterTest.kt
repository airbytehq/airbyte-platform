/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.api.client.infrastructure

import io.airbyte.commons.json.Jsons
import io.airbyte.config.StandardSyncSummary
import io.airbyte.config.StandardSyncSummary.ReplicationStatus
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.openapitools.client.infrastructure.Serializer

internal class ReplicationStatusAdapterTest {
  private lateinit var adapter: ReplicationStatusAdapter
  private lateinit var replicationStatus: ReplicationStatus

  @BeforeEach
  internal fun setUp() {
    adapter = ReplicationStatusAdapter()
    replicationStatus = ReplicationStatus.COMPLETED
  }

  @Test
  internal fun testToJson() {
    assertEquals(replicationStatus.value(), adapter.toJson(replicationStatus))
  }

  @Test
  internal fun testFromJson() {
    assertEquals(replicationStatus, adapter.fromJson(replicationStatus.value()))
  }

  @Test
  internal fun testSerialization() {
    assertEquals("\"${replicationStatus.value()}\"", Serializer.moshi.adapter(ReplicationStatus::class.java).toJson(replicationStatus))
  }

  @Test
  internal fun testDeserialization() {
    val standardSyncSummary = StandardSyncSummary()
    standardSyncSummary.status = replicationStatus
    assertEquals(standardSyncSummary, Serializer.moshi.adapter(StandardSyncSummary::class.java).fromJson(Jsons.serialize(standardSyncSummary)))
  }
}
