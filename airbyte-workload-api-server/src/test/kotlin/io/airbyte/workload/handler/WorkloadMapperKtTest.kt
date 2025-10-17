/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.handler

import io.airbyte.config.WorkloadPriority
import io.airbyte.workload.repository.domain.WorkloadLabel
import io.airbyte.workload.repository.domain.WorkloadStatus
import io.airbyte.workload.repository.domain.WorkloadType
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import java.time.Instant
import java.time.ZoneOffset
import java.util.UUID
import java.util.stream.Stream

class WorkloadMapperKtTest {
  @ParameterizedTest
  @MethodSource("priorityMatrix")
  fun `test from domain workload to api workload`(
    domainPriority: Int?,
    expectedApiPriority: WorkloadPriority?,
  ) {
    val createdAt = Instant.now().atOffset(ZoneOffset.UTC)
    val lastHeartbeatAt = createdAt.plusHours(1)
    val updatedAt = lastHeartbeatAt.plusHours(1)

    val domainWorkload =
      DomainWorkload(
        id = "id",
        dataplaneId = "dataplaneId",
        status = WorkloadStatus.PENDING,
        createdAt = createdAt,
        updatedAt = updatedAt,
        lastHeartbeatAt = lastHeartbeatAt,
        workloadLabels = listOf(WorkloadLabel(id = UUID.randomUUID(), key = "key", value = "value")),
        inputPayload = "inputPayload",
        workspaceId = UUID.randomUUID(),
        organizationId = UUID.randomUUID(),
        logPath = "log/path",
        mutexKey = "mutexKey",
        type = WorkloadType.SYNC,
        terminationReason = "terminationReason",
        terminationSource = "terminationSource",
        deadline = Instant.now().atOffset(ZoneOffset.UTC),
        autoId = UUID.randomUUID(),
        signalInput = "signalPayload",
        dataplaneGroup = "dataplane-group",
        priority = domainPriority,
      )

    val apiWorkload = domainWorkload.toApi()

    assertEquals(domainWorkload.id, apiWorkload.id)
    assertEquals(domainWorkload.dataplaneId, apiWorkload.dataplaneId)
    assertEquals(domainWorkload.status.toApi(), apiWorkload.status)
    assertEquals(domainWorkload.workloadLabels!!.map { x -> x.key }, apiWorkload.labels.map { x -> x.key })
    assertEquals(domainWorkload.workloadLabels!!.map { x -> x.value }, apiWorkload.labels.map { x -> x.value })
    assertEquals(domainWorkload.inputPayload, apiWorkload.inputPayload)
    assertEquals(domainWorkload.logPath, apiWorkload.logPath)
    assertEquals(domainWorkload.mutexKey, apiWorkload.mutexKey)
    assertEquals(ApiWorkloadType.SYNC, apiWorkload.type)
    assertEquals(domainWorkload.terminationReason, apiWorkload.terminationReason)
    assertEquals(domainWorkload.terminationSource, apiWorkload.terminationSource)
    assertEquals(domainWorkload.autoId, apiWorkload.autoId)
    assertEquals(domainWorkload.signalInput, apiWorkload.signalInput)
    assertEquals(domainWorkload.dataplaneGroup, apiWorkload.dataplaneGroup)
    assertEquals(expectedApiPriority, apiWorkload.priority)
    assertEquals(domainWorkload.workspaceId, apiWorkload.workspaceId)
    assertEquals(domainWorkload.organizationId, apiWorkload.organizationId)
  }

  @Test
  fun `test from domain workload label to api workload label`() {
    val domainWorkloadLabel = WorkloadLabel(id = UUID.randomUUID(), key = "key", value = "value")
    val apiWorkloadLabel = domainWorkloadLabel.toApi()
    assertEquals(domainWorkloadLabel.key, apiWorkloadLabel.key)
    assertEquals(domainWorkloadLabel.value, apiWorkloadLabel.value)
  }

  @Test
  fun `test labels fallback - prefers JSONB labels when present`() {
    val domainWorkload =
      DomainWorkload(
        id = "id",
        dataplaneId = null,
        status = WorkloadStatus.PENDING,
        workloadLabels =
          listOf(
            WorkloadLabel(id = UUID.randomUUID(), key = "legacy_key", value = "legacy_value"),
          ),
        labels = mapOf("jsonb_key" to "jsonb_value"), // JSONB labels present
        inputPayload = "payload",
        workspaceId = UUID.randomUUID(),
        organizationId = UUID.randomUUID(),
        logPath = "/log",
        mutexKey = null,
        type = WorkloadType.SYNC,
        autoId = UUID.randomUUID(),
        signalInput = null,
      )

    val apiWorkload = domainWorkload.toApi()

    // Should use JSONB labels, not workloadLabels
    assertEquals(1, apiWorkload.labels.size)
    assertEquals("jsonb_key", apiWorkload.labels[0].key)
    assertEquals("jsonb_value", apiWorkload.labels[0].value)
  }

  @Test
  fun `test labels fallback - uses workloadLabels when JSONB is null`() {
    val domainWorkload =
      DomainWorkload(
        id = "id",
        dataplaneId = null,
        status = WorkloadStatus.PENDING,
        workloadLabels =
          listOf(
            WorkloadLabel(id = UUID.randomUUID(), key = "legacy_key", value = "legacy_value"),
          ),
        labels = null, // JSONB labels not present
        inputPayload = "payload",
        workspaceId = UUID.randomUUID(),
        organizationId = UUID.randomUUID(),
        logPath = "/log",
        mutexKey = null,
        type = WorkloadType.SYNC,
        autoId = UUID.randomUUID(),
        signalInput = null,
      )

    val apiWorkload = domainWorkload.toApi()

    // Should fall back to workloadLabels
    assertEquals(1, apiWorkload.labels.size)
    assertEquals("legacy_key", apiWorkload.labels[0].key)
    assertEquals("legacy_value", apiWorkload.labels[0].value)
  }

  @Test
  fun `test labels fallback - uses workloadLabels when JSONB is empty`() {
    val domainWorkload =
      DomainWorkload(
        id = "id",
        dataplaneId = null,
        status = WorkloadStatus.PENDING,
        workloadLabels =
          listOf(
            WorkloadLabel(id = UUID.randomUUID(), key = "legacy_key", value = "legacy_value"),
          ),
        labels = emptyMap(), // JSONB labels empty
        inputPayload = "payload",
        workspaceId = UUID.randomUUID(),
        organizationId = UUID.randomUUID(),
        logPath = "/log",
        mutexKey = null,
        type = WorkloadType.SYNC,
        autoId = UUID.randomUUID(),
        signalInput = null,
      )

    val apiWorkload = domainWorkload.toApi()

    // Should fall back to workloadLabels when JSONB is empty
    assertEquals(1, apiWorkload.labels.size)
    assertEquals("legacy_key", apiWorkload.labels[0].key)
    assertEquals("legacy_value", apiWorkload.labels[0].value)
  }

  @Test
  fun `test labels fallback - returns empty list when both are null`() {
    val domainWorkload =
      DomainWorkload(
        id = "id",
        dataplaneId = null,
        status = WorkloadStatus.PENDING,
        workloadLabels = null,
        labels = null,
        inputPayload = "payload",
        workspaceId = UUID.randomUUID(),
        organizationId = UUID.randomUUID(),
        logPath = "/log",
        mutexKey = null,
        type = WorkloadType.SYNC,
        autoId = UUID.randomUUID(),
        signalInput = null,
      )

    val apiWorkload = domainWorkload.toApi()

    // Should return empty list when both are null
    assertEquals(0, apiWorkload.labels.size)
  }

  companion object {
    @JvmStatic
    fun priorityMatrix(): Stream<Arguments> =
      Stream.of(
        Arguments.of(null, null),
        Arguments.of(0, WorkloadPriority.DEFAULT),
        Arguments.of(1, WorkloadPriority.HIGH),
      )
  }
}
