package io.airbyte.workload.handler

import io.airbyte.workload.repository.domain.WorkloadLabel
import io.airbyte.workload.repository.domain.WorkloadStatus
import io.airbyte.workload.repository.domain.WorkloadType
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.time.Instant
import java.time.ZoneOffset
import java.util.UUID

class WorkloadMapperKtTest {
  @Test
  fun `test from domain workload to api workload`() {
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
        logPath = "log/path",
        mutexKey = "mutexKey",
        type = WorkloadType.SYNC,
        terminationReason = "terminationReason",
        terminationSource = "terminationSource",
        deadline = Instant.now().atOffset(ZoneOffset.UTC),
        autoId = UUID.randomUUID(),
        signalInput = "signalPayload",
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
  }

  @Test
  fun `test from domain workload label to api workload label`() {
    val domainWorkloadLabel = WorkloadLabel(id = UUID.randomUUID(), key = "key", value = "value")
    val apiWorkloadLabel = domainWorkloadLabel.toApi()
    assertEquals(domainWorkloadLabel.key, apiWorkloadLabel.key)
    assertEquals(domainWorkloadLabel.value, apiWorkloadLabel.value)
  }
}
