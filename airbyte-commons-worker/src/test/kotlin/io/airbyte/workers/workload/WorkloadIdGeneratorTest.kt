/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.workload

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.util.UUID

class WorkloadIdGeneratorTest {
  private val generator = WorkloadIdGenerator()

  @Test
  internal fun `test that the correct workload ID is generated for check`() {
    val actorId = UUID.randomUUID()
    val jobId = UUID.randomUUID()

    val generatedWorkloadId = generator.generateCheckWorkloadId(actorId, jobId)
    assertEquals(
      "${actorId}_${jobId}_check",
      generatedWorkloadId,
    )
  }

  @Test
  internal fun `test that the correct workload ID is generated for discover`() {
    val actorId = UUID.randomUUID()
    val jobId = UUID.randomUUID()

    val generatedWorkloadId = generator.generateDiscoverWorkloadId(actorId, jobId)
    assertEquals(
      "${actorId}_${jobId}_discover",
      generatedWorkloadId,
    )
  }

  @Test
  internal fun `test that the correct workload ID is generated for specs`() {
    val workspaceId = UUID.randomUUID()
    val jobId = UUID.randomUUID()

    val generatedWorkloadId = generator.generateSpeckWorkloadId(workspaceId, jobId)
    assertEquals(
      "${workspaceId}_${jobId}_spec",
      generatedWorkloadId,
    )
  }

  @Test
  internal fun `test that the correct workload ID is generated for syncs`() {
    val connectionId = UUID.randomUUID()
    val jobId = 12345L
    val attemptNumber = 1

    val generatedWorkloadId = generator.generateSyncWorkloadId(connectionId, jobId, attemptNumber)
    assertEquals(
      "${connectionId}_${jobId}_${attemptNumber}_sync",
      generatedWorkloadId,
    )
  }
}
