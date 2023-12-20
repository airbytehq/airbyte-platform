/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.workload

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import java.util.UUID
import java.util.stream.Stream

class WorkloadIdGeneratorTest {
  private val generator = WorkloadIdGenerator()

  @ParameterizedTest
  @MethodSource("checkWorkloadIdArgsMatrix")
  internal fun `test that the correct workload ID is generated for check`(
    actorId: UUID,
    jobId: String,
    attemptNumber: Int,
  ) {
    val generatedWorkloadId = generator.generateCheckWorkloadId(actorId, jobId, attemptNumber)
    assertEquals(
      "${actorId}_${jobId}_${attemptNumber}_check",
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

  companion object {
    @JvmStatic
    private fun checkWorkloadIdArgsMatrix(): Stream<Arguments> {
      return Stream.of(
        Arguments.of(UUID.randomUUID(), 12412431L.toString(), 1),
        Arguments.of(UUID.randomUUID(), "89127421", 2),
        Arguments.of(UUID.randomUUID(), UUID.randomUUID().toString(), 0),
        Arguments.of(UUID.randomUUID(), "any string really", 0),
      )
    }
  }
}
