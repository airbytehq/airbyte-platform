/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.workload

import io.airbyte.workload.api.client.model.generated.WorkloadType
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.EnumSource
import java.util.Locale
import java.util.UUID

class WorkloadIdGeneratorTest {
  @ParameterizedTest()
  @EnumSource(WorkloadType::class)
  internal fun `test that the correct workload ID is generated`(workloadType: WorkloadType) {
    val connectionId = UUID.randomUUID()
    val jobId = 12345L
    val attemptNumber = 1
    val generator = WorkloadIdGenerator()

    val generatedWorkloadId = generator.generate(connectionId, jobId, attemptNumber, workloadType.name)
    assertEquals(
      "${connectionId}_${jobId}_${attemptNumber}_${workloadType.name.lowercase(Locale.ENGLISH)}",
      generatedWorkloadId,
    )
  }
}
