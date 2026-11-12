/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher.diagnostics

import io.airbyte.api.client.model.generated.DataplaneDiagnosticKind
import io.airbyte.api.client.model.generated.DataplaneDiagnosticResult
import io.airbyte.api.client.model.generated.DataplaneDiagnosticStatus
import io.airbyte.workload.launcher.model.DataplaneConfig
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.util.UUID

internal class ScheduledDiagnosticProbeTest {
  private val dataplane =
    DataplaneConfig(
      dataplaneId = UUID.randomUUID(),
      dataplaneName = "dataplane",
      dataplaneEnabled = true,
      dataplaneGroupId = UUID.randomUUID(),
      dataplaneGroupName = "region",
      organizationId = UUID.randomUUID(),
    )

  @Test
  fun `has no results before the first run`() {
    assertEquals(emptyList<DataplaneDiagnosticResult>(), TestProbe().latestCompletedResults())
  }

  @Test
  fun `skips a run until the data plane is known`() {
    val probe = TestProbe()

    probe.run()

    assertEquals(emptyList<DataplaneDiagnosticResult>(), probe.latestCompletedResults())
  }

  @Test
  fun `keeps the results of the latest run`() {
    val probe = TestProbe()
    probe.onApplicationEvent(dataplane)

    probe.run()
    probe.run()

    assertEquals(listOf(result(dataplane.dataplaneId, 2)), probe.latestCompletedResults())
  }

  private class TestProbe : ScheduledDiagnosticProbe() {
    private var runs = 0

    fun run() {
      val dataplane = dataplane ?: return
      runs++
      publish(listOf(result(dataplane.dataplaneId, runs)))
    }
  }

  companion object {
    private fun result(
      targetId: UUID,
      run: Int,
    ) = DataplaneDiagnosticResult(
      kind = DataplaneDiagnosticKind.SECRET_STORAGE.value,
      targetId = targetId,
      status = DataplaneDiagnosticStatus.PASSED,
      checkedAt = run.toLong(),
    )
  }
}
