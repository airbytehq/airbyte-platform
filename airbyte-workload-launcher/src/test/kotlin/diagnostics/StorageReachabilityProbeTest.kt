/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher.diagnostics

import io.airbyte.api.client.model.generated.DataplaneDiagnosticKind
import io.airbyte.api.client.model.generated.DataplaneDiagnosticResult
import io.airbyte.api.client.model.generated.DataplaneDiagnosticStatus
import io.airbyte.workload.launcher.model.DataplaneConfig
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.time.Duration
import java.util.UUID
import java.util.concurrent.CountDownLatch
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors

internal class StorageReachabilityProbeTest {
  private val dataplane =
    DataplaneConfig(
      dataplaneId = UUID.randomUUID(),
      dataplaneName = "dataplane",
      dataplaneEnabled = true,
      dataplaneGroupId = UUID.randomUUID(),
      dataplaneGroupName = "region",
      organizationId = UUID.randomUUID(),
    )

  private lateinit var checker: StorageReachabilityChecker
  private lateinit var executor: ExecutorService

  @BeforeEach
  fun setup() {
    checker = mockk()
    executor = Executors.newCachedThreadPool()
    every { checker.check(dataplane.dataplaneId) } returns result(DataplaneDiagnosticStatus.PASSED)
    every { checker.timedOut(dataplane.dataplaneId) } returns result(DataplaneDiagnosticStatus.UNABLE_TO_COMPLETE)
  }

  @AfterEach
  fun tearDown() {
    executor.shutdownNow()
  }

  @Test
  fun `keeps the check's result for the heartbeat`() {
    val probe = probe()
    probe.onApplicationEvent(dataplane)

    probe.probe()

    assertEquals(listOf(result(DataplaneDiagnosticStatus.PASSED)), probe.latestCompletedResults())
  }

  @Test
  fun `a stalled check times out`() {
    val release = CountDownLatch(1)
    every { checker.check(dataplane.dataplaneId) } answers {
      release.await()
      result(DataplaneDiagnosticStatus.PASSED)
    }
    val probe = probe(checkTimeout = Duration.ofMillis(200))
    probe.onApplicationEvent(dataplane)

    try {
      probe.probe()
    } finally {
      release.countDown()
    }

    assertEquals(listOf(result(DataplaneDiagnosticStatus.UNABLE_TO_COMPLETE)), probe.latestCompletedResults())
  }

  @Test
  fun `does nothing before the data plane is known`() {
    val probe = probe()

    probe.probe()

    assertEquals(emptyList<DataplaneDiagnosticResult>(), probe.latestCompletedResults())
    verify(exactly = 0) { checker.check(any()) }
  }

  private fun probe(checkTimeout: Duration = Duration.ofSeconds(5)) = StorageReachabilityProbe(checker, executor, checkTimeout)

  private fun result(status: DataplaneDiagnosticStatus) =
    DataplaneDiagnosticResult(
      kind = DataplaneDiagnosticKind.STORAGE_REACHABILITY.value,
      targetId = dataplane.dataplaneId,
      status = status,
      checkedAt = 1_794_480_000_000L,
    )
}
