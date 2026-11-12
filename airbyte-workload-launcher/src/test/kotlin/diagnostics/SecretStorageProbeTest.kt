/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher.diagnostics

import io.airbyte.api.client.model.generated.DataplaneDiagnosticKind
import io.airbyte.api.client.model.generated.DataplaneDiagnosticResult
import io.airbyte.api.client.model.generated.DataplaneDiagnosticStatus
import io.airbyte.api.client.model.generated.ScopeType
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

internal class SecretStorageProbeTest {
  private val organizationId = UUID.randomUUID()
  private val dataplane =
    DataplaneConfig(
      dataplaneId = UUID.randomUUID(),
      dataplaneName = "dataplane",
      dataplaneEnabled = true,
      dataplaneGroupId = UUID.randomUUID(),
      dataplaneGroupName = "region",
      organizationId = organizationId,
    )
  private val storeA = SecretStorageProbeTarget(UUID.randomUUID(), ScopeType.WORKSPACE, UUID.randomUUID())
  private val storeB = SecretStorageProbeTarget(UUID.randomUUID(), ScopeType.ORGANIZATION, organizationId)

  private lateinit var targets: SecretStorageProbeTargets
  private lateinit var checker: SecretStorageChecker
  private lateinit var executor: ExecutorService

  @BeforeEach
  fun setup() {
    targets = mockk()
    checker = mockk()
    executor = Executors.newCachedThreadPool()
    every { targets.current() } returns listOf(storeA, storeB)
    every { checker.check(any(), organizationId) } answers { result(firstArg(), DataplaneDiagnosticStatus.PASSED) }
    every { checker.timedOut(any()) } answers { result(firstArg(), DataplaneDiagnosticStatus.UNABLE_TO_COMPLETE) }
  }

  @AfterEach
  fun tearDown() {
    executor.shutdownNow()
  }

  @Test
  fun `checks every store and keeps the results for the heartbeat`() {
    val probe = probe()
    probe.onApplicationEvent(dataplane)

    probe.probe()

    assertEquals(
      listOf(result(storeA, DataplaneDiagnosticStatus.PASSED), result(storeB, DataplaneDiagnosticStatus.PASSED)),
      probe.latestCompletedResults(),
    )
  }

  @Test
  fun `a stalled store times out without holding back the others`() {
    val release = CountDownLatch(1)
    every { checker.check(storeA, organizationId) } answers {
      release.await()
      result(storeA, DataplaneDiagnosticStatus.PASSED)
    }
    val probe = probe(checkTimeout = Duration.ofMillis(200))
    probe.onApplicationEvent(dataplane)

    try {
      probe.probe()
    } finally {
      release.countDown()
    }

    assertEquals(
      listOf(result(storeA, DataplaneDiagnosticStatus.UNABLE_TO_COMPLETE), result(storeB, DataplaneDiagnosticStatus.PASSED)),
      probe.latestCompletedResults(),
    )
  }

  @Test
  fun `a later run replaces the earlier results`() {
    val probe = probe()
    probe.onApplicationEvent(dataplane)
    probe.probe()

    every { targets.current() } returns listOf(storeB)
    probe.probe()

    assertEquals(listOf(result(storeB, DataplaneDiagnosticStatus.PASSED)), probe.latestCompletedResults())
  }

  @Test
  fun `no stores reports not configured`() {
    val notConfigured =
      DataplaneDiagnosticResult(
        kind = DataplaneDiagnosticKind.SECRET_STORAGE,
        targetId = dataplane.dataplaneId,
        status = DataplaneDiagnosticStatus.NOT_CONFIGURED,
        checkedAt = 1_793_750_400_000L,
      )
    every { targets.current() } returns emptyList()
    every { checker.notConfigured(dataplane.dataplaneId, organizationId) } returns notConfigured
    val probe = probe()
    probe.onApplicationEvent(dataplane)

    probe.probe()

    assertEquals(listOf(notConfigured), probe.latestCompletedResults())
  }

  @Test
  fun `does nothing before the data plane is known`() {
    val probe = probe()

    probe.probe()

    assertEquals(emptyList<DataplaneDiagnosticResult>(), probe.latestCompletedResults())
    verify(exactly = 0) { targets.current() }
  }

  @Test
  fun `does nothing without an organization`() {
    val probe = probe()
    probe.onApplicationEvent(dataplane.copy(organizationId = null))

    probe.probe()

    assertEquals(emptyList<DataplaneDiagnosticResult>(), probe.latestCompletedResults())
    verify(exactly = 0) { targets.current() }
  }

  private fun probe(checkTimeout: Duration = Duration.ofSeconds(5)) = SecretStorageProbe(targets, checker, executor, checkTimeout)

  private fun result(
    target: SecretStorageProbeTarget,
    status: DataplaneDiagnosticStatus,
  ) = DataplaneDiagnosticResult(
    kind = DataplaneDiagnosticKind.SECRET_STORAGE,
    targetId = target.secretStorageId,
    status = status,
    checkedAt = 1_793_750_400_000L,
  )
}
