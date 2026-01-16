/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.services

import io.airbyte.featureflag.TestClient
import io.airbyte.workload.common.DefaultDeadlineValues
import io.airbyte.workload.repository.WorkloadQueueRepository
import io.airbyte.workload.repository.WorkloadRepository
import io.airbyte.workload.repository.domain.Workload
import io.airbyte.workload.repository.domain.WorkloadLabel
import io.airbyte.workload.repository.domain.WorkloadStatus
import io.airbyte.workload.repository.domain.WorkloadType
import io.airbyte.workload.signal.SignalSender
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.time.OffsetDateTime
import java.util.Optional
import java.util.UUID
import io.airbyte.config.WorkloadType as ConfigWorkloadType

class WorkloadServiceTest {
  private lateinit var workloadRepository: WorkloadRepository
  private lateinit var workloadQueueRepository: WorkloadQueueRepository
  private lateinit var signalSender: SignalSender
  private lateinit var workloadService: WorkloadService

  @BeforeEach
  fun setup() {
    workloadRepository = mockk()
    workloadQueueRepository = mockk()
    signalSender = mockk(relaxed = true)

    workloadService =
      WorkloadService(
        workloadRepository = workloadRepository,
        workloadQueueRepository = workloadQueueRepository,
        signalSender = signalSender,
        defaultDeadlineValues = DefaultDeadlineValues(),
        featureFlagClient = TestClient(emptyMap()),
        metricClient = mockk(relaxed = true),
      )
  }

  @Test
  fun `cancelling an unknown workload throws a NotFoundException`() {
    every { workloadRepository.cancel(defaultWorkloadId, any(), any()) } returns null
    every { workloadRepository.findById(defaultWorkloadId) } returns Optional.empty()
    assertThrows<NotFoundException> {
      workloadService.cancelWorkload(defaultWorkloadId, "oops", "not found")
    }
  }

  @Test
  fun `cancelling a workload successfully sends a signal and acks from the queue`() {
    val cancelledWorkload = defaultWorkload.copy(status = WorkloadStatus.CANCELLED)
    val reason = "cancellation reason"
    val source = "test"
    every { workloadRepository.cancel(defaultWorkloadId, reason, source) } returns cancelledWorkload
    every { workloadQueueRepository.ackWorkloadQueueItem(defaultWorkloadId) } returns Unit

    workloadService.cancelWorkload(workloadId = defaultWorkloadId, reason = reason, source = source)

    verify { signalSender.sendSignal(cancelledWorkload.type, signalInput) }
    verify { workloadQueueRepository.ackWorkloadQueueItem(defaultWorkloadId) }
  }

  @Test
  fun `cancelling a terminal workload doesn't send a signal nor ack from the queue`() {
    val failedWorkload = defaultWorkload.copy(status = WorkloadStatus.FAILURE)
    val reason = "cancellation reason"
    val source = "test"
    every { workloadRepository.cancel(defaultWorkloadId, reason, source) } returns null
    every { workloadRepository.findById(defaultWorkloadId) } returns Optional.of(failedWorkload)

    assertThrows<InvalidStatusTransitionException> {
      workloadService.cancelWorkload(workloadId = defaultWorkloadId, reason = reason, source = source)
    }

    verify(exactly = 0) { signalSender.sendSignal(any(), any()) }
    verify(exactly = 0) { workloadQueueRepository.ackWorkloadQueueItem(defaultWorkloadId) }
  }

  @Test
  fun `failing an unknown workload throws a NotFoundException`() {
    every { workloadRepository.fail(defaultWorkloadId, any(), any()) } returns null
    every { workloadRepository.findById(defaultWorkloadId) } returns Optional.empty()
    assertThrows<NotFoundException> {
      workloadService.failWorkload(defaultWorkloadId, "oops", "not found", null)
    }
  }

  @Test
  fun `failing a workload successfully sends a signal and acks from the queue`() {
    val failedWorkload = defaultWorkload.copy(status = WorkloadStatus.FAILURE)
    val reason = "failure reason"
    val source = "failure test"
    every { workloadRepository.fail(defaultWorkloadId, reason, source) } returns failedWorkload
    every { workloadQueueRepository.ackWorkloadQueueItem(defaultWorkloadId) } returns Unit

    workloadService.failWorkload(workloadId = defaultWorkloadId, reason = reason, source = source, dataplaneVersion = null)

    verify { signalSender.sendSignal(failedWorkload.type, signalInput) }
    verify { workloadQueueRepository.ackWorkloadQueueItem(defaultWorkloadId) }
  }

  @Test
  fun `failing a terminal workload doesn't send a signal nor ack from the queue`() {
    val successfulWorkload = defaultWorkload.copy(status = WorkloadStatus.SUCCESS)
    val reason = "failure reason"
    val source = "failure test"
    every { workloadRepository.fail(defaultWorkloadId, reason, source) } returns null
    every { workloadRepository.findById(defaultWorkloadId) } returns Optional.of(successfulWorkload)

    assertThrows<InvalidStatusTransitionException> {
      workloadService.failWorkload(workloadId = defaultWorkloadId, reason = reason, source = source, dataplaneVersion = null)
    }

    verify(exactly = 0) { signalSender.sendSignal(any(), any()) }
    verify(exactly = 0) { workloadQueueRepository.ackWorkloadQueueItem(defaultWorkloadId) }
  }

  @Test
  fun `heartbeat an unknown workload throws a NotFoundException`() {
    every { workloadRepository.heartbeat(defaultWorkloadId, any()) } returns 0
    every { workloadRepository.findById(defaultWorkloadId) } returns Optional.empty()
    assertThrows<NotFoundException> {
      workloadService.heartbeatWorkload(defaultWorkloadId, OffsetDateTime.now(), null)
    }
  }

  @Test
  fun `heartbeat a workload to running does nothing else`() {
    every { workloadRepository.heartbeat(defaultWorkloadId, any()) } returns 1

    workloadService.heartbeatWorkload(defaultWorkloadId, OffsetDateTime.now().plusMinutes(5), null)

    verify(exactly = 0) { signalSender.sendSignal(any(), any()) }
    verify(exactly = 0) { workloadQueueRepository.ackWorkloadQueueItem(any()) }
  }

  @Test
  fun `heartbeat a workload to running only throws when it fails`() {
    val failedWorkload = defaultWorkload.copy(status = WorkloadStatus.FAILURE)
    every { workloadRepository.heartbeat(defaultWorkloadId, any()) } returns 0
    every { workloadRepository.findById(defaultWorkloadId) } returns Optional.of(failedWorkload)

    assertThrows<InvalidStatusTransitionException> {
      workloadService.heartbeatWorkload(defaultWorkloadId, OffsetDateTime.now().plusMinutes(5), null)
    }

    verify(exactly = 0) { signalSender.sendSignal(any(), any()) }
    verify(exactly = 0) { workloadQueueRepository.ackWorkloadQueueItem(defaultWorkloadId) }
  }

  @Test
  fun `heartbeat a workload throws when workload is in CLAIMED state`() {
    val claimedWorkload = defaultWorkload.copy(status = WorkloadStatus.CLAIMED)
    every { workloadRepository.heartbeat(defaultWorkloadId, any()) } returns 0
    every { workloadRepository.findById(defaultWorkloadId) } returns Optional.of(claimedWorkload)

    assertThrows<InvalidStatusTransitionException> {
      workloadService.heartbeatWorkload(defaultWorkloadId, OffsetDateTime.now().plusMinutes(5), null)
    }

    verify(exactly = 0) { signalSender.sendSignal(any(), any()) }
    verify(exactly = 0) { workloadQueueRepository.ackWorkloadQueueItem(defaultWorkloadId) }
  }

  @Test
  fun `heartbeat a workload throws when workload is in LAUNCHED state`() {
    val launchedWorkload = defaultWorkload.copy(status = WorkloadStatus.LAUNCHED)
    every { workloadRepository.heartbeat(defaultWorkloadId, any()) } returns 0
    every { workloadRepository.findById(defaultWorkloadId) } returns Optional.of(launchedWorkload)

    assertThrows<InvalidStatusTransitionException> {
      workloadService.heartbeatWorkload(defaultWorkloadId, OffsetDateTime.now().plusMinutes(5), null)
    }

    verify(exactly = 0) { signalSender.sendSignal(any(), any()) }
    verify(exactly = 0) { workloadQueueRepository.ackWorkloadQueueItem(defaultWorkloadId) }
  }

  @Test
  fun `setting an unknown workload to launch throws a NotFoundException`() {
    every { workloadRepository.launch(defaultWorkloadId, any()) } returns null
    every { workloadRepository.findById(defaultWorkloadId) } returns Optional.empty()
    assertThrows<NotFoundException> {
      workloadService.launchWorkload(defaultWorkloadId, OffsetDateTime.now(), null)
    }
  }

  @Test
  fun `setting a workload to launched does nothing else`() {
    val launchedWorkload = defaultWorkload.copy(status = WorkloadStatus.LAUNCHED)
    every { workloadRepository.launch(defaultWorkloadId, any()) } returns launchedWorkload
    every { workloadQueueRepository.ackWorkloadQueueItem(defaultWorkloadId) } returns Unit

    workloadService.launchWorkload(defaultWorkloadId, OffsetDateTime.now().plusMinutes(5), null)

    verify(exactly = 0) { signalSender.sendSignal(any(), any()) }
    verify(exactly = 0) { workloadQueueRepository.ackWorkloadQueueItem(any()) }
  }

  @Test
  fun `setting a workload to launched only throws when it fails`() {
    val failedWorkload = defaultWorkload.copy(status = WorkloadStatus.FAILURE)
    every { workloadRepository.launch(defaultWorkloadId, any()) } returns null
    every { workloadRepository.findById(defaultWorkloadId) } returns Optional.of(failedWorkload)

    assertThrows<InvalidStatusTransitionException> {
      workloadService.launchWorkload(defaultWorkloadId, OffsetDateTime.now().plusMinutes(5), null)
    }

    verify(exactly = 0) { signalSender.sendSignal(any(), any()) }
    verify(exactly = 0) { workloadQueueRepository.ackWorkloadQueueItem(defaultWorkloadId) }
  }

  @Test
  fun `setting an unknown workload to running throws a NotFoundException`() {
    every { workloadRepository.running(defaultWorkloadId, any()) } returns null
    every { workloadRepository.findById(defaultWorkloadId) } returns Optional.empty()
    assertThrows<NotFoundException> {
      workloadService.runningWorkload(defaultWorkloadId, OffsetDateTime.now(), null)
    }
  }

  @Test
  fun `setting a workload to running does nothing else`() {
    val launchedWorkload = defaultWorkload.copy(status = WorkloadStatus.RUNNING)
    every { workloadRepository.running(defaultWorkloadId, any()) } returns launchedWorkload
    every { workloadQueueRepository.ackWorkloadQueueItem(defaultWorkloadId) } returns Unit

    workloadService.runningWorkload(defaultWorkloadId, OffsetDateTime.now().plusMinutes(5), null)

    verify(exactly = 0) { signalSender.sendSignal(any(), any()) }
    verify(exactly = 0) { workloadQueueRepository.ackWorkloadQueueItem(any()) }
  }

  @Test
  fun `setting a workload to running only throws when it fails`() {
    val failedWorkload = defaultWorkload.copy(status = WorkloadStatus.FAILURE)
    every { workloadRepository.running(defaultWorkloadId, any()) } returns null
    every { workloadRepository.findById(defaultWorkloadId) } returns Optional.of(failedWorkload)

    assertThrows<InvalidStatusTransitionException> {
      workloadService.runningWorkload(defaultWorkloadId, OffsetDateTime.now().plusMinutes(5), null)
    }

    verify(exactly = 0) { signalSender.sendSignal(any(), any()) }
    verify(exactly = 0) { workloadQueueRepository.ackWorkloadQueueItem(defaultWorkloadId) }
  }

  @Test
  fun `succeeding an unknown workload throws a NotFoundException`() {
    every { workloadRepository.succeed(defaultWorkloadId) } returns null
    every { workloadRepository.findById(defaultWorkloadId) } returns Optional.empty()
    assertThrows<NotFoundException> {
      workloadService.succeedWorkload(defaultWorkloadId, null)
    }
  }

  @Test
  fun `succeeding a workload successfully sends a signal and acks from the queue`() {
    val failedWorkload = defaultWorkload.copy(status = WorkloadStatus.SUCCESS)
    every { workloadRepository.succeed(defaultWorkloadId) } returns failedWorkload
    every { workloadQueueRepository.ackWorkloadQueueItem(defaultWorkloadId) } returns Unit

    workloadService.succeedWorkload(defaultWorkloadId, null)

    verify { signalSender.sendSignal(failedWorkload.type, signalInput) }
    verify { workloadQueueRepository.ackWorkloadQueueItem(defaultWorkloadId) }
  }

  @Test
  fun `succeeding a terminal workload doesn't send a signal nor ack from the queue`() {
    val failedWorkload = defaultWorkload.copy(status = WorkloadStatus.FAILURE)
    every { workloadRepository.succeed(defaultWorkloadId) } returns null
    every { workloadRepository.findById(defaultWorkloadId) } returns Optional.of(failedWorkload)

    assertThrows<InvalidStatusTransitionException> {
      workloadService.succeedWorkload(workloadId = defaultWorkloadId, null)
    }

    verify(exactly = 0) { signalSender.sendSignal(any(), any()) }
    verify(exactly = 0) { workloadQueueRepository.ackWorkloadQueueItem(defaultWorkloadId) }
  }

  @Test
  fun `createWorkload populates both workloadLabels and labels fields for dual-write`() {
    val workloadId = "test-workload-${UUID.randomUUID()}"
    val labels =
      listOf(
        WorkloadLabel(key = "job_id", value = "123", workload = null),
        WorkloadLabel(key = "attempt_id", value = "1", workload = null),
        WorkloadLabel(key = "connection_id", value = "conn-456", workload = null),
      )
    val input = "test-input"
    val logPath = "/test/log"
    val mutexKey = "test-mutex"
    val autoId = UUID.randomUUID()

    every { workloadRepository.existsById(workloadId) } returns false
    every { workloadRepository.save(any()) } answers { firstArg() }
    every { workloadRepository.searchByMutexKeyAndStatusInList(any(), any()) } returns emptyList()

    workloadService.createWorkload(
      workloadId = workloadId,
      labels = labels,
      input = input,
      workspaceId = workspaceId,
      organizationId = organizationId,
      logPath = logPath,
      mutexKey = mutexKey,
      type = ConfigWorkloadType.SYNC,
      autoId = autoId,
      deadline = null,
      signalInput = null,
      dataplaneGroup = null,
      priority = null,
    )

    // Verify that workloadRepository.save was called with BOTH fields populated
    verify {
      workloadRepository.save(
        match { workload ->
          // Verify workloadLabels field (legacy) is populated
          workload.workloadLabels == labels &&
            // Verify labels field (new JSONB) is populated with the converted map
            workload.labels ==
            mapOf(
              "job_id" to "123",
              "attempt_id" to "1",
              "connection_id" to "conn-456",
            )
        },
      )
    }
  }

  companion object Fixtures {
    val defaultWorkloadId = UUID.randomUUID().toString()
    val signalInput = "signal-input"
    val workspaceId = UUID.randomUUID()
    val organizationId = UUID.randomUUID()
    val defaultWorkload =
      Workload(
        id = defaultWorkloadId,
        dataplaneId = null,
        status = WorkloadStatus.PENDING,
        logPath = "log path",
        inputPayload = "payload",
        workspaceId = workspaceId,
        organizationId = organizationId,
        mutexKey = "mutex",
        signalInput = signalInput,
        type = WorkloadType.SYNC,
        workloadLabels = null,
      )
  }
}
