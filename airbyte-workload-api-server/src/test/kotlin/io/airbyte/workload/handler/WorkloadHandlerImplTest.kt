/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.handler

import io.airbyte.api.client.AirbyteApiClient
import io.airbyte.api.client.generated.SignalApi
import io.airbyte.api.client.model.generated.SignalInput
import io.airbyte.commons.json.Jsons
import io.airbyte.config.SignalInput.Companion.SYNC_WORKFLOW
import io.airbyte.config.WorkloadPriority
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.metrics.MetricAttribute
import io.airbyte.metrics.MetricClient
import io.airbyte.metrics.OssMetricsRegistry
import io.airbyte.metrics.lib.MetricTags
import io.airbyte.workload.api.domain.WorkloadLabel
import io.airbyte.workload.errors.ConflictException
import io.airbyte.workload.errors.InvalidStatusTransitionException
import io.airbyte.workload.errors.NotFoundException
import io.airbyte.workload.handler.WorkloadHandlerImplTest.Fixtures.DATAPLANE_ID
import io.airbyte.workload.handler.WorkloadHandlerImplTest.Fixtures.WORKLOAD_ID
import io.airbyte.workload.handler.WorkloadHandlerImplTest.Fixtures.metricClient
import io.airbyte.workload.handler.WorkloadHandlerImplTest.Fixtures.mockApi
import io.airbyte.workload.handler.WorkloadHandlerImplTest.Fixtures.mockApiFailingSignal
import io.airbyte.workload.handler.WorkloadHandlerImplTest.Fixtures.signalApi
import io.airbyte.workload.handler.WorkloadHandlerImplTest.Fixtures.verifyApi
import io.airbyte.workload.handler.WorkloadHandlerImplTest.Fixtures.verifyFailedSignal
import io.airbyte.workload.handler.WorkloadHandlerImplTest.Fixtures.workloadHandler
import io.airbyte.workload.handler.WorkloadHandlerImplTest.Fixtures.workloadRepository
import io.airbyte.workload.repository.WorkloadRepository
import io.airbyte.workload.repository.domain.Workload
import io.airbyte.workload.repository.domain.WorkloadQueueStats
import io.airbyte.workload.repository.domain.WorkloadStatus
import io.airbyte.workload.repository.domain.WorkloadType
import io.micrometer.core.instrument.Counter
import io.mockk.Called
import io.mockk.Runs
import io.mockk.clearAllMocks
import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import io.mockk.spyk
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.EnumSource
import org.junit.jupiter.params.provider.MethodSource
import java.time.OffsetDateTime
import java.util.Optional
import java.util.UUID
import io.airbyte.config.SignalInput as ConfigSignalInput

class WorkloadHandlerImplTest {
  private val now: OffsetDateTime = OffsetDateTime.now()

  @BeforeEach
  fun reset() {
    clearAllMocks()
    every { workloadHandler.offsetDateTime() }.returns(now)
  }

  @Test
  fun `verify active statuses doesn't contain terminal statuses`() {
    assertEquals(
      setOf(WorkloadStatus.PENDING, WorkloadStatus.CLAIMED, WorkloadStatus.LAUNCHED, WorkloadStatus.RUNNING),
      WorkloadHandlerImpl.ACTIVE_STATUSES.toSet(),
    )
    assertFalse(WorkloadHandlerImpl.ACTIVE_STATUSES.contains(WorkloadStatus.CANCELLED))
    assertFalse(WorkloadHandlerImpl.ACTIVE_STATUSES.contains(WorkloadStatus.FAILURE))
    assertFalse(WorkloadHandlerImpl.ACTIVE_STATUSES.contains(WorkloadStatus.SUCCESS))
  }

  @Test
  fun `getWorkload returns the expected workload`() {
    val domainWorkload =
      Workload(
        id = WORKLOAD_ID,
        dataplaneId = null,
        status = WorkloadStatus.PENDING,
        workloadLabels = null,
        inputPayload = "",
        logPath = "/",
        mutexKey = null,
        type = WorkloadType.SYNC,
        autoId = UUID.randomUUID(),
        signalInput = "",
      )

    every { workloadRepository.findById(WORKLOAD_ID) }.returns(Optional.of(domainWorkload))
    val apiWorkload = workloadHandler.getWorkload(WORKLOAD_ID)
    assertEquals(domainWorkload.id, apiWorkload.id)
  }

  @Test
  fun `getWorkload throws NotFoundException if the workload isn't found`() {
    every { workloadRepository.findById(WORKLOAD_ID) }.returns(Optional.empty())
    assertThrows<NotFoundException> { workloadHandler.getWorkload(WORKLOAD_ID) }
  }

  @Test
  fun `createWorkload saves the expected workload`() {
    val workloadLabel1 = WorkloadLabel("key1", "value1")
    val workloadLabel2 = WorkloadLabel("key2", "value2")
    val workloadLabels = mutableListOf(workloadLabel1, workloadLabel2)

    every { workloadRepository.existsById(WORKLOAD_ID) }.returns(false)
    every { workloadRepository.searchByMutexKeyAndStatusInList("mutex-this", WorkloadHandlerImpl.ACTIVE_STATUSES) }.returns(listOf())
    every { workloadRepository.save(any()) }.returns(
      Fixtures.workload(),
    )
    workloadHandler.createWorkload(
      WORKLOAD_ID,
      workloadLabels,
      "input payload",
      "/log/path",
      "mutex-this",
      io.airbyte.config.WorkloadType.SYNC,
      UUID.randomUUID(),
      now.plusHours(2),
      signalInput = "signal payload",
      "queue-name-1",
      WorkloadPriority.DEFAULT,
    )
    verify {
      workloadRepository.save(
        match {
          it.id == WORKLOAD_ID &&
            it.dataplaneId == null &&
            it.status == WorkloadStatus.PENDING &&
            it.lastHeartbeatAt == null &&
            it.workloadLabels!![0].key == workloadLabel1.key &&
            it.workloadLabels!![0].value == workloadLabel1.value &&
            it.workloadLabels!![1].key == workloadLabel2.key &&
            it.workloadLabels!![1].value == workloadLabel2.value &&
            it.inputPayload == "input payload" &&
            it.logPath == "/log/path" &&
            it.mutexKey == "mutex-this" &&
            it.type == WorkloadType.SYNC &&
            it.deadline!! == now.plusHours(2) &&
            it.signalInput == "signal payload" &&
            it.dataplaneGroup == "queue-name-1" &&
            it.priority == 0
        },
      )
    }
  }

  @Test
  fun `test create workload id conflict`() {
    every { workloadRepository.existsById(WORKLOAD_ID) }.returns(true)
    assertThrows<ConflictException> {
      workloadHandler.createWorkload(
        WORKLOAD_ID,
        null,
        "",
        "",
        "mutex-this",
        io.airbyte.config.WorkloadType.SYNC,
        UUID.randomUUID(),
        now,
        "",
        "queue-name-1",
        WorkloadPriority.DEFAULT,
      )
    }
  }

  @Test
  fun `test create workload mutex conflict`() {
    val workloadIdWithSuccessfulFail = "workload-id-with-successful-fail"
    val workloadIdWithFailedFail = "workload-id-with-failed-fail"
    val duplWorkloads =
      listOf(
        Fixtures.workload(workloadIdWithSuccessfulFail, createdAt = OffsetDateTime.now().minusSeconds(5)),
        Fixtures.workload(workloadIdWithFailedFail, createdAt = OffsetDateTime.now().minusSeconds(10)),
      )
    val newWorkload = Fixtures.workload(WORKLOAD_ID)
    every { workloadRepository.existsById(WORKLOAD_ID) }.returns(false)
    every {
      workloadHandler.failWorkload(workloadIdWithSuccessfulFail, any(), any())
    }.answers {}
    every {
      workloadHandler.failWorkload(workloadIdWithFailedFail, any(), any())
    }.throws(InvalidStatusTransitionException(workloadIdWithFailedFail))
    every { workloadRepository.save(any()) }.returns(newWorkload)
    every {
      workloadRepository.searchByMutexKeyAndStatusInList(
        "mutex-this",
        WorkloadHandlerImpl.ACTIVE_STATUSES,
      )
    }.returns(duplWorkloads + listOf(newWorkload))

    workloadHandler.createWorkload(
      WORKLOAD_ID,
      null,
      "",
      "",
      "mutex-this",
      io.airbyte.config.WorkloadType.SYNC,
      UUID.randomUUID(),
      now,
      "",
      "queue-name-1",
      WorkloadPriority.DEFAULT,
    )
    verify {
      workloadHandler.failWorkload(workloadIdWithFailedFail, any(), any())
      workloadHandler.failWorkload(workloadIdWithSuccessfulFail, any(), any())
      workloadRepository.save(
        match {
          it.id == WORKLOAD_ID && it.mutexKey == "mutex-this"
        },
      )
    }
  }

  @Test
  fun `test get workloads`() {
    val domainWorkload =
      Fixtures.workload(
        id = WORKLOAD_ID,
        dataplaneId = null,
        status = WorkloadStatus.PENDING,
        workloadLabels = null,
        inputPayload = "a payload",
        logPath = "/log/path",
        mutexKey = "mutex-this",
        type = WorkloadType.DISCOVER,
        dataplaneGroup = "queue-name-1",
        priority = 0,
      )
    every { workloadRepository.search(any(), any(), any()) }.returns(listOf(domainWorkload))
    val workloads = workloadHandler.getWorkloads(listOf("dataplane1"), listOf(ApiWorkloadStatus.CLAIMED, ApiWorkloadStatus.FAILURE), null)
    assertEquals(1, workloads.size)
    assertEquals(WORKLOAD_ID, workloads[0].id)
    assertEquals("a payload", workloads[0].inputPayload)
    assertEquals("/log/path", workloads[0].logPath)
    assertEquals("mutex-this", workloads[0].mutexKey)
    assertEquals(io.airbyte.config.WorkloadType.DISCOVER, workloads[0].type)
    assertEquals("queue-name-1", workloads[0].dataplaneGroup)
    assertEquals(WorkloadPriority.DEFAULT, workloads[0].priority)
  }

  @ParameterizedTest
  @EnumSource(value = WorkloadStatus::class, names = ["CLAIMED", "LAUNCHED", "RUNNING"])
  fun `test successfulHeartbeat`(workloadStatus: WorkloadStatus) {
    every { workloadRepository.findById(WORKLOAD_ID) }.returns(
      Optional.of(
        Fixtures.workload(
          id = WORKLOAD_ID,
          status = workloadStatus,
        ),
      ),
    )
    every { workloadRepository.update(eq(WORKLOAD_ID), any(), ofType(OffsetDateTime::class), eq(now.plusMinutes(10))) }.returns(Unit)
    workloadHandler.heartbeat(WORKLOAD_ID, now.plusMinutes(10))
    verify { workloadRepository.update(eq(WORKLOAD_ID), eq(WorkloadStatus.RUNNING), any(), eq(now.plusMinutes(10))) }
  }

  @ParameterizedTest
  @EnumSource(value = WorkloadStatus::class, names = ["CANCELLED", "FAILURE", "SUCCESS", "PENDING"])
  fun `test nonAuthorizedHeartbeat`(workloadStatus: WorkloadStatus) {
    every { workloadRepository.findById(WORKLOAD_ID) }.returns(
      Optional.of(
        Fixtures.workload(
          id = WORKLOAD_ID,
          status = workloadStatus,
        ),
      ),
    )
    assertThrows<InvalidStatusTransitionException> { workloadHandler.heartbeat(WORKLOAD_ID, now) }
  }

  @Test
  fun `claiming a workload unsuccesfully returns false`() {
    every { workloadRepository.claim(WORKLOAD_ID, any(), any()) }.returns(null)
    assertFalse { workloadHandler.claimWorkload(WORKLOAD_ID, DATAPLANE_ID, now) }
  }

  @Test
  fun `claiming a workload successfully returns true`() {
    every { workloadRepository.claim(WORKLOAD_ID, DATAPLANE_ID, any()) }.returns(
      Fixtures.workload(
        id = WORKLOAD_ID,
        dataplaneId = DATAPLANE_ID,
        status = WorkloadStatus.CLAIMED,
      ),
    )
    assertTrue(workloadHandler.claimWorkload(WORKLOAD_ID, DATAPLANE_ID, now))
  }

  @Test
  fun `test claiming a workload successfully`() {
    every { workloadRepository.claim(WORKLOAD_ID, DATAPLANE_ID, any()) }.returns(
      Fixtures.workload(
        id = WORKLOAD_ID,
        status = WorkloadStatus.CLAIMED,
        dataplaneId = DATAPLANE_ID,
      ),
    )
    assertTrue(workloadHandler.claimWorkload(WORKLOAD_ID, DATAPLANE_ID, now))
  }

  @Test
  fun `test claiming a workload unsuccessfully`() {
    every { workloadRepository.claim(WORKLOAD_ID, DATAPLANE_ID, any()) }.returns(null)
    assertFalse(workloadHandler.claimWorkload(WORKLOAD_ID, DATAPLANE_ID, now))
  }

  @Test
  fun `test workload not found when cancelling workload`() {
    every { workloadRepository.findById(WORKLOAD_ID) }.returns(Optional.empty())
    assertThrows<NotFoundException> { workloadHandler.cancelWorkload(WORKLOAD_ID, "test", "test cancel") }
  }

  @ParameterizedTest
  @EnumSource(value = WorkloadStatus::class, names = ["SUCCESS", "FAILURE"])
  fun `test cancel workload in terminal state`(workloadStatus: WorkloadStatus) {
    every { workloadRepository.findById(WORKLOAD_ID) }.returns(
      Optional.of(
        Fixtures.workload(
          id = WORKLOAD_ID,
          status = workloadStatus,
        ),
      ),
    )

    assertThrows<InvalidStatusTransitionException> { workloadHandler.cancelWorkload(WORKLOAD_ID, "test", "invalid cancel") }
  }

  @ParameterizedTest
  @EnumSource(value = WorkloadStatus::class, names = ["CLAIMED", "LAUNCHED", "RUNNING", "PENDING"])
  fun `test successful cancel`(workloadStatus: WorkloadStatus) {
    every { workloadRepository.findById(WORKLOAD_ID) }.returns(
      Optional.of(
        Fixtures.workload(
          id = WORKLOAD_ID,
          status = workloadStatus,
          signalPayload = Jsons.serialize(Fixtures.configSignalInput),
        ),
      ),
    )

    every { workloadRepository.update(any(), ofType(WorkloadStatus::class), eq("test"), eq("test cancel"), null) } just Runs
    mockApi()

    workloadHandler.cancelWorkload(WORKLOAD_ID, "test", "test cancel")
    verify { workloadRepository.update(eq(WORKLOAD_ID), eq(WorkloadStatus.CANCELLED), eq("test"), eq("test cancel"), null) }
    verifyApi()
  }

  @ParameterizedTest
  @EnumSource(value = WorkloadStatus::class, names = ["CLAIMED", "LAUNCHED", "RUNNING", "PENDING"])
  fun `test successful cancel no signal`(workloadStatus: WorkloadStatus) {
    every { workloadRepository.findById(WORKLOAD_ID) }.returns(
      Optional.of(
        Fixtures.workload(
          id = WORKLOAD_ID,
          status = workloadStatus,
          signalPayload = null,
        ),
      ),
    )

    every { workloadRepository.update(any(), ofType(WorkloadStatus::class), eq("test"), eq("test cancel"), null) } just Runs
    mockApi()

    workloadHandler.cancelWorkload(WORKLOAD_ID, "test", "test cancel")
    verify { workloadRepository.update(eq(WORKLOAD_ID), eq(WorkloadStatus.CANCELLED), eq("test"), eq("test cancel"), null) }
    verify { signalApi wasNot Called }
  }

  @ParameterizedTest
  @EnumSource(value = WorkloadStatus::class, names = ["CLAIMED", "LAUNCHED", "RUNNING", "PENDING"])
  fun `test failed signal cancel`(workloadStatus: WorkloadStatus) {
    every { workloadRepository.findById(WORKLOAD_ID) }.returns(
      Optional.of(
        Fixtures.workload(
          id = WORKLOAD_ID,
          status = workloadStatus,
          signalPayload = Jsons.serialize(Fixtures.configSignalInput),
        ),
      ),
    )

    every { workloadRepository.update(any(), ofType(WorkloadStatus::class), eq("test"), eq("test cancel"), null) } just Runs
    mockApiFailingSignal()

    workloadHandler.cancelWorkload(WORKLOAD_ID, "test", "test cancel")
    verify { workloadRepository.update(eq(WORKLOAD_ID), eq(WorkloadStatus.CANCELLED), eq("test"), eq("test cancel"), null) }
    verifyFailedSignal()
  }

  @ParameterizedTest
  @EnumSource(value = WorkloadStatus::class, names = ["CLAIMED", "LAUNCHED", "RUNNING", "PENDING"])
  fun `test failed bad signal input cancel`(workloadStatus: WorkloadStatus) {
    every { workloadRepository.findById(WORKLOAD_ID) }.returns(
      Optional.of(
        Fixtures.workload(
          id = WORKLOAD_ID,
          status = workloadStatus,
          signalPayload = "Not a valid Json",
        ),
      ),
    )

    every { workloadRepository.update(any(), ofType(WorkloadStatus::class), eq("test"), eq("test cancel"), null) } just Runs
    every { metricClient.count(OssMetricsRegistry.WORKLOADS_SIGNAL, any(), any()) } returns mockk<Counter>()
    workloadHandler.cancelWorkload(WORKLOAD_ID, "test", "test cancel")
    verify { workloadRepository.update(eq(WORKLOAD_ID), eq(WorkloadStatus.CANCELLED), eq("test"), eq("test cancel"), null) }
    verify {
      metricClient.count(
        metric = OssMetricsRegistry.WORKLOADS_SIGNAL,
        value = 1,
        attributes =
          arrayOf(
            MetricAttribute(MetricTags.STATUS, MetricTags.FAILURE),
            MetricAttribute(MetricTags.FAILURE_TYPE, "deserialization"),
            any(),
          ),
      )
    }
  }

  @Test
  fun `test noop cancel`() {
    every { workloadRepository.findById(WORKLOAD_ID) }.returns(
      Optional.of(
        Fixtures.workload(
          id = WORKLOAD_ID,
          status = WorkloadStatus.CANCELLED,
        ),
      ),
    )

    workloadHandler.cancelWorkload(WORKLOAD_ID, "test", "test cancel again")
    verify(exactly = 0) { workloadRepository.update(eq(WORKLOAD_ID), eq(WorkloadStatus.CANCELLED), "test", "test cancel again", null) }
  }

  @Test
  fun `test workload not found when failing workload`() {
    every { workloadRepository.findById(WORKLOAD_ID) }.returns(Optional.empty())
    assertThrows<NotFoundException> { workloadHandler.failWorkload(WORKLOAD_ID, "test", "fail") }
  }

  @ParameterizedTest
  @EnumSource(value = WorkloadStatus::class, names = ["SUCCESS", "CANCELLED"])
  fun `test fail workload in inactive status`(workloadStatus: WorkloadStatus) {
    every { workloadRepository.findById(WORKLOAD_ID) }.returns(
      Optional.of(
        Fixtures.workload(
          id = WORKLOAD_ID,
          status = workloadStatus,
        ),
      ),
    )

    assertThrows<InvalidStatusTransitionException> { workloadHandler.failWorkload(WORKLOAD_ID, "test", "fail") }
  }

  @ParameterizedTest
  @EnumSource(value = WorkloadStatus::class, names = ["CLAIMED", "LAUNCHED", "RUNNING"])
  fun `test failing workload succeeded`(workloadStatus: WorkloadStatus) {
    every { workloadRepository.findById(WORKLOAD_ID) }.returns(
      Optional.of(
        Fixtures.workload(
          id = WORKLOAD_ID,
          status = workloadStatus,
          signalPayload = Jsons.serialize(Fixtures.configSignalInput),
        ),
      ),
    )

    every { workloadRepository.update(any(), ofType(WorkloadStatus::class), eq("test"), eq("failing a workload"), null) } just Runs
    mockApi()

    workloadHandler.failWorkload(WORKLOAD_ID, "test", "failing a workload")
    verify { workloadRepository.update(eq(WORKLOAD_ID), eq(WorkloadStatus.FAILURE), eq("test"), eq("failing a workload"), null) }
    verifyApi()
  }

  @Test
  fun `test noop failure`() {
    every { workloadRepository.findById(WORKLOAD_ID) }.returns(
      Optional.of(
        Fixtures.workload(
          id = WORKLOAD_ID,
          status = WorkloadStatus.FAILURE,
        ),
      ),
    )

    workloadHandler.failWorkload(WORKLOAD_ID, "test", "noop")
    verify(exactly = 0) { workloadRepository.update(eq(WORKLOAD_ID), eq(WorkloadStatus.FAILURE), eq("test"), eq("noop"), null) }
  }

  @Test
  fun `test workload not found when succeeding workload`() {
    every { workloadRepository.findById(WORKLOAD_ID) }.returns(Optional.empty())
    assertThrows<NotFoundException> { workloadHandler.succeedWorkload(WORKLOAD_ID) }
  }

  @ParameterizedTest
  @EnumSource(value = WorkloadStatus::class, names = ["PENDING", "CANCELLED", "FAILURE"])
  fun `test succeed workload in inactive status`(workloadStatus: WorkloadStatus) {
    every { workloadRepository.findById(WORKLOAD_ID) }.returns(
      Optional.of(
        Fixtures.workload(
          id = WORKLOAD_ID,
          status = workloadStatus,
        ),
      ),
    )

    assertThrows<InvalidStatusTransitionException> { workloadHandler.succeedWorkload(WORKLOAD_ID) }
  }

  @ParameterizedTest
  @EnumSource(value = WorkloadStatus::class, names = ["CLAIMED", "LAUNCHED", "RUNNING"])
  fun `test succeeding workload succeeded`(workloadStatus: WorkloadStatus) {
    every { workloadRepository.findById(WORKLOAD_ID) }.returns(
      Optional.of(
        Fixtures.workload(
          id = WORKLOAD_ID,
          status = workloadStatus,
          signalPayload = Jsons.serialize(Fixtures.configSignalInput),
        ),
      ),
    )

    every { workloadRepository.update(any(), ofType(WorkloadStatus::class), null) } just Runs
    mockApi()

    workloadHandler.succeedWorkload(WORKLOAD_ID)
    verify { workloadRepository.update(eq(WORKLOAD_ID), eq(WorkloadStatus.SUCCESS), null) }
    verifyApi()
  }

  @Test
  fun `test noop success`() {
    every { workloadRepository.findById(WORKLOAD_ID) }.returns(
      Optional.of(
        Fixtures.workload(
          id = WORKLOAD_ID,
          status = WorkloadStatus.SUCCESS,
        ),
      ),
    )

    workloadHandler.succeedWorkload(WORKLOAD_ID)
    verify(exactly = 0) { workloadRepository.update(eq(WORKLOAD_ID), eq(WorkloadStatus.SUCCESS), null) }
  }

  @Test
  fun `test workload not found when setting status to running`() {
    every { workloadRepository.findById(WORKLOAD_ID) }.returns(Optional.empty())
    assertThrows<NotFoundException> { workloadHandler.setWorkloadStatusToRunning(WORKLOAD_ID, now) }
  }

  @ParameterizedTest
  @EnumSource(value = WorkloadStatus::class, names = ["SUCCESS", "CANCELLED", "FAILURE"])
  fun `test set workload status to running when workload is in terminal state`(workloadStatus: WorkloadStatus) {
    every { workloadRepository.findById(WORKLOAD_ID) }.returns(
      Optional.of(
        Fixtures.workload(
          id = WORKLOAD_ID,
          status = workloadStatus,
        ),
      ),
    )

    assertThrows<InvalidStatusTransitionException> { workloadHandler.setWorkloadStatusToRunning(WORKLOAD_ID, now) }
  }

  @Test
  fun `test set workload status to running on unclaimed workload`() {
    every { workloadRepository.findById(WORKLOAD_ID) }.returns(
      Optional.of(
        Fixtures.workload(
          id = WORKLOAD_ID,
          dataplaneId = null,
          status = WorkloadStatus.PENDING,
        ),
      ),
    )

    assertThrows<InvalidStatusTransitionException> { workloadHandler.setWorkloadStatusToRunning(WORKLOAD_ID, now) }
  }

  @ParameterizedTest
  @EnumSource(value = WorkloadStatus::class, names = ["CLAIMED", "LAUNCHED"])
  fun `test set workload status to running succeeded`(workloadStatus: WorkloadStatus) {
    every { workloadRepository.findById(WORKLOAD_ID) }.returns(
      Optional.of(
        Fixtures.workload(
          id = WORKLOAD_ID,
          status = workloadStatus,
        ),
      ),
    )

    every { workloadRepository.update(any(), ofType(WorkloadStatus::class), any()) } just Runs

    workloadHandler.setWorkloadStatusToRunning(WORKLOAD_ID, now.plusMinutes(10))
    verify { workloadRepository.update(eq(WORKLOAD_ID), eq(WorkloadStatus.RUNNING), eq(now.plusMinutes(10))) }
  }

  @Test
  fun `test noop when setting workload status to running`() {
    every { workloadRepository.findById(WORKLOAD_ID) }.returns(
      Optional.of(
        Fixtures.workload(
          id = WORKLOAD_ID,
          status = WorkloadStatus.RUNNING,
        ),
      ),
    )

    workloadHandler.setWorkloadStatusToRunning(WORKLOAD_ID, now.plusMinutes(10))
    verify(exactly = 0) { workloadRepository.update(eq(WORKLOAD_ID), eq(WorkloadStatus.RUNNING), eq(now.plusMinutes(10))) }
  }

  @Test
  fun `test workload not found when setting status to launched`() {
    every { workloadRepository.findById(WORKLOAD_ID) }.returns(Optional.empty())
    assertThrows<NotFoundException> { workloadHandler.setWorkloadStatusToLaunched(WORKLOAD_ID, now) }
  }

  @ParameterizedTest
  @EnumSource(value = WorkloadStatus::class, names = ["PENDING", "SUCCESS", "CANCELLED", "FAILURE"])
  fun `test set workload status to launched when is not in claimed state`(workloadStatus: WorkloadStatus) {
    every { workloadRepository.findById(WORKLOAD_ID) }.returns(
      Optional.of(
        Fixtures.workload(
          id = WORKLOAD_ID,
          status = workloadStatus,
        ),
      ),
    )

    assertThrows<InvalidStatusTransitionException> { workloadHandler.setWorkloadStatusToLaunched(WORKLOAD_ID, now) }
  }

  @Test
  fun `test set workload status to launched succeeded`() {
    every { workloadRepository.findById(WORKLOAD_ID) }.returns(
      Optional.of(
        Fixtures.workload(
          id = WORKLOAD_ID,
          status = WorkloadStatus.CLAIMED,
        ),
      ),
    )

    every { workloadRepository.update(any(), ofType(WorkloadStatus::class), any()) } just Runs

    workloadHandler.setWorkloadStatusToLaunched(WORKLOAD_ID, now.plusMinutes(10))
    verify { workloadRepository.update(eq(WORKLOAD_ID), eq(WorkloadStatus.LAUNCHED), eq(now.plusMinutes(10))) }
  }

  @ParameterizedTest
  @EnumSource(value = WorkloadStatus::class, names = ["LAUNCHED", "RUNNING"])
  fun `test set workload status to launched is a noop`(workloadStatus: WorkloadStatus) {
    every { workloadRepository.findById(WORKLOAD_ID) }.returns(
      Optional.of(
        Fixtures.workload(
          id = WORKLOAD_ID,
          status = workloadStatus,
        ),
      ),
    )

    every { workloadRepository.update(any(), ofType(WorkloadStatus::class), any()) } just Runs

    workloadHandler.setWorkloadStatusToLaunched(WORKLOAD_ID, now.plusMinutes(10))
    verify(exactly = 0) { workloadRepository.update(eq(WORKLOAD_ID), eq(WorkloadStatus.LAUNCHED), eq(now.plusMinutes(10))) }
  }

  @Test
  fun `test noop when setting workload status to launched`() {
    every { workloadRepository.findById(WORKLOAD_ID) }.returns(
      Optional.of(
        Fixtures.workload(
          id = WORKLOAD_ID,
          status = WorkloadStatus.LAUNCHED,
        ),
      ),
    )

    workloadHandler.setWorkloadStatusToLaunched(WORKLOAD_ID, now.plusMinutes(10))
    verify(exactly = 0) { workloadRepository.update(eq(WORKLOAD_ID), eq(WorkloadStatus.LAUNCHED), eq(now.plusMinutes(10))) }
  }

  @Test
  fun `test get workload running before`() {
    every { workloadRepository.searchByTypeStatusAndCreationDate(any(), eq(listOf(WorkloadStatus.RUNNING)), any(), any()) }
      .returns(listOf())
    val dataplaneIds = listOf("dataplaneId")
    val workloadTypes = listOf(ApiWorkloadType.CHECK)
    val createdAt = OffsetDateTime.now()

    workloadHandler.getWorkloadsRunningCreatedBefore(dataplaneIds, workloadTypes, createdAt)

    verify {
      workloadRepository.searchByTypeStatusAndCreationDate(
        dataplaneIds,
        listOf(WorkloadStatus.RUNNING),
        listOf(WorkloadType.CHECK),
        createdAt,
      )
    }
  }

  @Test
  fun `offsetDateTime method should always return current time`() {
    val workloadHandlerImpl =
      WorkloadHandlerImpl(
        mockk<WorkloadRepository>(),
        mockk<AirbyteApiClient>(),
        mockk<MetricClient>(),
        mockk<FeatureFlagClient>(),
      )
    val offsetDateTime = workloadHandlerImpl.offsetDateTime()
    Thread.sleep(10)
    val offsetDateTimeAfter10Ms = workloadHandlerImpl.offsetDateTime()
    assertTrue(offsetDateTimeAfter10Ms.isAfter(offsetDateTime))
  }

  @ParameterizedTest
  @MethodSource("getPendingWorkloadMatrix")
  fun `poll workloads returns pending workloads`(
    group: String,
    priority: Int,
    domainWorkloads: List<Workload>,
  ) {
    every { workloadRepository.getPendingWorkloads(group, priority, 10) }.returns(domainWorkloads)
    val result = workloadHandler.pollWorkloadQueue(group, WorkloadPriority.fromInt(priority), 10)
    val expected = domainWorkloads.map { it.toApi() }

    assertEquals(expected, result)
  }

  @ParameterizedTest
  @MethodSource("countPendingWorkloadMatrix")
  fun `count workload queue depth returns count of pending workloads`(
    group: String,
    priority: Int,
    count: Long,
  ) {
    every { workloadRepository.countPendingWorkloads(group, priority) }.returns(count)
    val result = workloadHandler.countWorkloadQueueDepth(group, WorkloadPriority.fromInt(priority))

    assertEquals(count, result)
  }

  @ParameterizedTest
  @MethodSource("workloadStatsMatrix")
  fun `get workload queue stats returns stats with enqueued workloads for each logical queue (dataplane group x priority)`(
    stats: List<WorkloadQueueStats>,
  ) {
    every { workloadRepository.getPendingWorkloadQueueStats() }.returns(stats)
    val result = workloadHandler.getWorkloadQueueStats()
    val expected = stats.map { it.toApi() }

    assertEquals(expected, result)
  }

  object Fixtures {
    val workloadRepository = mockk<WorkloadRepository>()
    val metricClient: MetricClient = mockk(relaxed = true)
    private val airbyteApi: AirbyteApiClient = mockk()
    val featureFlagClient: FeatureFlagClient = mockk(relaxed = true)
    val signalApi: SignalApi = mockk()
    const val WORKLOAD_ID = "test"
    const val DATAPLANE_ID = "dataplaneId"
    val workloadHandler = spyk(WorkloadHandlerImpl(workloadRepository, airbyteApi, metricClient, featureFlagClient))

    val configSignalInput =
      ConfigSignalInput(
        workflowType = SYNC_WORKFLOW,
        workflowId = "workflowId",
      )

    val signalInput =
      SignalInput(
        workflowType = configSignalInput.workflowType,
        workflowId = configSignalInput.workflowId,
      )

    fun mockApi() {
      every { airbyteApi.signalApi } returns signalApi
      every { signalApi.signal(signalInput) } returns Unit
    }

    fun verifyApi() {
      verify { signalApi.signal(signalInput) }
    }

    fun mockApiFailingSignal() {
      every { airbyteApi.signalApi } returns signalApi
      every { signalApi.signal(signalInput) } throws Exception("Failed to signal")
    }

    fun verifyFailedSignal() {
      verify {
        metricClient.count(
          metric = OssMetricsRegistry.WORKLOADS_SIGNAL,
          value = 1,
          attributes =
            arrayOf(
              MetricAttribute(MetricTags.WORKFLOW_TYPE, signalInput.workflowType),
              any(),
              MetricAttribute(MetricTags.STATUS, MetricTags.FAILURE),
              any(),
            ),
        )
      }
    }

    fun workload(
      id: String = WORKLOAD_ID,
      dataplaneId: String? = null,
      status: WorkloadStatus = WorkloadStatus.PENDING,
      workloadLabels: List<io.airbyte.workload.repository.domain.WorkloadLabel>? = listOf(),
      inputPayload: String = "",
      logPath: String = "/",
      mutexKey: String = "",
      type: WorkloadType = WorkloadType.SYNC,
      createdAt: OffsetDateTime = OffsetDateTime.now(),
      signalPayload: String? = "",
      dataplaneGroup: String? = "",
      priority: Int? = 0,
    ): Workload =
      Workload(
        id = id,
        dataplaneId = dataplaneId,
        status = status,
        workloadLabels = workloadLabels,
        inputPayload = inputPayload,
        logPath = logPath,
        mutexKey = mutexKey,
        type = type,
        createdAt = createdAt,
        signalInput = signalPayload,
        dataplaneGroup = dataplaneGroup,
        priority = priority,
      )
  }

  companion object {
    @JvmStatic
    fun getPendingWorkloadMatrix(): List<Arguments> =
      listOf(
        Arguments.of("group-1", 0, listOf(Fixtures.workload("1"), Fixtures.workload("2"), Fixtures.workload("3"))),
        Arguments.of("group-2", 1, listOf(Fixtures.workload("1"), Fixtures.workload("3"))),
        Arguments.of("group-3", 0, listOf(Fixtures.workload("4"))),
        Arguments.of("group-1", 1, listOf(Fixtures.workload("1"), Fixtures.workload("2"))),
      )

    @JvmStatic
    fun countPendingWorkloadMatrix(): List<Arguments> =
      listOf(
        Arguments.of("group-1", 0, 10),
        Arguments.of("group-2", 1, 9),
        Arguments.of("group-3", 0, 0),
        Arguments.of("group-1", 1, 124124124),
      )

    @JvmStatic
    fun workloadStatsMatrix(): List<Arguments> =
      listOf(
        Arguments.of(listOf(WorkloadQueueStats("group-1", 0, 10), WorkloadQueueStats("group-2", 1, 9), WorkloadQueueStats("group-3", 0, 0))),
        Arguments.of(listOf(WorkloadQueueStats("group-2", 1, 9), WorkloadQueueStats("group-1", 1, 124124124))),
        Arguments.of(listOf(WorkloadQueueStats("group-3", 0, 0))),
      )
  }
}
