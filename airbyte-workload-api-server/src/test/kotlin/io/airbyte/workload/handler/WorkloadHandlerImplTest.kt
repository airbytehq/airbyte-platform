/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.handler

import io.airbyte.api.client.AirbyteApiClient
import io.airbyte.api.client.generated.SignalApi
import io.airbyte.api.client.model.generated.SignalInput
import io.airbyte.config.SignalInput.Companion.SYNC_WORKFLOW
import io.airbyte.config.WorkloadPriority
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.metrics.MetricAttribute
import io.airbyte.metrics.MetricClient
import io.airbyte.metrics.OssMetricsRegistry
import io.airbyte.metrics.lib.MetricTags
import io.airbyte.workload.api.domain.WorkloadLabel
import io.airbyte.workload.common.DefaultDeadlineValues
import io.airbyte.workload.errors.ConflictException
import io.airbyte.workload.errors.InvalidStatusTransitionException
import io.airbyte.workload.errors.NotFoundException
import io.airbyte.workload.handler.WorkloadHandlerImplTest.Fixtures.DATAPLANE_ID
import io.airbyte.workload.handler.WorkloadHandlerImplTest.Fixtures.WORKLOAD_ID
import io.airbyte.workload.handler.WorkloadHandlerImplTest.Fixtures.metricClient
import io.airbyte.workload.handler.WorkloadHandlerImplTest.Fixtures.signalApi
import io.airbyte.workload.handler.WorkloadHandlerImplTest.Fixtures.workloadHandler
import io.airbyte.workload.handler.WorkloadHandlerImplTest.Fixtures.workloadQueueRepository
import io.airbyte.workload.handler.WorkloadHandlerImplTest.Fixtures.workloadRepository
import io.airbyte.workload.handler.WorkloadHandlerImplTest.Fixtures.workloadService
import io.airbyte.workload.repository.WorkloadQueueRepository
import io.airbyte.workload.repository.WorkloadRepository
import io.airbyte.workload.repository.domain.Workload
import io.airbyte.workload.repository.domain.WorkloadQueueStats
import io.airbyte.workload.repository.domain.WorkloadStatus
import io.airbyte.workload.repository.domain.WorkloadType
import io.airbyte.workload.services.WorkloadService
import io.airbyte.workload.signal.ApiSignalSender
import io.airbyte.workload.signal.SignalSender
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
import kotlin.time.Duration
import kotlin.time.Duration.Companion.minutes
import kotlin.time.toJavaDuration
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
      WorkloadService.ACTIVE_STATUSES.toSet(),
    )
    assertFalse(WorkloadService.ACTIVE_STATUSES.contains(WorkloadStatus.CANCELLED))
    assertFalse(WorkloadService.ACTIVE_STATUSES.contains(WorkloadStatus.FAILURE))
    assertFalse(WorkloadService.ACTIVE_STATUSES.contains(WorkloadStatus.SUCCESS))
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
        workspaceId = UUID.randomUUID(),
        organizationId = UUID.randomUUID(),
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
    every { workloadRepository.searchByMutexKeyAndStatusInList("mutex-this", WorkloadService.ACTIVE_STATUSES) }.returns(listOf())
    every { workloadRepository.save(any()) }.returns(
      Fixtures.workload(),
    )
    workloadHandler.createWorkload(
      WORKLOAD_ID,
      workloadLabels,
      "input payload",
      UUID.randomUUID(),
      UUID.randomUUID(),
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
        UUID.randomUUID(),
        UUID.randomUUID(),
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
      workloadService.failWorkload(workloadIdWithSuccessfulFail, any(), any())
    }.answers {}
    every {
      workloadService.failWorkload(workloadIdWithFailedFail, any(), any())
    }.throws(
      io.airbyte.workload.services
        .InvalidStatusTransitionException(workloadIdWithFailedFail),
    )
    every { workloadRepository.save(any()) }.returns(newWorkload)
    every {
      workloadRepository.searchByMutexKeyAndStatusInList(
        "mutex-this",
        WorkloadService.ACTIVE_STATUSES,
      )
    }.returns(duplWorkloads + listOf(newWorkload))

    workloadHandler.createWorkload(
      WORKLOAD_ID,
      null,
      "",
      UUID.randomUUID(),
      UUID.randomUUID(),
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
      workloadService.failWorkload(workloadIdWithFailedFail, any(), any())
      workloadService.failWorkload(workloadIdWithSuccessfulFail, any(), any())
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
  fun `verify successful heartbeat`(workloadStatus: WorkloadStatus) {
    every { workloadService.heartbeatWorkload(WORKLOAD_ID, any()) } returns Unit
    workloadHandler.heartbeat(WORKLOAD_ID, now.plusMinutes(10))
    verify { workloadService.heartbeatWorkload(WORKLOAD_ID, now.plusMinutes(10)) }
  }

  @ParameterizedTest
  @EnumSource(value = WorkloadStatus::class, names = ["CANCELLED", "FAILURE", "SUCCESS", "PENDING"])
  fun `verify heartbeat failure exceptions are converted`(workloadStatus: WorkloadStatus) {
    every { workloadService.heartbeatWorkload(WORKLOAD_ID, any()) } throws
      io.airbyte.workload.services
        .InvalidStatusTransitionException("oops")
    assertThrows<InvalidStatusTransitionException> { workloadHandler.heartbeat(WORKLOAD_ID, now) }
  }

  @Test
  fun `claiming a workload unsuccesfully returns false`() {
    every { workloadRepository.claim(WORKLOAD_ID, any(), any()) }.returns(null)
    assertFalse { workloadHandler.claimWorkload(WORKLOAD_ID, DATAPLANE_ID, now) }
  }

  @Test
  fun `claiming a workload successfully returns true`() {
    every { workloadQueueRepository.ackWorkloadQueueItem(WORKLOAD_ID) } just Runs
    every { workloadRepository.claim(WORKLOAD_ID, DATAPLANE_ID, any()) }.returns(
      Fixtures.workload(
        id = WORKLOAD_ID,
        dataplaneId = DATAPLANE_ID,
        status = WorkloadStatus.CLAIMED,
      ),
    )
    val result = workloadHandler.claimWorkload(WORKLOAD_ID, DATAPLANE_ID, now)
    assertTrue(result)
    verify(exactly = 1) { workloadQueueRepository.ackWorkloadQueueItem(WORKLOAD_ID) }
  }

  @Test
  fun `test claiming a workload successfully`() {
    every { workloadQueueRepository.ackWorkloadQueueItem(WORKLOAD_ID) } just Runs
    every { workloadRepository.claim(WORKLOAD_ID, DATAPLANE_ID, any()) }.returns(
      Fixtures.workload(
        id = WORKLOAD_ID,
        status = WorkloadStatus.CLAIMED,
        dataplaneId = DATAPLANE_ID,
      ),
    )
    val result = workloadHandler.claimWorkload(WORKLOAD_ID, DATAPLANE_ID, now)
    assertTrue(result)
    verify(exactly = 1) { workloadQueueRepository.ackWorkloadQueueItem(WORKLOAD_ID) }
  }

  @Test
  fun `test claiming a workload unsuccessfully`() {
    every { workloadRepository.claim(WORKLOAD_ID, DATAPLANE_ID, any()) }.returns(null)
    val result = workloadHandler.claimWorkload(WORKLOAD_ID, DATAPLANE_ID, now)
    assertFalse(result)
    verify(exactly = 0) { workloadQueueRepository.ackWorkloadQueueItem(WORKLOAD_ID) }
  }

  @Test
  fun `cancelling workload errors are converted`() {
    every { workloadService.cancelWorkload(WORKLOAD_ID, any(), any()) } throws
      io.airbyte.workload.services
        .NotFoundException("who are you")
    assertThrows<NotFoundException> { workloadHandler.cancelWorkload(WORKLOAD_ID, "test", "test cancel") }
  }

  @Test
  fun `succeeding workload errors are converted`() {
    every { workloadRepository.succeed(WORKLOAD_ID) } throws
      io.airbyte.workload.services
        .NotFoundException("where are you")
    assertThrows<NotFoundException> { workloadHandler.succeedWorkload(WORKLOAD_ID) }
  }

  @Test
  fun `setting status to running errors are converted`() {
    every { workloadRepository.running(WORKLOAD_ID, any()) } throws
      io.airbyte.workload.services
        .InvalidStatusTransitionException("bad timing")
    assertThrows<InvalidStatusTransitionException> { workloadHandler.setWorkloadStatusToRunning(WORKLOAD_ID, now) }
  }

  @Test
  fun `setting status to launched errors are converted`() {
    every { workloadRepository.launch(WORKLOAD_ID, any()) } throws
      io.airbyte.workload.services
        .InvalidStatusTransitionException("boom")
    assertThrows<InvalidStatusTransitionException> { workloadHandler.setWorkloadStatusToLaunched(WORKLOAD_ID, now) }
  }

  @Test
  fun `failing workload errors are converted`() {
    every { workloadRepository.fail(WORKLOAD_ID, any(), any()) } throws
      io.airbyte.workload.services
        .InvalidStatusTransitionException("boom")
    assertThrows<InvalidStatusTransitionException> { workloadHandler.failWorkload(WORKLOAD_ID, "testing", "errors") }
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
        mockk<WorkloadService>(),
        mockk<WorkloadRepository>(),
        mockk<WorkloadQueueRepository>(),
        mockk<SignalSender>(),
        mockk<MetricClient>(),
        mockk<FeatureFlagClient>(),
        Fixtures.redeliveryWindow.toJavaDuration(),
      )
    val offsetDateTime = workloadHandlerImpl.offsetDateTime()
    Thread.sleep(10)
    val offsetDateTimeAfter10Ms = workloadHandlerImpl.offsetDateTime()
    assertTrue(offsetDateTimeAfter10Ms.isAfter(offsetDateTime))
  }

  @ParameterizedTest
  @MethodSource("pendingWorkloadMatrix")
  fun `poll workloads returns enqueued workloads (separate table enabled)`(
    group: String,
    priority: Int,
    domainWorkloads: List<Workload>,
  ) {
    every { workloadQueueRepository.pollWorkloadQueue(group, priority, 10, any()) }.returns(domainWorkloads)
    val result = workloadHandler.pollWorkloadQueue(group, WorkloadPriority.fromInt(priority), 10)
    val expected = domainWorkloads.map { it.toApi() }

    assertEquals(expected, result)
  }

  @ParameterizedTest
  @MethodSource("countPendingWorkloadMatrix")
  fun `count workload queue depth returns count of enqueued workloads (separate table enabled)`(
    group: String,
    priority: Int,
    count: Long,
  ) {
    every { workloadQueueRepository.countEnqueuedWorkloads(group, priority) }.returns(count)
    val result = workloadHandler.countWorkloadQueueDepth(group, WorkloadPriority.fromInt(priority))

    assertEquals(count, result)
  }

  @ParameterizedTest
  @MethodSource("workloadStatsMatrix")
  fun `get workload queue stats returns stats with enqueued workloads for each logical queue (dataplane group x priority) (separate table enabled)`(
    stats: List<WorkloadQueueStats>,
  ) {
    every { workloadQueueRepository.getEnqueuedWorkloadStats() }.returns(stats)
    val result = workloadHandler.getWorkloadQueueStats()
    val expected = stats.map { it.toApi() }

    assertEquals(expected, result)
  }

  object Fixtures {
    val workloadRepository = mockk<WorkloadRepository>()
    val workloadQueueRepository = mockk<WorkloadQueueRepository>()
    val metricClient: MetricClient = mockk(relaxed = true)
    private val airbyteApi: AirbyteApiClient = mockk()
    val featureFlagClient: FeatureFlagClient = mockk(relaxed = true)
    val signalApi: SignalApi = mockk()
    val signalSender = ApiSignalSender(airbyteApi, metricClient)
    val workloadService =
      spyk(
        WorkloadService(
          workloadRepository = workloadRepository,
          workloadQueueRepository = workloadQueueRepository,
          signalSender = signalSender,
          defaultDeadlineValues = DefaultDeadlineValues(),
          featureFlagClient = featureFlagClient,
        ),
      )
    const val WORKLOAD_ID = "test"
    const val DATAPLANE_ID = "dataplaneId"
    val redeliveryWindow: Duration = 30.minutes
    val workloadHandler =
      spyk(
        WorkloadHandlerImpl(
          workloadService = workloadService,
          workloadRepository,
          workloadQueueRepository,
          signalSender,
          metricClient,
          featureFlagClient,
          redeliveryWindow.toJavaDuration(),
        ),
      )

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
      workspaceId: UUID? = UUID.randomUUID(),
      organizationId: UUID? = UUID.randomUUID(),
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
        workspaceId = workspaceId,
        organizationId = organizationId,
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
    fun pendingWorkloadMatrix(): List<Arguments> =
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
