package io.airbyte.workload.handler

import io.airbyte.workload.api.domain.WorkloadLabel
import io.airbyte.workload.errors.ConflictException
import io.airbyte.workload.errors.InvalidStatusTransitionException
import io.airbyte.workload.errors.NotFoundException
import io.airbyte.workload.handler.WorkloadHandlerImplTest.Fixtures.DATAPLANE_ID
import io.airbyte.workload.handler.WorkloadHandlerImplTest.Fixtures.WORKLOAD_ID
import io.airbyte.workload.handler.WorkloadHandlerImplTest.Fixtures.workloadHandler
import io.airbyte.workload.handler.WorkloadHandlerImplTest.Fixtures.workloadRepository
import io.airbyte.workload.repository.WorkloadRepository
import io.airbyte.workload.repository.domain.Workload
import io.airbyte.workload.repository.domain.WorkloadStatus
import io.airbyte.workload.repository.domain.WorkloadType
import io.mockk.Runs
import io.mockk.clearAllMocks
import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.EnumSource
import java.time.OffsetDateTime
import java.util.Optional

class WorkloadHandlerImplTest {
  @BeforeEach
  fun reset() {
    clearAllMocks()
  }

  @Test
  fun `test get workload`() {
    val domainWorkload =
      Workload(
        id = WORKLOAD_ID,
        dataplaneId = null,
        status = WorkloadStatus.PENDING,
        workloadLabels = null,
        inputPayload = "",
        logPath = "/",
        geography = "US",
        mutexKey = null,
        type = WorkloadType.SYNC,
      )

    every { workloadRepository.findById(WORKLOAD_ID) }.returns(Optional.of(domainWorkload))
    val apiWorkload = workloadHandler.getWorkload(WORKLOAD_ID)
    assertEquals(domainWorkload.id, apiWorkload.id)
  }

  @Test
  fun `test workload not found`() {
    every { workloadRepository.findById(WORKLOAD_ID) }.returns(Optional.empty())
    assertThrows<NotFoundException> { workloadHandler.getWorkload(WORKLOAD_ID) }
  }

  @Test
  fun `test create workload`() {
    val workloadLabel1 = WorkloadLabel("key1", "value1")
    val workloadLabel2 = WorkloadLabel("key2", "value2")
    val workloadLabels = mutableListOf(workloadLabel1, workloadLabel2)

    every { workloadRepository.existsById(WORKLOAD_ID) }.returns(false)
    every { workloadRepository.save(any()) }.returns(
      Fixtures.workload(),
    )
    workloadHandler.createWorkload(
      WORKLOAD_ID,
      workloadLabels,
      "input payload",
      "/log/path",
      "US",
      "mutex-this",
      io.airbyte.config.WorkloadType.SYNC,
    )
    verify {
      workloadRepository.save(
        match {
          it.id == WORKLOAD_ID &&
            it.dataplaneId == null &&
            it.status == WorkloadStatus.PENDING &&
            it.lastHeartbeatAt == null &&
            it.workloadLabels!!.get(0).key == workloadLabel1.key &&
            it.workloadLabels!!.get(0).value == workloadLabel1.value &&
            it.workloadLabels!!.get(1).key == workloadLabel2.key &&
            it.workloadLabels!!.get(1).value == workloadLabel2.value &&
            it.inputPayload == "input payload" &&
            it.logPath == "/log/path" &&
            it.geography == "US" &&
            it.mutexKey == "mutex-this" &&
            it.type == WorkloadType.SYNC
        },
      )
    }
  }

  @Test
  fun `test create workload id conflict`() {
    every { workloadRepository.existsById(WORKLOAD_ID) }.returns(true)
    assertThrows<ConflictException> {
      workloadHandler.createWorkload(WORKLOAD_ID, null, "", "", "US", "mutex-this", io.airbyte.config.WorkloadType.SYNC)
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
        geography = "US",
        mutexKey = "mutex-this",
        type = WorkloadType.DISCOVER,
      )
    every { workloadRepository.search(any(), any(), any()) }.returns(listOf(domainWorkload))
    val workloads = workloadHandler.getWorkloads(listOf("dataplane1"), listOf(ApiWorkloadStatus.CLAIMED, ApiWorkloadStatus.FAILURE), null)
    assertEquals(1, workloads.size)
    assertEquals(WORKLOAD_ID, workloads[0].id)
    assertEquals("a payload", workloads[0].inputPayload)
    assertEquals("/log/path", workloads[0].logPath)
    assertEquals("US", workloads[0].geography)
    assertEquals("mutex-this", workloads[0].mutexKey)
    assertEquals(io.airbyte.config.WorkloadType.DISCOVER, workloads[0].type)
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
    every { workloadRepository.update(eq(WORKLOAD_ID), any(), ofType(OffsetDateTime::class)) }.returns(Unit)
    workloadHandler.heartbeat(WORKLOAD_ID)
    verify { workloadRepository.update(eq(WORKLOAD_ID), eq(WorkloadStatus.RUNNING), any()) }
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
    assertThrows<InvalidStatusTransitionException> { workloadHandler.heartbeat(WORKLOAD_ID) }
  }

  @Test
  fun `test workload not found when claiming workload`() {
    every { workloadRepository.findById(WORKLOAD_ID) }.returns(Optional.empty())
    assertThrows<NotFoundException> { workloadHandler.claimWorkload(WORKLOAD_ID, DATAPLANE_ID) }
  }

  @Test
  fun `test claiming workload has already been claimed by another plane`() {
    every { workloadRepository.findById(WORKLOAD_ID) }.returns(
      Optional.of(
        Fixtures.workload(
          id = WORKLOAD_ID,
          dataplaneId = "otherDataplaneId",
          status = WorkloadStatus.CLAIMED,
        ),
      ),
    )
    assertFalse(workloadHandler.claimWorkload(WORKLOAD_ID, DATAPLANE_ID))
  }

  @Test
  fun `test claiming pending workload has already been claimed by the same plane`() {
    every { workloadRepository.update(WORKLOAD_ID, DATAPLANE_ID, WorkloadStatus.CLAIMED) }.returns(Unit)
    every { workloadRepository.findById(WORKLOAD_ID) }.returns(
      Optional.of(
        Fixtures.workload(
          id = WORKLOAD_ID,
          dataplaneId = DATAPLANE_ID,
          status = WorkloadStatus.PENDING,
        ),
      ),
    )
    assertTrue(workloadHandler.claimWorkload(WORKLOAD_ID, DATAPLANE_ID))
  }

  @Test
  fun `test claiming claimed workload has already been claimed by the same plane`() {
    every { workloadRepository.findById(WORKLOAD_ID) }.returns(
      Optional.of(
        Fixtures.workload(
          id = WORKLOAD_ID,
          dataplaneId = DATAPLANE_ID,
          status = WorkloadStatus.CLAIMED,
        ),
      ),
    )
    assertTrue(workloadHandler.claimWorkload(WORKLOAD_ID, DATAPLANE_ID))
  }

  @Test
  fun `test claiming running workload has already been claimed by the same plane`() {
    every { workloadRepository.findById(WORKLOAD_ID) }.returns(
      Optional.of(
        Fixtures.workload(
          id = WORKLOAD_ID,
          dataplaneId = DATAPLANE_ID,
          status = WorkloadStatus.RUNNING,
        ),
      ),
    )
    assertThrows<InvalidStatusTransitionException> { workloadHandler.claimWorkload(WORKLOAD_ID, DATAPLANE_ID) }
  }

  @ParameterizedTest
  @EnumSource(value = WorkloadStatus::class, names = ["RUNNING", "LAUNCHED", "SUCCESS", "FAILURE", "CANCELLED"])
  fun `test claiming workload that is not pending`(workloadStatus: WorkloadStatus) {
    every { workloadRepository.findById(WORKLOAD_ID) }.returns(
      Optional.of(
        Fixtures.workload(
          id = WORKLOAD_ID,
          status = workloadStatus,
        ),
      ),
    )
    assertThrows<InvalidStatusTransitionException> { workloadHandler.claimWorkload(WORKLOAD_ID, DATAPLANE_ID) }
  }

  @Test
  fun `test successful claim`() {
    every { workloadRepository.findById(WORKLOAD_ID) }.returns(
      Optional.of(
        Fixtures.workload(
          id = WORKLOAD_ID,
          dataplaneId = null,
        ),
      ),
    )

    every { workloadRepository.update(any(), any(), ofType(WorkloadStatus::class)) } just Runs

    assertTrue(workloadHandler.claimWorkload(WORKLOAD_ID, DATAPLANE_ID))

    verify { workloadRepository.update(WORKLOAD_ID, DATAPLANE_ID, WorkloadStatus.CLAIMED) }
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
        ),
      ),
    )

    every { workloadRepository.update(any(), ofType(WorkloadStatus::class), eq("test"), eq("test cancel")) } just Runs

    workloadHandler.cancelWorkload(WORKLOAD_ID, "test", "test cancel")
    verify { workloadRepository.update(eq(WORKLOAD_ID), eq(WorkloadStatus.CANCELLED), eq("test"), eq("test cancel")) }
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
    verify(exactly = 0) { workloadRepository.update(eq(WORKLOAD_ID), eq(WorkloadStatus.CANCELLED), "test", "test cancel again") }
  }

  @Test
  fun `test workload not found when failing workload`() {
    every { workloadRepository.findById(WORKLOAD_ID) }.returns(Optional.empty())
    assertThrows<NotFoundException> { workloadHandler.failWorkload(WORKLOAD_ID, "test", "fail") }
  }

  @ParameterizedTest
  @EnumSource(value = WorkloadStatus::class, names = ["SUCCESS", "PENDING", "CANCELLED"])
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
        ),
      ),
    )

    every { workloadRepository.update(any(), ofType(WorkloadStatus::class), eq("test"), eq("failing a workload")) } just Runs

    workloadHandler.failWorkload(WORKLOAD_ID, "test", "failing a workload")
    verify { workloadRepository.update(eq(WORKLOAD_ID), eq(WorkloadStatus.FAILURE), eq("test"), eq("failing a workload")) }
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
    verify(exactly = 0) { workloadRepository.update(eq(WORKLOAD_ID), eq(WorkloadStatus.FAILURE), eq("test"), eq("noop")) }
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
        ),
      ),
    )

    every { workloadRepository.update(any(), ofType(WorkloadStatus::class)) } just Runs

    workloadHandler.succeedWorkload(WORKLOAD_ID)
    verify { workloadRepository.update(eq(WORKLOAD_ID), eq(WorkloadStatus.SUCCESS)) }
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
    verify(exactly = 0) { workloadRepository.update(eq(WORKLOAD_ID), eq(WorkloadStatus.SUCCESS)) }
  }

  @Test
  fun `test workload not found when setting status to running`() {
    every { workloadRepository.findById(WORKLOAD_ID) }.returns(Optional.empty())
    assertThrows<NotFoundException> { workloadHandler.setWorkloadStatusToRunning(WORKLOAD_ID) }
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

    assertThrows<InvalidStatusTransitionException> { workloadHandler.setWorkloadStatusToRunning(WORKLOAD_ID) }
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

    assertThrows<InvalidStatusTransitionException> { workloadHandler.setWorkloadStatusToRunning(WORKLOAD_ID) }
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

    every { workloadRepository.update(any(), ofType(WorkloadStatus::class)) } just Runs

    workloadHandler.setWorkloadStatusToRunning(WORKLOAD_ID)
    verify { workloadRepository.update(eq(WORKLOAD_ID), eq(WorkloadStatus.RUNNING)) }
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

    workloadHandler.setWorkloadStatusToRunning(WORKLOAD_ID)
    verify(exactly = 0) { workloadRepository.update(eq(WORKLOAD_ID), eq(WorkloadStatus.RUNNING)) }
  }

  @Test
  fun `test workload not found when setting status to launched`() {
    every { workloadRepository.findById(WORKLOAD_ID) }.returns(Optional.empty())
    assertThrows<NotFoundException> { workloadHandler.setWorkloadStatusToLaunched(WORKLOAD_ID) }
  }

  @ParameterizedTest
  @EnumSource(value = WorkloadStatus::class, names = ["PENDING", "RUNNING", "SUCCESS", "CANCELLED", "FAILURE"])
  fun `test set workload status to launched when is not in claimed state`(workloadStatus: WorkloadStatus) {
    every { workloadRepository.findById(WORKLOAD_ID) }.returns(
      Optional.of(
        Fixtures.workload(
          id = WORKLOAD_ID,
          status = workloadStatus,
        ),
      ),
    )

    assertThrows<InvalidStatusTransitionException> { workloadHandler.setWorkloadStatusToLaunched(WORKLOAD_ID) }
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

    every { workloadRepository.update(any(), ofType(WorkloadStatus::class)) } just Runs

    workloadHandler.setWorkloadStatusToLaunched(WORKLOAD_ID)
    verify { workloadRepository.update(eq(WORKLOAD_ID), eq(WorkloadStatus.LAUNCHED)) }
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

    workloadHandler.setWorkloadStatusToLaunched(WORKLOAD_ID)
    verify(exactly = 0) { workloadRepository.update(eq(WORKLOAD_ID), eq(WorkloadStatus.LAUNCHED)) }
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

  object Fixtures {
    val workloadRepository = mockk<WorkloadRepository>()
    const val WORKLOAD_ID = "test"
    const val DATAPLANE_ID = "dataplaneId"
    val workloadHandler = WorkloadHandlerImpl(workloadRepository)

    fun workload(
      id: String = WORKLOAD_ID,
      dataplaneId: String? = null,
      status: WorkloadStatus = WorkloadStatus.PENDING,
      workloadLabels: List<io.airbyte.workload.repository.domain.WorkloadLabel>? = listOf(),
      inputPayload: String = "",
      logPath: String = "/",
      geography: String = "US",
      mutexKey: String = "",
      type: WorkloadType = WorkloadType.SYNC,
    ): Workload =
      Workload(
        id = id,
        dataplaneId = dataplaneId,
        status = status,
        workloadLabels = workloadLabels,
        inputPayload = inputPayload,
        logPath = logPath,
        geography = geography,
        mutexKey = mutexKey,
        type = type,
      )
  }
}
