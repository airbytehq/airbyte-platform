package io.airbyte.workload.handler

import io.airbyte.db.instance.configs.jooq.generated.enums.WorkloadStatus
import io.airbyte.db.instance.configs.jooq.generated.enums.WorkloadType
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
        status = WorkloadStatus.pending,
        workloadLabels = null,
        inputPayload = "",
        logPath = "/",
        geography = "US",
        mutexKey = null,
        type = WorkloadType.sync,
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
      io.airbyte.workload.api.domain.WorkloadType.SYNC,
    )
    verify {
      workloadRepository.save(
        match {
          it.id == WORKLOAD_ID &&
            it.dataplaneId == null &&
            it.status == WorkloadStatus.pending &&
            it.lastHeartbeatAt == null &&
            it.workloadLabels!!.get(0).key == workloadLabel1.key &&
            it.workloadLabels!!.get(0).value == workloadLabel1.value &&
            it.workloadLabels!!.get(1).key == workloadLabel2.key &&
            it.workloadLabels!!.get(1).value == workloadLabel2.value &&
            it.inputPayload == "input payload" &&
            it.logPath == "/log/path" &&
            it.geography == "US" &&
            it.mutexKey == "mutex-this" &&
            it.type == WorkloadType.sync
        },
      )
    }
  }

  @Test
  fun `test create workload id conflict`() {
    every { workloadRepository.existsById(WORKLOAD_ID) }.returns(true)
    assertThrows<ConflictException> {
      workloadHandler.createWorkload(WORKLOAD_ID, null, "", "", "US", "mutex-this", io.airbyte.workload.api.domain.WorkloadType.SYNC)
    }
  }

  @Test
  fun `test get workloads`() {
    val domainWorkload =
      Fixtures.workload(
        id = WORKLOAD_ID,
        dataplaneId = null,
        status = WorkloadStatus.pending,
        workloadLabels = null,
        inputPayload = "a payload",
        logPath = "/log/path",
        geography = "US",
        mutexKey = "mutex-this",
        type = WorkloadType.discover,
      )
    every { workloadRepository.search(any(), any(), any()) }.returns(listOf(domainWorkload))
    val workloads = workloadHandler.getWorkloads(listOf("dataplane1"), listOf(ApiWorkloadStatus.CLAIMED, ApiWorkloadStatus.FAILURE), null)
    assertEquals(1, workloads.size)
    assertEquals(WORKLOAD_ID, workloads[0].id)
    assertEquals("a payload", workloads[0].inputPayload)
    assertEquals("/log/path", workloads[0].logPath)
    assertEquals("US", workloads[0].geography)
    assertEquals("mutex-this", workloads[0].mutexKey)
    assertEquals(io.airbyte.workload.api.domain.WorkloadType.DISCOVER, workloads[0].type)
  }

  @ParameterizedTest
  @EnumSource(value = WorkloadStatus::class, names = ["claimed", "running"])
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
    verify { workloadRepository.update(eq(WORKLOAD_ID), eq(WorkloadStatus.running), any()) }
  }

  @ParameterizedTest
  @EnumSource(value = WorkloadStatus::class, names = ["cancelled", "failure", "success", "pending"])
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
          status = WorkloadStatus.claimed,
        ),
      ),
    )
    assertFalse(workloadHandler.claimWorkload(WORKLOAD_ID, DATAPLANE_ID))
  }

  @Test
  fun `test claiming pending workload has already been claimed by the same plane`() {
    every { workloadRepository.update(WORKLOAD_ID, DATAPLANE_ID, WorkloadStatus.claimed) }.returns(Unit)
    every { workloadRepository.findById(WORKLOAD_ID) }.returns(
      Optional.of(
        Fixtures.workload(
          id = WORKLOAD_ID,
          dataplaneId = DATAPLANE_ID,
          status = WorkloadStatus.pending,
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
          status = WorkloadStatus.claimed,
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
          status = WorkloadStatus.running,
        ),
      ),
    )
    assertThrows<InvalidStatusTransitionException> { workloadHandler.claimWorkload(WORKLOAD_ID, DATAPLANE_ID) }
  }

  @ParameterizedTest
  @EnumSource(value = WorkloadStatus::class, names = ["running", "success", "failure", "cancelled"])
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

    verify { workloadRepository.update(WORKLOAD_ID, DATAPLANE_ID, WorkloadStatus.claimed) }
  }

  @Test
  fun `test workload not found when cancelling workload`() {
    every { workloadRepository.findById(WORKLOAD_ID) }.returns(Optional.empty())
    assertThrows<NotFoundException> { workloadHandler.cancelWorkload(WORKLOAD_ID) }
  }

  @ParameterizedTest
  @EnumSource(value = WorkloadStatus::class, names = ["success", "failure"])
  fun `test cancel workload in terminal state`(workloadStatus: WorkloadStatus) {
    every { workloadRepository.findById(WORKLOAD_ID) }.returns(
      Optional.of(
        Fixtures.workload(
          id = WORKLOAD_ID,
          status = workloadStatus,
        ),
      ),
    )

    assertThrows<InvalidStatusTransitionException> { workloadHandler.cancelWorkload(WORKLOAD_ID) }
  }

  @ParameterizedTest
  @EnumSource(value = WorkloadStatus::class, names = ["claimed", "running", "pending"])
  fun `test successful cancel`(workloadStatus: WorkloadStatus) {
    every { workloadRepository.findById(WORKLOAD_ID) }.returns(
      Optional.of(
        Fixtures.workload(
          id = WORKLOAD_ID,
          status = workloadStatus,
        ),
      ),
    )

    every { workloadRepository.update(any(), ofType(WorkloadStatus::class)) } just Runs

    workloadHandler.cancelWorkload(WORKLOAD_ID)
    verify { workloadRepository.update(eq(WORKLOAD_ID), eq(WorkloadStatus.cancelled)) }
  }

  @Test
  fun `test noop cancel`() {
    every { workloadRepository.findById(WORKLOAD_ID) }.returns(
      Optional.of(
        Fixtures.workload(
          id = WORKLOAD_ID,
          status = WorkloadStatus.cancelled,
        ),
      ),
    )

    workloadHandler.cancelWorkload(WORKLOAD_ID)
    verify(exactly = 0) { workloadRepository.update(eq(WORKLOAD_ID), eq(WorkloadStatus.cancelled)) }
  }

  @Test
  fun `test workload not found when failing workload`() {
    every { workloadRepository.findById(WORKLOAD_ID) }.returns(Optional.empty())
    assertThrows<NotFoundException> { workloadHandler.failWorkload(WORKLOAD_ID) }
  }

  @ParameterizedTest
  @EnumSource(value = WorkloadStatus::class, names = ["success", "pending", "cancelled"])
  fun `test fail workload in inactive status`(workloadStatus: WorkloadStatus) {
    every { workloadRepository.findById(WORKLOAD_ID) }.returns(
      Optional.of(
        Fixtures.workload(
          id = WORKLOAD_ID,
          status = workloadStatus,
        ),
      ),
    )

    assertThrows<InvalidStatusTransitionException> { workloadHandler.failWorkload(WORKLOAD_ID) }
  }

  @ParameterizedTest
  @EnumSource(value = WorkloadStatus::class, names = ["claimed", "running"])
  fun `test failing workload succeeded`(workloadStatus: WorkloadStatus) {
    every { workloadRepository.findById(WORKLOAD_ID) }.returns(
      Optional.of(
        Fixtures.workload(
          id = WORKLOAD_ID,
          status = workloadStatus,
        ),
      ),
    )

    every { workloadRepository.update(any(), ofType(WorkloadStatus::class)) } just Runs

    workloadHandler.failWorkload(WORKLOAD_ID)
    verify { workloadRepository.update(eq(WORKLOAD_ID), eq(WorkloadStatus.failure)) }
  }

  @Test
  fun `test noop failure`() {
    every { workloadRepository.findById(WORKLOAD_ID) }.returns(
      Optional.of(
        Fixtures.workload(
          id = WORKLOAD_ID,
          status = WorkloadStatus.failure,
        ),
      ),
    )

    workloadHandler.failWorkload(WORKLOAD_ID)
    verify(exactly = 0) { workloadRepository.update(eq(WORKLOAD_ID), eq(WorkloadStatus.failure)) }
  }

  @Test
  fun `test workload not found when succeeding workload`() {
    every { workloadRepository.findById(WORKLOAD_ID) }.returns(Optional.empty())
    assertThrows<NotFoundException> { workloadHandler.succeedWorkload(WORKLOAD_ID) }
  }

  @ParameterizedTest
  @EnumSource(value = WorkloadStatus::class, names = ["pending", "cancelled", "failure"])
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
  @EnumSource(value = WorkloadStatus::class, names = ["claimed", "running"])
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
    verify { workloadRepository.update(eq(WORKLOAD_ID), eq(WorkloadStatus.success)) }
  }

  @Test
  fun `test noop success`() {
    every { workloadRepository.findById(WORKLOAD_ID) }.returns(
      Optional.of(
        Fixtures.workload(
          id = WORKLOAD_ID,
          status = WorkloadStatus.success,
        ),
      ),
    )

    workloadHandler.succeedWorkload(WORKLOAD_ID)
    verify(exactly = 0) { workloadRepository.update(eq(WORKLOAD_ID), eq(WorkloadStatus.success)) }
  }

  @Test
  fun `test workload not found when setting status to running`() {
    every { workloadRepository.findById(WORKLOAD_ID) }.returns(Optional.empty())
    assertThrows<NotFoundException> { workloadHandler.setWorkloadStatusToRunning(WORKLOAD_ID) }
  }

  @ParameterizedTest
  @EnumSource(value = WorkloadStatus::class, names = ["success", "cancelled", "failure"])
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
          status = WorkloadStatus.pending,
        ),
      ),
    )

    assertThrows<InvalidStatusTransitionException> { workloadHandler.setWorkloadStatusToRunning(WORKLOAD_ID) }
  }

  @Test
  fun `test set workload status to running succeeded`() {
    every { workloadRepository.findById(WORKLOAD_ID) }.returns(
      Optional.of(
        Fixtures.workload(
          id = WORKLOAD_ID,
          status = WorkloadStatus.claimed,
        ),
      ),
    )

    every { workloadRepository.update(any(), ofType(WorkloadStatus::class)) } just Runs

    workloadHandler.succeedWorkload(WORKLOAD_ID)
    verify { workloadRepository.update(eq(WORKLOAD_ID), eq(WorkloadStatus.success)) }
  }

  @Test
  fun `test noop when setting workload status to running`() {
    every { workloadRepository.findById(WORKLOAD_ID) }.returns(
      Optional.of(
        Fixtures.workload(
          id = WORKLOAD_ID,
          status = WorkloadStatus.success,
        ),
      ),
    )

    workloadHandler.succeedWorkload(WORKLOAD_ID)
    verify(exactly = 0) { workloadRepository.update(eq(WORKLOAD_ID), eq(WorkloadStatus.success)) }
  }

  object Fixtures {
    val workloadRepository = mockk<WorkloadRepository>()
    const val WORKLOAD_ID = "test"
    const val DATAPLANE_ID = "dataplaneId"
    val workloadHandler = WorkloadHandlerImpl(workloadRepository)

    fun workload(
      id: String = WORKLOAD_ID,
      dataplaneId: String? = null,
      status: WorkloadStatus = WorkloadStatus.pending,
      workloadLabels: List<io.airbyte.workload.repository.domain.WorkloadLabel>? = listOf(),
      inputPayload: String = "",
      logPath: String = "/",
      geography: String = "US",
      mutexKey: String = "",
      type: WorkloadType = WorkloadType.sync,
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
