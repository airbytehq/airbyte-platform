package io.airbyte.workload.handler

import io.airbyte.db.instance.configs.jooq.generated.enums.WorkloadStatus
import io.airbyte.workload.api.domain.WorkloadLabel
import io.airbyte.workload.errors.InvalidStatusTransitionException
import io.airbyte.workload.errors.NotFoundException
import io.airbyte.workload.errors.NotModifiedException
import io.airbyte.workload.repository.WorkloadRepository
import io.airbyte.workload.repository.domain.Workload
import io.mockk.Runs
import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.EnumSource
import java.time.OffsetDateTime
import java.util.Optional

class WorkloadHandlerImplTest {
  private val workloadRepository = mockk<WorkloadRepository>()
  private val workloadId = "test"
  private val dataplaneId = "dataplaneId"
  private val workloadHandler = WorkloadHandlerImpl(workloadRepository)

  @Test
  fun `test get workload`() {
    val domainWorkload =
      Workload(
        id = workloadId,
        dataplaneId = null,
        status = WorkloadStatus.pending,
        createdAt = null,
        updatedAt = null,
        lastHeartbeatAt = null,
        workloadLabels = null,
        inputPayload = "",
        logPath = "/",
      )

    every { workloadRepository.findById(workloadId) }.returns(Optional.of(domainWorkload))
    val apiWorkload = workloadHandler.getWorkload(workloadId)
    assertEquals(domainWorkload.id, apiWorkload.id)
  }

  @Test
  fun `test workload not found`() {
    every { workloadRepository.findById(workloadId) }.returns(Optional.empty())
    assertThrows<NotFoundException> { workloadHandler.getWorkload(workloadId) }
  }

  @Test
  fun `test create workload`() {
    val workloadLabel1 = WorkloadLabel("key1", "value1")
    val workloadLabel2 = WorkloadLabel("key2", "value2")
    val workloadLabels = mutableListOf(workloadLabel1, workloadLabel2)

    every { workloadRepository.existsById(workloadId) }.returns(false)
    every { workloadRepository.save(any()) }.returns(
      Workload(
        id = workloadId,
        dataplaneId = null,
        status = WorkloadStatus.pending,
        lastHeartbeatAt = null,
        workloadLabels = null,
        inputPayload = "",
        logPath = "/",
      ),
    )
    workloadHandler.createWorkload(workloadId, workloadLabels, "input payload", "/log/path")
    verify {
      workloadRepository.save(
        match {
          it.id == workloadId &&
            it.dataplaneId == null &&
            it.status == WorkloadStatus.pending &&
            it.lastHeartbeatAt == null &&
            it.workloadLabels!!.get(0).key == workloadLabel1.key &&
            it.workloadLabels!!.get(0).value == workloadLabel1.value &&
            it.workloadLabels!!.get(1).key == workloadLabel2.key &&
            it.workloadLabels!!.get(1).value == workloadLabel2.value &&
            it.inputPayload == "input payload" &&
            it.logPath == "/log/path"
        },
      )
    }
  }

  @Test
  fun `test create workload id conflict`() {
    every { workloadRepository.existsById(workloadId) }.returns(true)
    assertThrows<NotModifiedException> { workloadHandler.createWorkload(workloadId, null, "", "") }
  }

  @Test
  fun `test get workloads`() {
    val domainWorkload =
      Workload(
        id = workloadId,
        dataplaneId = null,
        status = WorkloadStatus.pending,
        createdAt = null,
        updatedAt = null,
        lastHeartbeatAt = null,
        workloadLabels = null,
        inputPayload = "a payload",
        logPath = "/log/path",
      )
    every { workloadRepository.search(any(), any(), any()) }.returns(listOf(domainWorkload))
    val workloads = workloadHandler.getWorkloads(listOf("dataplane1"), listOf(ApiWorkloadStatus.CLAIMED, ApiWorkloadStatus.FAILURE), null)
    assertEquals(1, workloads.size)
    assertEquals(workloadId, workloads[0].id)
    assertEquals("a payload", workloads[0].inputPayload)
    assertEquals("/log/path", workloads[0].logPath)
  }

  @ParameterizedTest
  @EnumSource(value = WorkloadStatus::class, names = ["claimed", "running"])
  fun `test successfulHeartbeat`(workloadStatus: WorkloadStatus) {
    every { workloadRepository.findById(workloadId) }.returns(
      Optional.of(
        Workload(
          id = workloadId,
          dataplaneId = null,
          status = workloadStatus,
          lastHeartbeatAt = null,
          workloadLabels = listOf(),
          inputPayload = "",
          logPath = "/",
        ),
      ),
    )
    every { workloadRepository.update(eq(workloadId), any(), ofType(OffsetDateTime::class)) }.returns(Unit)
    workloadHandler.heartbeat(workloadId)
    verify { workloadRepository.update(eq(workloadId), eq(WorkloadStatus.running), any()) }
  }

  @ParameterizedTest
  @EnumSource(value = WorkloadStatus::class, names = ["cancelled", "failure", "success", "pending"])
  fun `test nonAuthorizedHeartbeat`(workloadStatus: WorkloadStatus) {
    every { workloadRepository.findById(workloadId) }.returns(
      Optional.of(
        Workload(
          id = workloadId,
          dataplaneId = null,
          status = workloadStatus,
          lastHeartbeatAt = null,
          workloadLabels = listOf(),
          inputPayload = "",
          logPath = "/",
        ),
      ),
    )
    assertThrows<InvalidStatusTransitionException> { workloadHandler.heartbeat(workloadId) }
  }

  @Test
  fun `test workload not found when claiming workload`() {
    every { workloadRepository.findById(workloadId) }.returns(Optional.empty())
    assertThrows<NotFoundException> { workloadHandler.claimWorkload(workloadId, dataplaneId) }
  }

  @Test
  fun `test claiming workload has already been claimed by another plane`() {
    every { workloadRepository.findById(workloadId) }.returns(
      Optional.of(
        Workload(
          id = workloadId,
          dataplaneId = "otherDataplaneId",
          status = WorkloadStatus.claimed,
          lastHeartbeatAt = null,
          workloadLabels = listOf(),
          inputPayload = "",
          logPath = "/",
        ),
      ),
    )
    assertFalse(workloadHandler.claimWorkload(workloadId, dataplaneId))
  }

  @Test
  fun `test claiming pending workload has already been claimed by the same plane`() {
    every { workloadRepository.update(workloadId, dataplaneId, WorkloadStatus.claimed) }.returns(Unit)
    every { workloadRepository.findById(workloadId) }.returns(
      Optional.of(
        Workload(
          id = workloadId,
          dataplaneId = dataplaneId,
          status = WorkloadStatus.pending,
          lastHeartbeatAt = null,
          workloadLabels = listOf(),
          inputPayload = "",
          logPath = "/",
        ),
      ),
    )
    assertTrue(workloadHandler.claimWorkload(workloadId, dataplaneId))
  }

  @Test
  fun `test claiming claimed workload has already been claimed by the same plane`() {
    every { workloadRepository.findById(workloadId) }.returns(
      Optional.of(
        Workload(
          id = workloadId,
          dataplaneId = dataplaneId,
          status = WorkloadStatus.claimed,
          lastHeartbeatAt = null,
          workloadLabels = listOf(),
          inputPayload = "",
          logPath = "/",
        ),
      ),
    )
    assertTrue(workloadHandler.claimWorkload(workloadId, dataplaneId))
  }

  @Test
  fun `test claiming running workload has already been claimed by the same plane`() {
    every { workloadRepository.findById(workloadId) }.returns(
      Optional.of(
        Workload(
          id = workloadId,
          dataplaneId = dataplaneId,
          status = WorkloadStatus.running,
          lastHeartbeatAt = null,
          workloadLabels = listOf(),
          inputPayload = "",
          logPath = "/",
        ),
      ),
    )
    assertThrows<InvalidStatusTransitionException> { workloadHandler.claimWorkload(workloadId, dataplaneId) }
  }

  @ParameterizedTest
  @EnumSource(value = WorkloadStatus::class, names = ["running", "success", "failure", "cancelled"])
  fun `test claiming workload that is not pending`(workloadStatus: WorkloadStatus) {
    every { workloadRepository.findById(workloadId) }.returns(
      Optional.of(
        Workload(
          id = workloadId,
          dataplaneId = null,
          status = workloadStatus,
          lastHeartbeatAt = null,
          workloadLabels = listOf(),
          inputPayload = "",
          logPath = "/",
        ),
      ),
    )
    assertThrows<InvalidStatusTransitionException> { workloadHandler.claimWorkload(workloadId, dataplaneId) }
  }

  @Test
  fun `test successful claim`() {
    every { workloadRepository.findById(workloadId) }.returns(
      Optional.of(
        Workload(
          id = workloadId,
          dataplaneId = null,
          status = WorkloadStatus.pending,
          lastHeartbeatAt = null,
          workloadLabels = listOf(),
          inputPayload = "",
          logPath = "/",
        ),
      ),
    )

    every { workloadRepository.update(any(), any(), ofType(WorkloadStatus::class)) } just Runs

    assertTrue(workloadHandler.claimWorkload(workloadId, dataplaneId))

    verify { workloadRepository.update(workloadId, dataplaneId, WorkloadStatus.claimed) }
  }

  @Test
  fun `test workload not found when cancelling workload`() {
    every { workloadRepository.findById(workloadId) }.returns(Optional.empty())
    assertThrows<NotFoundException> { workloadHandler.cancelWorkload(workloadId) }
  }

  @ParameterizedTest
  @EnumSource(value = WorkloadStatus::class, names = ["success", "failure"])
  fun `test cancel workload in terminal state`(workloadStatus: WorkloadStatus) {
    every { workloadRepository.findById(workloadId) }.returns(
      Optional.of(
        Workload(
          id = workloadId,
          dataplaneId = null,
          status = workloadStatus,
          lastHeartbeatAt = null,
          workloadLabels = listOf(),
          inputPayload = "",
          logPath = "/",
        ),
      ),
    )

    assertThrows<InvalidStatusTransitionException> { workloadHandler.cancelWorkload(workloadId) }
  }

  @ParameterizedTest
  @EnumSource(value = WorkloadStatus::class, names = ["claimed", "running", "pending"])
  fun `test successful cancel`(workloadStatus: WorkloadStatus) {
    every { workloadRepository.findById(workloadId) }.returns(
      Optional.of(
        Workload(
          id = workloadId,
          dataplaneId = null,
          status = workloadStatus,
          lastHeartbeatAt = null,
          workloadLabels = listOf(),
          inputPayload = "",
          logPath = "/",
        ),
      ),
    )

    every { workloadRepository.update(any(), ofType(WorkloadStatus::class)) } just Runs

    workloadHandler.cancelWorkload(workloadId)
    verify { workloadRepository.update(eq(workloadId), eq(WorkloadStatus.cancelled)) }
  }

  @Test
  fun `test noop cancel`() {
    every { workloadRepository.findById(workloadId) }.returns(
      Optional.of(
        Workload(
          id = workloadId,
          dataplaneId = null,
          status = WorkloadStatus.cancelled,
          lastHeartbeatAt = null,
          workloadLabels = listOf(),
          inputPayload = "",
          logPath = "/",
        ),
      ),
    )

    workloadHandler.cancelWorkload(workloadId)
    verify(exactly = 0) { workloadRepository.update(eq(workloadId), eq(WorkloadStatus.cancelled)) }
  }

  @Test
  fun `test workload not found when failing workload`() {
    every { workloadRepository.findById(workloadId) }.returns(Optional.empty())
    assertThrows<NotFoundException> { workloadHandler.failWorkload(workloadId) }
  }

  @ParameterizedTest
  @EnumSource(value = WorkloadStatus::class, names = ["success", "pending", "cancelled"])
  fun `test fail workload in inactive status`(workloadStatus: WorkloadStatus) {
    every { workloadRepository.findById(workloadId) }.returns(
      Optional.of(
        Workload(
          id = workloadId,
          dataplaneId = null,
          status = workloadStatus,
          lastHeartbeatAt = null,
          workloadLabels = listOf(),
          inputPayload = "",
          logPath = "/",
        ),
      ),
    )

    assertThrows<InvalidStatusTransitionException> { workloadHandler.failWorkload(workloadId) }
  }

  @ParameterizedTest
  @EnumSource(value = WorkloadStatus::class, names = ["claimed", "running"])
  fun `test failing workload succeeded`(workloadStatus: WorkloadStatus) {
    every { workloadRepository.findById(workloadId) }.returns(
      Optional.of(
        Workload(
          id = workloadId,
          dataplaneId = null,
          status = workloadStatus,
          lastHeartbeatAt = null,
          workloadLabels = listOf(),
          inputPayload = "",
          logPath = "/",
        ),
      ),
    )

    every { workloadRepository.update(any(), ofType(WorkloadStatus::class)) } just Runs

    workloadHandler.failWorkload(workloadId)
    verify { workloadRepository.update(eq(workloadId), eq(WorkloadStatus.failure)) }
  }

  @Test
  fun `test noop failure`() {
    every { workloadRepository.findById(workloadId) }.returns(
      Optional.of(
        Workload(
          id = workloadId,
          dataplaneId = null,
          status = WorkloadStatus.failure,
          lastHeartbeatAt = null,
          workloadLabels = listOf(),
          inputPayload = "",
          logPath = "/",
        ),
      ),
    )

    workloadHandler.failWorkload(workloadId)
    verify(exactly = 0) { workloadRepository.update(eq(workloadId), eq(WorkloadStatus.failure)) }
  }

  @Test
  fun `test workload not found when succeeding workload`() {
    every { workloadRepository.findById(workloadId) }.returns(Optional.empty())
    assertThrows<NotFoundException> { workloadHandler.succeedWorkload(workloadId) }
  }

  @ParameterizedTest
  @EnumSource(value = WorkloadStatus::class, names = ["pending", "cancelled", "failure"])
  fun `test succeed workload in inactive status`(workloadStatus: WorkloadStatus) {
    every { workloadRepository.findById(workloadId) }.returns(
      Optional.of(
        Workload(
          id = workloadId,
          dataplaneId = null,
          status = workloadStatus,
          lastHeartbeatAt = null,
          workloadLabels = listOf(),
          inputPayload = "",
          logPath = "/",
        ),
      ),
    )

    assertThrows<InvalidStatusTransitionException> { workloadHandler.succeedWorkload(workloadId) }
  }

  @ParameterizedTest
  @EnumSource(value = WorkloadStatus::class, names = ["claimed", "running"])
  fun `test succeeding workload succeeded`(workloadStatus: WorkloadStatus) {
    every { workloadRepository.findById(workloadId) }.returns(
      Optional.of(
        Workload(
          id = workloadId,
          dataplaneId = null,
          status = workloadStatus,
          lastHeartbeatAt = null,
          workloadLabels = listOf(),
          inputPayload = "",
          logPath = "/",
        ),
      ),
    )

    every { workloadRepository.update(any(), ofType(WorkloadStatus::class)) } just Runs

    workloadHandler.succeedWorkload(workloadId)
    verify { workloadRepository.update(eq(workloadId), eq(WorkloadStatus.success)) }
  }

  @Test
  fun `test noop success`() {
    every { workloadRepository.findById(workloadId) }.returns(
      Optional.of(
        Workload(
          id = workloadId,
          dataplaneId = null,
          status = WorkloadStatus.success,
          lastHeartbeatAt = null,
          workloadLabels = listOf(),
          inputPayload = "",
          logPath = "/",
        ),
      ),
    )

    workloadHandler.succeedWorkload(workloadId)
    verify(exactly = 0) { workloadRepository.update(eq(workloadId), eq(WorkloadStatus.success)) }
  }

  @Test
  fun `test workload not found when setting status to running`() {
    every { workloadRepository.findById(workloadId) }.returns(Optional.empty())
    assertThrows<NotFoundException> { workloadHandler.setWorkloadStatusToRunning(workloadId) }
  }

  @ParameterizedTest
  @EnumSource(value = WorkloadStatus::class, names = ["success", "cancelled", "failure"])
  fun `test set workload status to running when workload is in terminal state`(workloadStatus: WorkloadStatus) {
    every { workloadRepository.findById(workloadId) }.returns(
      Optional.of(
        Workload(
          id = workloadId,
          dataplaneId = null,
          status = workloadStatus,
          lastHeartbeatAt = null,
          workloadLabels = listOf(),
          inputPayload = "",
          logPath = "/",
        ),
      ),
    )

    assertThrows<InvalidStatusTransitionException> { workloadHandler.setWorkloadStatusToRunning(workloadId) }
  }

  @Test
  fun `test set workload status to running on unclaimed workload`() {
    every { workloadRepository.findById(workloadId) }.returns(
      Optional.of(
        Workload(
          id = workloadId,
          dataplaneId = null,
          status = WorkloadStatus.pending,
          lastHeartbeatAt = null,
          workloadLabels = listOf(),
          inputPayload = "",
          logPath = "/",
        ),
      ),
    )

    assertThrows<InvalidStatusTransitionException> { workloadHandler.setWorkloadStatusToRunning(workloadId) }
  }

  @Test
  fun `test set workload status to running succeeded`() {
    every { workloadRepository.findById(workloadId) }.returns(
      Optional.of(
        Workload(
          id = workloadId,
          dataplaneId = null,
          status = WorkloadStatus.claimed,
          lastHeartbeatAt = null,
          workloadLabels = listOf(),
          inputPayload = "",
          logPath = "/",
        ),
      ),
    )

    every { workloadRepository.update(any(), ofType(WorkloadStatus::class)) } just Runs

    workloadHandler.succeedWorkload(workloadId)
    verify { workloadRepository.update(eq(workloadId), eq(WorkloadStatus.success)) }
  }

  @Test
  fun `test noop when setting workload status to running`() {
    every { workloadRepository.findById(workloadId) }.returns(
      Optional.of(
        Workload(
          id = workloadId,
          dataplaneId = null,
          status = WorkloadStatus.success,
          lastHeartbeatAt = null,
          workloadLabels = listOf(),
          inputPayload = "",
          logPath = "/",
        ),
      ),
    )

    workloadHandler.succeedWorkload(workloadId)
    verify(exactly = 0) { workloadRepository.update(eq(workloadId), eq(WorkloadStatus.success)) }
  }
}
