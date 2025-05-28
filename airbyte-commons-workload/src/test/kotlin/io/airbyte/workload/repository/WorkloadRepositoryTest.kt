/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.repository

import io.airbyte.db.factory.DSLContextFactory
import io.airbyte.db.instance.DatabaseConstants
import io.airbyte.db.instance.test.TestDatabaseProviders
import io.airbyte.workload.repository.WorkloadRepositoryTest.Fixtures.defaultDeadline
import io.airbyte.workload.repository.domain.Workload
import io.airbyte.workload.repository.domain.WorkloadLabel
import io.airbyte.workload.repository.domain.WorkloadStatus
import io.airbyte.workload.repository.domain.WorkloadType
import io.micronaut.context.ApplicationContext
import io.micronaut.context.env.PropertySource
import io.micronaut.data.connection.jdbc.advice.DelegatingDataSource
import org.jooq.DSLContext
import org.jooq.SQLDialect
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.EnumSource
import org.testcontainers.containers.PostgreSQLContainer
import java.time.OffsetDateTime
import java.time.temporal.ChronoUnit
import java.util.UUID
import javax.sql.DataSource

class WorkloadRepositoryTest {
  @AfterEach
  fun cleanDb() {
    workloadLabelRepo.deleteAll()
    workloadQueueRepo.deleteAll()
    workloadRepo.deleteAll()
  }

  @ParameterizedTest
  @EnumSource(WorkloadStatus::class, names = ["PENDING", "CLAIMED", "LAUNCHED", "RUNNING"])
  fun `cancel a non terminal workload updates the status to cancel and clears the deadline`(status: WorkloadStatus) {
    val workloadId = Fixtures.newWorkloadId()
    val reason = "cancel reason"
    val source = "cancel test"
    val workload =
      Fixtures.workload(
        id = workloadId,
        dataplaneId = null,
        status = status,
        deadline = OffsetDateTime.now(),
      )
    workloadRepo.save(workload)

    val safeguardWorkloadId = Fixtures.newWorkloadId()
    val safeguardWorkload = workload.copy(id = safeguardWorkloadId)
    workloadRepo.save(safeguardWorkload)

    val actualWorkload = workloadRepo.cancel(workloadId, reason, source)

    assertEquals(workloadId, actualWorkload?.id)
    assertEquals(WorkloadStatus.CANCELLED, actualWorkload?.status)
    assertEquals(reason, actualWorkload?.terminationReason)
    assertEquals(source, actualWorkload?.terminationSource)

    assertNull(actualWorkload?.deadline)

    val safeguardCheck = workloadRepo.findById(safeguardWorkloadId)
    assertEquals(safeguardWorkload.status, safeguardCheck.get().status)
  }

  @ParameterizedTest
  @EnumSource(WorkloadStatus::class, names = ["CANCELLED", "FAILURE", "SUCCESS"])
  fun `cancel a terminal workload doesn't update the workload and returns null`(status: WorkloadStatus) {
    val workloadId = Fixtures.newWorkloadId()
    val workload =
      Fixtures.workload(
        id = workloadId,
        dataplaneId = null,
        status = status,
        deadline = OffsetDateTime.now(),
      )
    workloadRepo.save(workload)
    val expected = workloadRepo.findById(workloadId).get()

    val safeguardWorkloadId = Fixtures.newWorkloadId()
    val safeguardWorkload = workload.copy(id = safeguardWorkloadId)
    workloadRepo.save(safeguardWorkload)

    val cancelResponse = workloadRepo.cancel(workloadId, "reason", "source")
    assertNull(cancelResponse)

    val actualWorkload = workloadRepo.findById(workloadId).get()
    assertEquals(expected, actualWorkload)

    val safeguardCheck = workloadRepo.findById(safeguardWorkloadId)
    assertEquals(safeguardWorkload.status, safeguardCheck.get().status)
  }

  @Test
  fun `claiming a pending workload marks the workload as claimed by the dataplane`() {
    val workloadId = Fixtures.newWorkloadId()
    val workload =
      Fixtures.workload(
        id = workloadId,
        dataplaneId = null,
        status = WorkloadStatus.PENDING,
      )
    workloadRepo.save(workload)
    val newDeadline = Fixtures.newTimestamp().plusMinutes(10)
    val dataplaneId = "my-data-plane"
    val actualWorkload = workloadRepo.claim(workloadId, dataplaneId, newDeadline)

    assertEquals(workloadId, actualWorkload?.id)
    assertEquals(dataplaneId, actualWorkload?.dataplaneId)
    assertEquals(WorkloadStatus.CLAIMED, actualWorkload?.status)
  }

  @Test
  fun `claiming a workload that has been claimed by another dataplane fails`() {
    val workloadId = Fixtures.newWorkloadId()
    val otherDataplaneId = "my-other-dataplane"
    val originalDeadline = Fixtures.newTimestamp().withNano(0).plusMinutes(5)
    val workload =
      Fixtures.workload(
        id = workloadId,
        dataplaneId = otherDataplaneId,
        status = WorkloadStatus.CLAIMED,
        deadline = originalDeadline,
      )
    workloadRepo.save(workload)
    val newDeadline = Fixtures.newTimestamp().plusMinutes(10)
    val dataplaneId = "my-data-plane"
    val claimResult = workloadRepo.claim(workloadId, dataplaneId, newDeadline)
    assertNull(claimResult)

    val actualWorkload = workloadRepo.findById(workloadId).get()
    assertEquals(otherDataplaneId, actualWorkload.dataplaneId)
    assertEquals(WorkloadStatus.CLAIMED, actualWorkload.status)
    assertEquals(originalDeadline, actualWorkload.deadline)
  }

  @Test
  fun `claiming a workload that has been claimed by the same dataplane succeeds and doesn't update the deadline`() {
    val workloadId = Fixtures.newWorkloadId()
    val dataplaneId = "my-data-plane"
    val originalDeadline = Fixtures.newTimestamp().withNano(0).plusMinutes(5)
    val workload =
      Fixtures.workload(
        id = workloadId,
        dataplaneId = dataplaneId,
        status = WorkloadStatus.CLAIMED,
        deadline = originalDeadline,
      )
    workloadRepo.save(workload)
    val newDeadline = Fixtures.newTimestamp().plusMinutes(10)
    val actualWorkload = workloadRepo.claim(workloadId, dataplaneId, newDeadline)

    assertEquals(workloadId, actualWorkload?.id)
    assertEquals(dataplaneId, actualWorkload?.dataplaneId)
    assertEquals(WorkloadStatus.CLAIMED, actualWorkload?.status)
    // note that we do not refresh the deadline in this case
    assertEquals(originalDeadline, actualWorkload?.deadline)
  }

  @Test
  fun `claiming workload that is running on the same dataplane fails`() {
    val workloadId = Fixtures.newWorkloadId()
    val dataplaneId = "my-data-plane"
    val originalDeadline = Fixtures.newTimestamp().withNano(0).plusMinutes(5)
    val workload =
      Fixtures.workload(
        id = workloadId,
        dataplaneId = dataplaneId,
        status = WorkloadStatus.RUNNING,
        deadline = originalDeadline,
      )
    workloadRepo.save(workload)
    val newDeadline = Fixtures.newTimestamp().plusMinutes(10)
    val claimResult = workloadRepo.claim(workloadId, dataplaneId, newDeadline)
    assertNull(claimResult)

    val actualWorkload = workloadRepo.findById(workloadId).get()
    // Verifying we didn't update the workload in this case, deadline should remain the original one.
    assertEquals(workloadId, actualWorkload.id)
    assertEquals(dataplaneId, actualWorkload.dataplaneId)
    assertEquals(WorkloadStatus.RUNNING, actualWorkload.status)
    // note that we do not refresh the deadline in this case
    assertEquals(originalDeadline, actualWorkload.deadline)
  }

  @ParameterizedTest
  @EnumSource(WorkloadStatus::class, names = ["PENDING", "CLAIMED", "LAUNCHED", "RUNNING"])
  fun `failing a non terminal workload updates the status to cancel and clears the deadline`(status: WorkloadStatus) {
    val workloadId = Fixtures.newWorkloadId()
    val reason = "failure reason"
    val source = "failure test"
    val workload =
      Fixtures.workload(
        id = workloadId,
        dataplaneId = null,
        status = status,
        deadline = OffsetDateTime.now(),
      )
    workloadRepo.save(workload)

    val safeguardWorkloadId = Fixtures.newWorkloadId()
    val safeguardWorkload = workload.copy(id = safeguardWorkloadId)
    workloadRepo.save(safeguardWorkload)

    val actualWorkload = workloadRepo.fail(workloadId, reason, source)

    assertEquals(workloadId, actualWorkload?.id)
    assertEquals(WorkloadStatus.FAILURE, actualWorkload?.status)
    assertNull(actualWorkload?.deadline)
    assertEquals(reason, actualWorkload?.terminationReason)
    assertEquals(source, actualWorkload?.terminationSource)

    val safeguardCheck = workloadRepo.findById(safeguardWorkloadId)
    assertEquals(safeguardWorkload.status, safeguardCheck.get().status)
  }

  @ParameterizedTest
  @EnumSource(WorkloadStatus::class, names = ["CANCELLED", "FAILURE", "SUCCESS"])
  fun `failing a terminal workload doesn't update the workload and returns null`(status: WorkloadStatus) {
    val workloadId = Fixtures.newWorkloadId()
    val workload =
      Fixtures.workload(
        id = workloadId,
        dataplaneId = null,
        status = status,
        deadline = OffsetDateTime.now(),
      )
    workloadRepo.save(workload)
    val expected = workloadRepo.findById(workloadId).get()

    val safeguardWorkloadId = Fixtures.newWorkloadId()
    val safeguardWorkload = workload.copy(id = safeguardWorkloadId)
    workloadRepo.save(safeguardWorkload)

    val response = workloadRepo.fail(workloadId, "reason", "source")
    assertNull(response)

    val actualWorkload = workloadRepo.findById(workloadId).get()
    assertEquals(expected, actualWorkload)

    val safeguardCheck = workloadRepo.findById(safeguardWorkloadId)
    assertEquals(safeguardWorkload.status, safeguardCheck.get().status)
  }

  @Test
  fun `saving a workload writes all the expected fields`() {
    val workloadId = Fixtures.newWorkloadId()
    val label1 =
      WorkloadLabel(
        id = null,
        key = "key1",
        value = "value1",
        workload = null,
      )
    val label2 =
      WorkloadLabel(
        id = null,
        key = "key2",
        value = "value2",
        workload = null,
      )
    val labels = ArrayList<WorkloadLabel>()
    labels.add(label1)
    labels.add(label2)
    val signalInput = "signalInput"
    val dataplaneGroup = "dataplane-group-1"
    val priority = 0
    val workspaceId = UUID.randomUUID()
    val organizationId = UUID.randomUUID()
    val workload =
      Fixtures.workload(
        id = workloadId,
        dataplaneId = null,
        status = WorkloadStatus.PENDING,
        workloadLabels = labels,
        workspaceId = workspaceId,
        organizationId = organizationId,
        deadline = defaultDeadline,
        signalInput = signalInput,
        dataplaneGroup = dataplaneGroup,
        priority = priority,
      )
    workloadRepo.save(workload)
    val persistedWorkload = workloadRepo.findById(workloadId)
    assertTrue(persistedWorkload.isPresent)
    assertNull(persistedWorkload.get().dataplaneId)
    assertEquals(WorkloadStatus.PENDING, persistedWorkload.get().status)
    assertNotNull(persistedWorkload.get().createdAt)
    assertNotNull(persistedWorkload.get().updatedAt)
    assertNull(persistedWorkload.get().lastHeartbeatAt)
    assertNotNull(persistedWorkload.get().deadline)
    assertEquals(defaultDeadline.toEpochSecond(), persistedWorkload.get().deadline!!.toEpochSecond())
    assertEquals(2, persistedWorkload.get().workloadLabels!!.size)
    assertEquals(signalInput, persistedWorkload.get().signalInput)
    assertEquals(dataplaneGroup, persistedWorkload.get().dataplaneGroup)
    assertEquals(priority, persistedWorkload.get().priority)
    assertEquals(workspaceId, persistedWorkload.get().workspaceId)
    assertEquals(organizationId, persistedWorkload.get().organizationId)

    val workloadLabels = persistedWorkload.get().workloadLabels!!.toMutableList()
    workloadLabels.sortWith(Comparator.comparing(WorkloadLabel::key))
    assertEquals("key1", workloadLabels[0].key)
    assertEquals("value1", workloadLabels[0].value)
    assertNotNull(workloadLabels[0].id)
    assertEquals("key2", workloadLabels[1].key)
    assertEquals("value2", workloadLabels[1].value)
    assertNotNull(workloadLabels[1].id)
  }

  @ParameterizedTest
  @EnumSource(WorkloadStatus::class, names = ["CLAIMED", "LAUNCHED", "RUNNING"])
  fun `heartbeat a workload updates the status if the workload was previously claimed, launched or running`(status: WorkloadStatus) {
    val workloadId = Fixtures.newWorkloadId()
    val workload =
      Fixtures.workload(
        id = workloadId,
        dataplaneId = null,
        status = status,
        deadline = OffsetDateTime.now(),
      )
    workloadRepo.save(workload)

    val safeguardWorkloadId = Fixtures.newWorkloadId()
    val safeguardWorkload = workload.copy(id = safeguardWorkloadId)
    workloadRepo.save(safeguardWorkload)

    val newDeadline = OffsetDateTime.now().plusMinutes(10)
    val actualWorkload = workloadRepo.heartbeat(workloadId, deadline = newDeadline)

    assertEquals(workloadId, actualWorkload?.id)
    assertEquals(WorkloadStatus.RUNNING, actualWorkload?.status)
    assertEquals(newDeadline.truncateToTestPrecision(), actualWorkload?.deadline?.truncateToTestPrecision())
    assertNotEquals(workload.lastHeartbeatAt?.truncateToTestPrecision(), actualWorkload?.lastHeartbeatAt?.truncateToTestPrecision())

    val safeguardCheck = workloadRepo.findById(safeguardWorkloadId)
    assertEquals(safeguardWorkload.status, safeguardCheck.get().status)
  }

  @ParameterizedTest
  @EnumSource(WorkloadStatus::class, names = ["PENDING", "CANCELLED", "FAILURE", "SUCCESS"])
  fun `heartbeat a workload that isn't claimed or launched doesn't update the workload and returns null`(status: WorkloadStatus) {
    val workloadId = Fixtures.newWorkloadId()
    val workload =
      Fixtures.workload(
        id = workloadId,
        dataplaneId = null,
        status = status,
        deadline = OffsetDateTime.now(),
      )
    workloadRepo.save(workload)
    val expected = workloadRepo.findById(workloadId).get()

    val safeguardWorkloadId = Fixtures.newWorkloadId()
    val safeguardWorkload = workload.copy(id = safeguardWorkloadId)
    workloadRepo.save(safeguardWorkload)

    val response = workloadRepo.heartbeat(workloadId, OffsetDateTime.now().plusMinutes(5))
    assertNull(response)

    val actualWorkload = workloadRepo.findById(workloadId).get()
    assertEquals(expected, actualWorkload)

    val safeguardCheck = workloadRepo.findById(safeguardWorkloadId)
    assertEquals(safeguardWorkload.status, safeguardCheck.get().status)
  }

  @ParameterizedTest
  @EnumSource(WorkloadStatus::class, names = ["CLAIMED", "LAUNCHED"])
  fun `launch a workload updates the status if the workload was previously claimed or launched`(status: WorkloadStatus) {
    val workloadId = Fixtures.newWorkloadId()
    val workload =
      Fixtures.workload(
        id = workloadId,
        dataplaneId = null,
        status = status,
        deadline = OffsetDateTime.now(),
      )
    workloadRepo.save(workload)

    val safeguardWorkloadId = Fixtures.newWorkloadId()
    val safeguardWorkload = workload.copy(id = safeguardWorkloadId)
    workloadRepo.save(safeguardWorkload)

    val newDeadline = OffsetDateTime.now().plusMinutes(10)
    val actualWorkload = workloadRepo.launch(workloadId, deadline = newDeadline)

    assertEquals(workloadId, actualWorkload?.id)
    assertEquals(WorkloadStatus.LAUNCHED, actualWorkload?.status)
    assertEquals(newDeadline.truncateToTestPrecision(), actualWorkload?.deadline?.truncateToTestPrecision())

    val safeguardCheck = workloadRepo.findById(safeguardWorkloadId)
    assertEquals(safeguardWorkload.status, safeguardCheck.get().status)
  }

  @ParameterizedTest
  @EnumSource(WorkloadStatus::class, names = ["PENDING", "RUNNING", "CANCELLED", "FAILURE", "SUCCESS"])
  fun `launch a workload that isn't claimed or launched doesn't update the workload and returns null`(status: WorkloadStatus) {
    val workloadId = Fixtures.newWorkloadId()
    val workload =
      Fixtures.workload(
        id = workloadId,
        dataplaneId = null,
        status = status,
        deadline = OffsetDateTime.now(),
      )
    workloadRepo.save(workload)
    val expected = workloadRepo.findById(workloadId).get()

    val safeguardWorkloadId = Fixtures.newWorkloadId()
    val safeguardWorkload = workload.copy(id = safeguardWorkloadId)
    workloadRepo.save(safeguardWorkload)

    val response = workloadRepo.launch(workloadId, OffsetDateTime.now().plusMinutes(5))
    assertNull(response)

    val actualWorkload = workloadRepo.findById(workloadId).get()
    assertEquals(expected, actualWorkload)

    val safeguardCheck = workloadRepo.findById(safeguardWorkloadId)
    assertEquals(safeguardWorkload.status, safeguardCheck.get().status)
  }

  @ParameterizedTest
  @EnumSource(WorkloadStatus::class, names = ["CLAIMED", "LAUNCHED", "RUNNING"])
  fun `running updates the status if the workload was previously claimed, launched or running`(status: WorkloadStatus) {
    val workloadId = Fixtures.newWorkloadId()
    val workload =
      Fixtures.workload(
        id = workloadId,
        dataplaneId = null,
        status = status,
        deadline = OffsetDateTime.now(),
      )
    workloadRepo.save(workload)

    val safeguardWorkloadId = Fixtures.newWorkloadId()
    val safeguardWorkload = workload.copy(id = safeguardWorkloadId)
    workloadRepo.save(safeguardWorkload)

    val newDeadline = OffsetDateTime.now().plusMinutes(10)
    val actualWorkload = workloadRepo.running(workloadId, deadline = newDeadline)

    assertEquals(workloadId, actualWorkload?.id)
    assertEquals(WorkloadStatus.RUNNING, actualWorkload?.status)
    assertEquals(newDeadline.truncateToTestPrecision(), actualWorkload?.deadline?.truncateToTestPrecision())

    val safeguardCheck = workloadRepo.findById(safeguardWorkloadId)
    assertEquals(safeguardWorkload.status, safeguardCheck.get().status)
  }

  @ParameterizedTest
  @EnumSource(WorkloadStatus::class, names = ["PENDING", "CANCELLED", "FAILURE", "SUCCESS"])
  fun `running for a workload that isn't claimed, launched or running doesn't update the workload and returns null`(status: WorkloadStatus) {
    val workloadId = Fixtures.newWorkloadId()
    val workload =
      Fixtures.workload(
        id = workloadId,
        dataplaneId = null,
        status = status,
        deadline = OffsetDateTime.now(),
      )
    workloadRepo.save(workload)
    val expected = workloadRepo.findById(workloadId).get()

    val safeguardWorkloadId = Fixtures.newWorkloadId()
    val safeguardWorkload = workload.copy(id = safeguardWorkloadId)
    workloadRepo.save(safeguardWorkload)

    val response = workloadRepo.running(workloadId, OffsetDateTime.now().plusMinutes(5))
    assertNull(response)

    val actualWorkload = workloadRepo.findById(workloadId).get()
    assertEquals(expected, actualWorkload)

    val safeguardCheck = workloadRepo.findById(safeguardWorkloadId)
    assertEquals(safeguardWorkload.status, safeguardCheck.get().status)
  }

  @ParameterizedTest
  @EnumSource(WorkloadStatus::class, names = ["PENDING", "CLAIMED", "LAUNCHED", "RUNNING"])
  fun `succeeding a non terminal workload updates the status to cancel and clears the deadline`(status: WorkloadStatus) {
    val workloadId = Fixtures.newWorkloadId()
    val workload =
      Fixtures.workload(
        id = workloadId,
        dataplaneId = null,
        status = status,
        deadline = OffsetDateTime.now(),
      )
    workloadRepo.save(workload)

    val safeguardWorkloadId = Fixtures.newWorkloadId()
    val safeguardWorkload = workload.copy(id = safeguardWorkloadId)
    workloadRepo.save(safeguardWorkload)

    val actualWorkload = workloadRepo.succeed(workloadId)

    assertEquals(workloadId, actualWorkload?.id)
    assertEquals(WorkloadStatus.SUCCESS, actualWorkload?.status)
    assertNull(actualWorkload?.deadline)

    val safeguardCheck = workloadRepo.findById(safeguardWorkloadId)
    assertEquals(safeguardWorkload.status, safeguardCheck.get().status)
  }

  @ParameterizedTest
  @EnumSource(WorkloadStatus::class, names = ["CANCELLED", "FAILURE", "SUCCESS"])
  fun `succeeding a terminal workload doesn't update the workload and returns null`(status: WorkloadStatus) {
    val workloadId = Fixtures.newWorkloadId()
    val workload =
      Fixtures.workload(
        id = workloadId,
        dataplaneId = null,
        status = status,
        deadline = OffsetDateTime.now(),
      )
    workloadRepo.save(workload)
    val expected = workloadRepo.findById(workloadId).get()

    val safeguardWorkloadId = Fixtures.newWorkloadId()
    val safeguardWorkload = workload.copy(id = safeguardWorkloadId)
    workloadRepo.save(safeguardWorkload)

    val response = workloadRepo.succeed(workloadId)
    assertNull(response)

    val actualWorkload = workloadRepo.findById(workloadId).get()
    assertEquals(expected, actualWorkload)

    val safeguardCheck = workloadRepo.findById(safeguardWorkloadId)
    assertEquals(safeguardWorkload.status, safeguardCheck.get().status)
  }

  @Test
  fun `searchByMutexKeyAndStatusInList returns the workloads matching the search`() {
    val mutexKey = "mutex-search-test"
    val workload1 =
      Fixtures.workload(
        id = "workload-mutex-search-1",
        status = WorkloadStatus.PENDING,
        mutexKey = mutexKey,
      )
    workloadRepo.save(workload1)

    val match = workloadRepo.searchByMutexKeyAndStatusInList(mutexKey, listOf(WorkloadStatus.PENDING, WorkloadStatus.RUNNING))
    assertEquals(1, match.size)
    assertEquals(workload1.id, match[0].id)

    val emptyResult = workloadRepo.searchByMutexKeyAndStatusInList(mutexKey, listOf(WorkloadStatus.CLAIMED, WorkloadStatus.RUNNING))
    assertEquals(0, emptyResult.size)

    val mutexMismatch = workloadRepo.searchByMutexKeyAndStatusInList("mismatch", listOf(WorkloadStatus.PENDING, WorkloadStatus.RUNNING))
    assertEquals(0, mutexMismatch.size)
  }

  @Test
  fun `test search`() {
    val workload1 =
      Fixtures.workload(
        id = "1-${Fixtures.newWorkloadId()}",
        dataplaneId = "dataplane1",
        status = WorkloadStatus.FAILURE,
      )
    val workload2 =
      Fixtures.workload(
        id = "2-${Fixtures.newWorkloadId()}",
        dataplaneId = "dataplane2",
        status = WorkloadStatus.SUCCESS,
      )
    workloadRepo.save(workload1)
    workloadRepo.save(workload2)
    val now = Fixtures.newTimestamp()
    val resulSearch1 = sortedSearch(null, null, now.plusDays(1))
    assertEquals(2, resulSearch1.size)
    assertEquals(workload1.id, resulSearch1[0].id)
    assertEquals(workload2.id, resulSearch1[1].id)

    val resultSearch2 = sortedSearch(null, listOf(WorkloadStatus.FAILURE, WorkloadStatus.SUCCESS), null)
    assertEquals(2, resultSearch2.size)
    assertEquals(workload1.id, resultSearch2[0].id)
    assertEquals(workload2.id, resultSearch2[1].id)

    val resultSearch3 = sortedSearch(listOf("dataplane1", "dataplane2"), null, null)
    assertEquals(2, resultSearch3.size)
    assertEquals(workload1.id, resultSearch3[0].id)
    assertEquals(workload2.id, resultSearch3[1].id)

    val resultSearch4 =
      sortedSearch(
        listOf("dataplane1", "dataplane2"),
        listOf(WorkloadStatus.FAILURE, WorkloadStatus.SUCCESS),
        now.plusDays(1),
      )
    assertEquals(2, resultSearch4.size)
    assertEquals(workload1.id, resultSearch4[0].id)
    assertEquals(workload2.id, resultSearch4[1].id)

    val resultSearch5 = sortedSearch(null, listOf(WorkloadStatus.FAILURE), now.plusDays(1))
    assertEquals(1, resultSearch5.size)
    assertEquals(workload1.id, resultSearch5[0].id)

    val resultSearch6 = sortedSearch(listOf("dataplane1"), null, now.plusDays(1))
    assertEquals(1, resultSearch6.size)
    assertEquals(workload1.id, resultSearch6[0].id)

    val resultSearch7 = sortedSearch(listOf("dataplane1", "dataplane2"), listOf(WorkloadStatus.FAILURE), null)
    assertEquals(1, resultSearch7.size)
    assertEquals(workload1.id, resultSearch7[0].id)

    val resultSearch8 = sortedSearch(listOf("dataplane1"), listOf(WorkloadStatus.FAILURE, WorkloadStatus.SUCCESS), now.plusDays(1))
    assertEquals(1, resultSearch8.size)
    assertEquals(workload1.id, resultSearch8[0].id)

    val resultSearch9 =
      sortedSearch(
        listOf("dataplane1", "dataplane2"),
        listOf(WorkloadStatus.FAILURE, WorkloadStatus.SUCCESS),
        now.minusDays(1),
      )
    assertEquals(0, resultSearch9.size)

    val resultSearch10 = sortedSearch(listOf("dataplane1", "dataplane2"), listOf(WorkloadStatus.CLAIMED), now.plusDays(1))
    assertEquals(0, resultSearch10.size)

    val resultSearch11 = sortedSearch(listOf("fakeDataplane"), listOf(WorkloadStatus.FAILURE, WorkloadStatus.SUCCESS), now.plusDays(1))
    assertEquals(0, resultSearch11.size)
  }

  @Test
  fun `test search by type status and creation date`() {
    val workload1 =
      Fixtures.workload(
        id = "1-${Fixtures.newWorkloadId()}",
        dataplaneId = "dataplane1",
        status = WorkloadStatus.RUNNING,
        type = WorkloadType.CHECK,
      )
    val workload2 =
      Fixtures.workload(
        id = "2-${Fixtures.newWorkloadId()}",
        dataplaneId = "dataplane2",
        status = WorkloadStatus.SUCCESS,
        type = WorkloadType.SYNC,
      )
    workloadRepo.save(workload1)
    workloadRepo.save(workload2)
    val now = Fixtures.newTimestamp()
    var resultSearch = sortedSearchByTypeStatusCreatedDate(null, null, null, now.plusDays(1))
    assertEquals(2, resultSearch.size)
    assertEquals(workload1.id, resultSearch[0].id)
    assertEquals(workload2.id, resultSearch[1].id)

    resultSearch = sortedSearchByTypeStatusCreatedDate(null, null, listOf(WorkloadType.CHECK), now.plusDays(1))
    assertEquals(1, resultSearch.size)
    assertEquals(workload1.id, resultSearch[0].id)

    resultSearch = sortedSearchByTypeStatusCreatedDate(null, null, listOf(WorkloadType.SPEC), now.plusDays(1))
    assertEquals(0, resultSearch.size)

    resultSearch = sortedSearchByTypeStatusCreatedDate(null, listOf(WorkloadStatus.RUNNING), null, now.plusDays(1))
    assertEquals(1, resultSearch.size)
    assertEquals(workload1.id, resultSearch[0].id)

    resultSearch = sortedSearchByTypeStatusCreatedDate(null, listOf(WorkloadStatus.CANCELLED), null, now.plusDays(1))
    assertEquals(0, resultSearch.size)

    resultSearch = sortedSearchByTypeStatusCreatedDate(null, null, null, now.minusDays(1))
    assertEquals(0, resultSearch.size)

    resultSearch = sortedSearchByTypeStatusCreatedDate(null, listOf(WorkloadStatus.RUNNING), listOf(WorkloadType.SYNC), now.plusDays(1))
    assertEquals(0, resultSearch.size)
  }

  @Test
  fun `test search by type expired deadline`() {
    val deadline: OffsetDateTime = Fixtures.newTimestamp()
    val workload1 =
      Fixtures.workload(
        id = "1-${Fixtures.newWorkloadId()}",
        dataplaneId = "dataplane1",
        status = WorkloadStatus.RUNNING,
        type = WorkloadType.CHECK,
        deadline = deadline,
      )
    val workload2 =
      Fixtures.workload(
        id = "2-${Fixtures.newWorkloadId()}",
        dataplaneId = "dataplane2",
        status = WorkloadStatus.CLAIMED,
        type = WorkloadType.SYNC,
        deadline = deadline,
      )
    workloadRepo.save(workload1)
    workloadRepo.save(workload2)
    var resultSearch = sortedSearchByExpiredDeadline(null, null, deadline.minusDays(1))
    assertEquals(0, resultSearch.size)

    resultSearch = sortedSearchByExpiredDeadline(null, null, deadline)
    assertEquals(0, resultSearch.size)

    resultSearch = sortedSearchByExpiredDeadline(null, null, deadline.plusDays(1))
    assertEquals(2, resultSearch.size)
    assertEquals(workload1.id, resultSearch[0].id)
    assertEquals(workload2.id, resultSearch[1].id)

    resultSearch = sortedSearchByExpiredDeadline(null, listOf(WorkloadStatus.RUNNING), deadline.minusDays(1))
    assertEquals(0, resultSearch.size)

    resultSearch = sortedSearchByExpiredDeadline(null, listOf(WorkloadStatus.CANCELLED), deadline.minusDays(1))
    assertEquals(0, resultSearch.size)

    resultSearch = sortedSearchByExpiredDeadline(null, listOf(WorkloadStatus.RUNNING), deadline.plusDays(1))
    assertEquals(1, resultSearch.size)
    assertEquals(workload1.id, resultSearch[0].id)

    resultSearch = sortedSearchByExpiredDeadline(null, listOf(WorkloadStatus.CANCELLED), deadline.plusDays(1))
    assertEquals(0, resultSearch.size)

    resultSearch = sortedSearchByExpiredDeadline(listOf("dataplane1", "dataplane2"), listOf(WorkloadStatus.RUNNING), deadline.minusDays(1))
    assertEquals(0, resultSearch.size)

    resultSearch = sortedSearchByExpiredDeadline(listOf("dataplane1", "dataplane2"), listOf(WorkloadStatus.RUNNING), deadline)
    assertEquals(0, resultSearch.size)

    resultSearch = sortedSearchByExpiredDeadline(listOf("dataplane1"), listOf(WorkloadStatus.RUNNING), deadline.plusDays(1))
    assertEquals(1, resultSearch.size)
    assertEquals(workload1.id, resultSearch[0].id)

    resultSearch = sortedSearchByExpiredDeadline(listOf("dataplane2"), listOf(WorkloadStatus.RUNNING), deadline.plusDays(1))
    assertEquals(0, resultSearch.size)

    resultSearch = sortedSearchByExpiredDeadline(listOf("dataplane2"), listOf(WorkloadStatus.CLAIMED), deadline.plusDays(1))
    assertEquals(1, resultSearch.size)
    assertEquals(workload2.id, resultSearch[0].id)

    resultSearch = sortedSearchByExpiredDeadline(listOf("dataplane1", "dataplane2"), listOf(WorkloadStatus.RUNNING), deadline.plusDays(1))
    assertEquals(1, resultSearch.size)
    assertEquals(workload1.id, resultSearch[0].id)

    resultSearch = sortedSearchByExpiredDeadline(listOf("dataplane1", "dataplane2"), null, deadline.plusDays(1))
    assertEquals(2, resultSearch.size)
    assertEquals(workload1.id, resultSearch[0].id)
    assertEquals(workload2.id, resultSearch[1].id)
  }

  object Fixtures {
    fun newWorkloadId() = "${UUID.randomUUID()}_test"

    val defaultDeadline: OffsetDateTime = newTimestamp()

    fun workload(
      id: String = newWorkloadId(),
      dataplaneId: String? = null,
      status: WorkloadStatus = WorkloadStatus.PENDING,
      workloadLabels: List<WorkloadLabel>? = null,
      inputPayload: String = "",
      workspaceId: UUID? = UUID.randomUUID(),
      organizationId: UUID? = UUID.randomUUID(),
      logPath: String = "/",
      mutexKey: String = "",
      type: WorkloadType = WorkloadType.SYNC,
      deadline: OffsetDateTime = newTimestamp(),
      signalInput: String = "",
      dataplaneGroup: String = "",
      priority: Int = 0,
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
        deadline = deadline.truncateToTestPrecision(),
        signalInput = signalInput,
        dataplaneGroup = dataplaneGroup,
        priority = priority,
      )

    /**
     * Generate a timestamp with truncated precision to ensure accurate test comparisons.
     */
    fun newTimestamp(): OffsetDateTime = OffsetDateTime.now().truncateToTestPrecision()
  }

  companion object {
    private lateinit var context: ApplicationContext
    lateinit var workloadRepo: WorkloadRepository
    lateinit var workloadLabelRepo: WorkloadLabelRepository
    lateinit var workloadQueueRepo: WorkloadQueueRepository
    private lateinit var jooqDslContext: DSLContext

    // we run against an actual database to ensure micronaut data and jooq properly integrate
    private val container: PostgreSQLContainer<*> =
      PostgreSQLContainer(DatabaseConstants.DEFAULT_DATABASE_VERSION)
        .withDatabaseName("airbyte")
        .withUsername("docker")
        .withPassword("docker")

    @BeforeAll
    @JvmStatic
    fun setup() {
      container.start()
      // set the micronaut datasource properties to match our container we started up
      context =
        ApplicationContext.run(
          PropertySource.of(
            "test",
            mapOf(
              "datasources.config.driverClassName" to "org.postgresql.Driver",
              "datasources.config.db-type" to "postgres",
              "datasources.config.dialect" to "POSTGRES",
              "datasources.config.url" to container.jdbcUrl,
              "datasources.config.username" to container.username,
              "datasources.config.password" to container.password,
            ),
          ),
        )

      // removes micronaut transactional wrapper that doesn't play nice with our non-micronaut factories
      val dataSource = (context.getBean(DataSource::class.java) as DelegatingDataSource).targetDataSource
      jooqDslContext = DSLContextFactory.create(dataSource, SQLDialect.POSTGRES)
      val databaseProviders = TestDatabaseProviders(dataSource, jooqDslContext)

      // this line is what runs the migrations
      databaseProviders.createNewConfigsDatabase()
      workloadRepo = context.getBean(WorkloadRepository::class.java)
      workloadLabelRepo = context.getBean(WorkloadLabelRepository::class.java)
      workloadQueueRepo = context.getBean(WorkloadQueueRepository::class.java)
    }

    @AfterAll
    @JvmStatic
    fun dbDown() {
      container.close()
    }

    private fun sortedSearch(
      dataplaneIds: List<String>?,
      statuses: List<WorkloadStatus>?,
      updatedBefore: OffsetDateTime?,
    ): MutableList<Workload> {
      val workloads = workloadRepo.search(dataplaneIds, statuses, updatedBefore).toMutableList()
      workloads.sortWith(Comparator.comparing(Workload::id))
      return workloads
    }

    private fun sortedSearchByTypeStatusCreatedDate(
      dataplaneIds: List<String>?,
      statuses: List<WorkloadStatus>?,
      types: List<WorkloadType>?,
      createdBefore: OffsetDateTime?,
    ): MutableList<Workload> {
      val workloads = workloadRepo.searchByTypeStatusAndCreationDate(dataplaneIds, statuses, types, createdBefore).toMutableList()
      workloads.sortWith(Comparator.comparing(Workload::id))
      return workloads
    }

    private fun sortedSearchByExpiredDeadline(
      dataplaneIds: List<String>?,
      statuses: List<WorkloadStatus>?,
      deadline: OffsetDateTime,
    ): MutableList<Workload> {
      val workloads = workloadRepo.searchForExpiredWorkloads(dataplaneIds, statuses, deadline).toMutableList()
      workloads.sortWith(Comparator.comparing(Workload::id))
      return workloads
    }

    /*
     * Utility for keeping timestamps at a consistent precision for test comparisons. Our test container sql truncates to millis.
     */
    private fun OffsetDateTime.truncateToTestPrecision(): OffsetDateTime = this.truncatedTo(ChronoUnit.MILLIS)
  }
}
