/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.domain.services.dataworker

import io.airbyte.commons.entitlements.EntitlementService
import io.airbyte.commons.entitlements.models.CommittedDataWorkersEntitlement
import io.airbyte.commons.entitlements.models.NumericEntitlementResult
import io.airbyte.config.Job
import io.airbyte.config.JobConfig
import io.airbyte.config.JobStatus
import io.airbyte.config.JobSyncConfig
import io.airbyte.data.repositories.DataWorkerUsageReservationRepository
import io.airbyte.data.repositories.OrganizationRepository
import io.airbyte.data.repositories.entities.DataWorkerUsage
import io.airbyte.data.repositories.entities.DataWorkerUsageReservation
import io.airbyte.data.repositories.entities.Organization
import io.airbyte.domain.models.OrganizationId
import io.micronaut.transaction.TransactionCallback
import io.micronaut.transaction.TransactionDefinition
import io.micronaut.transaction.TransactionOperations
import io.micronaut.transaction.TransactionStatus
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.sql.Connection
import java.time.OffsetDateTime
import java.util.Optional
import java.util.UUID

internal class DataWorkerCapacityServiceTest {
  private lateinit var entitlementService: EntitlementService
  private lateinit var organizationRepository: OrganizationRepository
  private lateinit var dataWorkerUsageReservationRepository: DataWorkerUsageReservationRepository
  private lateinit var dataWorkerUsageService: DataWorkerUsageService
  private lateinit var service: DataWorkerCapacityService

  @BeforeEach
  fun setUp() {
    entitlementService = mockk()
    organizationRepository = mockk()
    dataWorkerUsageReservationRepository = mockk()
    dataWorkerUsageService = mockk(relaxed = true)
    service =
      DataWorkerCapacityService(
        entitlementService,
        organizationRepository,
        dataWorkerUsageReservationRepository,
        dataWorkerUsageService,
        ImmediateConfigCapacityTransactionOperations(),
      )
  }

  @Test
  fun `getCapacityStatus returns the current and committed data worker counts`() {
    val organizationId = UUID.randomUUID()

    stubCommittedCapacity(organizationId, 5)
    every { dataWorkerUsageReservationRepository.sumReservedCpuForActiveJobsByOrganizationId(organizationId) } returns 20.0

    val result = service.getCapacityStatus(OrganizationId(organizationId))

    assertEquals(2.5, result.currentDataWorkers)
    assertEquals(5, result.committedDataWorkers)
    assertTrue(result.hasAvailableDataWorkers)
  }

  @Test
  fun `checkCapacityAndReserve is idempotent when reservation already exists`() {
    val organizationId = UUID.randomUUID()
    val job = buildJob(42L)

    stubOrgLock(organizationId)
    stubCommittedCapacity(organizationId, 5)
    every { dataWorkerUsageReservationRepository.findById(job.id) } returns
      Optional.of(
        DataWorkerUsageReservation(
          jobId = job.id,
          organizationId = organizationId,
          workspaceId = UUID.randomUUID(),
          dataplaneGroupId = UUID.randomUUID(),
          sourceCpuRequest = 1.0,
          destinationCpuRequest = 1.0,
          orchestratorCpuRequest = 0.5,
          usedOnDemandCapacity = true,
          createdAt = OffsetDateTime.now(),
        ),
      )
    every { dataWorkerUsageReservationRepository.sumReservedCpuForActiveJobsByOrganizationId(organizationId) } returns 16.0

    val result = service.checkCapacityAndReserve(OrganizationId(organizationId), job, 0.3125, true)

    assertTrue(result.hasAvailableCapacity)
    assertTrue(result.usedOnDemandCapacity)
    assertEquals(2.0, result.currentDataWorkers)
    verify(exactly = 0) { dataWorkerUsageService.prepareUsageForJob(any(), any<UUID>(), any()) }
    verify(exactly = 0) { dataWorkerUsageService.persistReservedUsageForJob(any<Long>(), any(), any()) }
  }

  @Test
  fun `checkCapacityAndReserve reserves committed capacity when available`() {
    val organizationId = UUID.randomUUID()
    val job = buildJob(84L)
    val preparedUsage = buildPreparedUsage(organizationId)

    stubOrgLock(organizationId)
    stubCommittedCapacity(organizationId, 5)
    every { dataWorkerUsageReservationRepository.findById(job.id) } returns Optional.empty()
    every { dataWorkerUsageReservationRepository.sumReservedCpuForActiveJobsByOrganizationId(organizationId) } returns 8.0
    every { dataWorkerUsageService.prepareUsageForJob(job, organizationId, any()) } returns preparedUsage

    val result = service.checkCapacityAndReserve(OrganizationId(organizationId), job, 0.5, false)

    assertTrue(result.hasAvailableCapacity)
    assertFalse(result.usedOnDemandCapacity)
    assertEquals(1.0, result.currentDataWorkers)
    verify(exactly = 1) { dataWorkerUsageService.persistReservedUsageForJob(job.id, preparedUsage, false) }
    verify(exactly = 1) { dataWorkerUsageService.recordUsageMetric(job.id, true, DataWorkerUsageService.INCREMENT_OPERATION, preparedUsage) }
  }

  @Test
  fun `checkCapacityAndReserve uses on demand capacity when committed capacity is exhausted`() {
    val organizationId = UUID.randomUUID()
    val job = buildJob(126L)
    val preparedUsage = buildPreparedUsage(organizationId)

    stubOrgLock(organizationId)
    stubCommittedCapacity(organizationId, 5)
    every { dataWorkerUsageReservationRepository.findById(job.id) } returns Optional.empty()
    every { dataWorkerUsageReservationRepository.sumReservedCpuForActiveJobsByOrganizationId(organizationId) } returns 40.0
    every { dataWorkerUsageService.prepareUsageForJob(job, organizationId, any()) } returns preparedUsage

    val result = service.checkCapacityAndReserve(OrganizationId(organizationId), job, 0.5, true)

    assertTrue(result.hasAvailableCapacity)
    assertTrue(result.usedOnDemandCapacity)
    assertEquals(5.0, result.currentDataWorkers)
    verify(exactly = 1) { dataWorkerUsageService.persistReservedUsageForJob(job.id, preparedUsage, true) }
    verify(exactly = 1) { dataWorkerUsageService.recordUsageMetric(job.id, true, DataWorkerUsageService.INCREMENT_OPERATION, preparedUsage) }
  }

  @Test
  fun `checkCapacityAndReserve queues when committed capacity is exhausted and on demand is disabled`() {
    val organizationId = UUID.randomUUID()
    val job = buildJob(168L)
    val preparedUsage = buildPreparedUsage(organizationId)

    stubOrgLock(organizationId)
    stubCommittedCapacity(organizationId, 5)
    every { dataWorkerUsageReservationRepository.findById(job.id) } returns Optional.empty()
    every { dataWorkerUsageReservationRepository.sumReservedCpuForActiveJobsByOrganizationId(organizationId) } returns 40.0
    every { dataWorkerUsageService.prepareUsageForJob(job, organizationId, any()) } returns preparedUsage

    val result = service.checkCapacityAndReserve(OrganizationId(organizationId), job, 0.5, false)

    assertFalse(result.hasAvailableCapacity)
    assertFalse(result.usedOnDemandCapacity)
    assertEquals(5.0, result.currentDataWorkers)
    verify(exactly = 0) { dataWorkerUsageService.prepareUsageForJob(job, organizationId, any()) }
    verify(exactly = 0) { dataWorkerUsageService.persistReservedUsageForJob(any<Long>(), any(), any()) }
    verify(exactly = 0) { dataWorkerUsageService.recordUsageMetric(any(), any(), any(), any()) }
  }

  @Test
  fun `getCommittedDataWorkersOrNull returns value for finite entitlement`() {
    val organizationId = UUID.randomUUID()
    every {
      entitlementService.getNumericEntitlement(OrganizationId(organizationId), CommittedDataWorkersEntitlement)
    } returns
      NumericEntitlementResult(
        featureId = CommittedDataWorkersEntitlement.featureId,
        hasAccess = true,
        value = 10L,
      )

    val result = service.getCommittedDataWorkersOrNull(OrganizationId(organizationId))

    assertEquals(10, result)
  }

  @Test
  fun `getCommittedDataWorkersOrNull returns null for unlimited entitlement`() {
    val organizationId = UUID.randomUUID()
    every {
      entitlementService.getNumericEntitlement(OrganizationId(organizationId), CommittedDataWorkersEntitlement)
    } returns
      NumericEntitlementResult(
        featureId = CommittedDataWorkersEntitlement.featureId,
        hasAccess = true,
        value = 999L,
        isUnlimited = true,
      )

    val result = service.getCommittedDataWorkersOrNull(OrganizationId(organizationId))

    assertEquals(null, result)
  }

  @Test
  fun `getCommittedDataWorkersOrNull returns null when no access`() {
    val organizationId = UUID.randomUUID()
    every {
      entitlementService.getNumericEntitlement(OrganizationId(organizationId), CommittedDataWorkersEntitlement)
    } returns
      NumericEntitlementResult(
        featureId = CommittedDataWorkersEntitlement.featureId,
        hasAccess = false,
        value = null,
      )

    val result = service.getCommittedDataWorkersOrNull(OrganizationId(organizationId))

    assertEquals(null, result)
  }

  @Test
  fun `checkCapacityAndReserve fails when usage cannot be prepared`() {
    val organizationId = UUID.randomUUID()
    val job = buildJob(210L)

    stubOrgLock(organizationId)
    stubCommittedCapacity(organizationId, 5)
    every { dataWorkerUsageReservationRepository.findById(job.id) } returns Optional.empty()
    every { dataWorkerUsageReservationRepository.sumReservedCpuForActiveJobsByOrganizationId(organizationId) } returns 8.0
    every { dataWorkerUsageService.prepareUsageForJob(job, organizationId, any()) } returns null

    assertThrows(IllegalStateException::class.java) {
      service.checkCapacityAndReserve(OrganizationId(organizationId), job, 0.5, true)
    }

    verify(exactly = 1) { organizationRepository.findByIdForUpdate(organizationId) }
    verify(exactly = 1) { dataWorkerUsageService.prepareUsageForJob(job, organizationId, any()) }
    verify(exactly = 0) { dataWorkerUsageService.persistReservedUsageForJob(any<Long>(), any(), any()) }
  }

  private fun stubOrgLock(organizationId: UUID) {
    every { organizationRepository.findByIdForUpdate(organizationId) } returns
      Optional.of(Organization(organizationId, "test-org", null, "test@example.com"))
  }

  private fun stubCommittedCapacity(
    organizationId: UUID,
    committedCapacity: Int,
  ) {
    every {
      entitlementService.getNumericEntitlement(OrganizationId(organizationId), CommittedDataWorkersEntitlement)
    } returns
      NumericEntitlementResult(
        featureId = CommittedDataWorkersEntitlement.featureId,
        hasAccess = true,
        value = committedCapacity.toLong(),
      )
  }

  private fun buildJob(jobId: Long): Job =
    Job(
      jobId,
      JobConfig.ConfigType.SYNC,
      UUID.randomUUID().toString(),
      JobConfig().withConfigType(JobConfig.ConfigType.SYNC).withSync(JobSyncConfig().withWorkspaceId(UUID.randomUUID())),
      emptyList(),
      JobStatus.PENDING,
      null,
      System.currentTimeMillis() / 1000,
      System.currentTimeMillis() / 1000,
      false,
    )

  private fun buildPreparedUsage(organizationId: UUID): DataWorkerUsage {
    val now = OffsetDateTime.now()
    return DataWorkerUsage(
      organizationId = organizationId,
      workspaceId = UUID.randomUUID(),
      dataplaneGroupId = UUID.randomUUID(),
      sourceCpuRequest = 1.0,
      destinationCpuRequest = 1.0,
      orchestratorCpuRequest = 1.0,
      bucketStart = now,
      createdAt = now,
      maxSourceCpuRequest = 1.0,
      maxDestinationCpuRequest = 1.0,
      maxOrchestratorCpuRequest = 1.0,
    )
  }
}

private class ImmediateConfigCapacityTransactionOperations : TransactionOperations<Connection> {
  override fun getConnection(): Connection = mockk(relaxed = true)

  override fun hasConnection(): Boolean = true

  override fun findTransactionStatus(): Optional<out TransactionStatus<*>> = Optional.empty()

  override fun <R : Any?> execute(
    definition: TransactionDefinition,
    callback: TransactionCallback<Connection, R>,
  ): R = callback.call(mockk(relaxed = true))
}
