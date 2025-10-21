/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.domain.services.dataworker

import io.airbyte.config.Job
import io.airbyte.config.JobConfig
import io.airbyte.config.JobStatus
import io.airbyte.config.JobSyncConfig
import io.airbyte.config.Organization
import io.airbyte.config.ResourceRequirements
import io.airbyte.config.StandardWorkspace
import io.airbyte.config.SyncResourceRequirements
import io.airbyte.data.repositories.entities.DataWorkerUsage
import io.airbyte.data.services.DataWorkerUsageDataService
import io.airbyte.data.services.DataplaneGroupService
import io.airbyte.data.services.OrganizationService
import io.airbyte.data.services.WorkspaceService
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.time.OffsetDateTime
import java.util.Optional
import java.util.UUID

class DataWorkerUsageServiceTest {
  private lateinit var organizationService: OrganizationService
  private lateinit var dataplaneGroupService: DataplaneGroupService
  private lateinit var dataWorkerUsageDataService: DataWorkerUsageDataService
  private lateinit var workspaceService: WorkspaceService

  private lateinit var service: DataWorkerUsageService

  @BeforeEach
  fun setup() {
    organizationService = mockk()
    dataplaneGroupService = mockk()
    dataWorkerUsageDataService = mockk()
    workspaceService = mockk()
    service =
      DataWorkerUsageService(
        organizationService,
        dataplaneGroupService,
        dataWorkerUsageDataService,
        workspaceService,
      )
  }

  @Test
  fun `insertUsageWhenJobStarts should insert usage when job has workspace id and organization exists`() {
    val workspaceId = UUID.randomUUID()
    val organizationId = UUID.randomUUID()
    val dataplaneGroupId = UUID.randomUUID()
    val jobId = 123L

    val organization =
      Organization()
        .withOrganizationId(organizationId)
        .withEmail("test@example.com")
        .withName("Test Org")

    val workspace =
      StandardWorkspace()
        .withWorkspaceId(workspaceId)
        .withDataplaneGroupId(dataplaneGroupId)

    val job = buildTestJob(jobId, workspaceId, "2.0", "3.0", "1.0")

    every { organizationService.getOrganizationForWorkspaceId(workspaceId) } returns Optional.of(organization)
    every { workspaceService.getStandardWorkspaceNoSecrets(workspaceId, false) } returns workspace
    every { dataWorkerUsageDataService.insertDataWorkerUsage(any()) } returns mockk()

    service.insertUsageWhenJobStarts(job)

    verify(exactly = 1) {
      dataWorkerUsageDataService.insertDataWorkerUsage(
        match {
          it.jobId == jobId &&
            it.organizationId == organizationId &&
            it.workspaceId == workspaceId &&
            it.dataplaneGroupId == dataplaneGroupId &&
            it.sourceCpuRequest == 2.0 &&
            it.destinationCpuRequest == 3.0 &&
            it.orchestratorCpuRequest == 1.0 &&
            it.jobEnd == null
        },
      )
    }
  }

  @Test
  fun `insertUsageWhenJobStarts should not insert usage when job has no workspace id`() {
    val jobSyncConfig = JobSyncConfig()
    val jobConfig =
      JobConfig()
        .withConfigType(JobConfig.ConfigType.SYNC)
        .withSync(jobSyncConfig)

    val job =
      Job(
        123L,
        JobConfig.ConfigType.SYNC,
        UUID.randomUUID().toString(),
        jobConfig,
        emptyList(),
        JobStatus.PENDING,
        null,
        System.currentTimeMillis() / 1000,
        System.currentTimeMillis() / 1000,
        false,
      )

    service.insertUsageWhenJobStarts(job)

    verify(exactly = 0) {
      dataWorkerUsageDataService.insertDataWorkerUsage(any())
    }
  }

  @Test
  fun `insertUsageWhenJobStarts should not insert usage when organization does not exist`() {
    val workspaceId = UUID.randomUUID()
    val jobId = 123L

    val job = buildTestJob(jobId, workspaceId, "2.0", "2.0", "2.0")

    every { organizationService.getOrganizationForWorkspaceId(workspaceId) } returns Optional.empty()

    service.insertUsageWhenJobStarts(job)

    verify(exactly = 0) {
      dataWorkerUsageDataService.insertDataWorkerUsage(any())
    }
  }

  @Test
  fun `insertUsageWhenJobStarts should correctly parse cpu request values`() {
    val workspaceId = UUID.randomUUID()
    val organizationId = UUID.randomUUID()
    val dataplaneGroupId = UUID.randomUUID()
    val jobId = 456L

    val organization =
      Organization()
        .withOrganizationId(organizationId)
        .withEmail("test@example.com")
        .withName("Test Org")

    val workspace =
      StandardWorkspace()
        .withWorkspaceId(workspaceId)
        .withDataplaneGroupId(dataplaneGroupId)

    val job = buildTestJob(jobId, workspaceId, "1.5", "2.5", "0.5")

    every { organizationService.getOrganizationForWorkspaceId(workspaceId) } returns Optional.of(organization)
    every { workspaceService.getStandardWorkspaceNoSecrets(workspaceId, false) } returns workspace
    every { dataWorkerUsageDataService.insertDataWorkerUsage(any()) } returns mockk()

    service.insertUsageWhenJobStarts(job)

    verify(exactly = 1) {
      dataWorkerUsageDataService.insertDataWorkerUsage(
        match {
          it.jobId == jobId &&
            it.organizationId == organizationId &&
            it.workspaceId == workspaceId &&
            it.dataplaneGroupId == dataplaneGroupId &&
            it.sourceCpuRequest == 1.5 &&
            it.destinationCpuRequest == 2.5 &&
            it.orchestratorCpuRequest == 0.5 &&
            it.jobEnd == null
        },
      )
    }
  }

  @Test
  fun `updateUsageWhenJobFinishes should update usage with job end time`() {
    val workspaceId = UUID.randomUUID()
    val jobId = 789L
    val updatedAtInSecond = 1609459200L

    val job = buildTestJob(jobId, workspaceId, "2.0", "2.0", "2.0", updatedAtInSecond)

    every { dataWorkerUsageDataService.updateDataWorkerUsage(any(), any()) } returns Unit

    service.updateUsageWhenJobFinishes(job)

    verify(exactly = 1) {
      dataWorkerUsageDataService.updateDataWorkerUsage(
        jobId = jobId,
        jobEnd = any(),
      )
    }
  }

  @Test
  fun `calculateDataWorkers should divide total CPU by 8`() {
    val dataWorkerUsage =
      DataWorkerUsage(
        jobId = 123L,
        organizationId = UUID.randomUUID(),
        workspaceId = UUID.randomUUID(),
        dataplaneGroupId = UUID.randomUUID(),
        sourceCpuRequest = 2.0,
        destinationCpuRequest = 3.0,
        orchestratorCpuRequest = 1.0,
        jobStart = OffsetDateTime.now(),
        jobEnd = null,
        createdAt = OffsetDateTime.now(),
      )

    val result = dataWorkerUsage.calculateDataWorkers()
    assertEquals(0.75, result, 0.001)
  }

  private fun buildTestJob(
    jobId: Long,
    workspaceId: UUID,
    sourceCpu: String,
    destinationCpu: String,
    orchestratorCpu: String,
    updatedAtInSecond: Long = System.currentTimeMillis() / 1000,
    startedAtInSecond: Long? = System.currentTimeMillis() / 1000,
  ): Job {
    val sourceResourceRequirements =
      ResourceRequirements()
        .withCpuRequest(sourceCpu)

    val destinationResourceRequirements =
      ResourceRequirements()
        .withCpuRequest(destinationCpu)

    val orchestratorResourceRequirements =
      ResourceRequirements()
        .withCpuRequest(orchestratorCpu)

    val syncResourceRequirements =
      SyncResourceRequirements()
        .withSource(sourceResourceRequirements)
        .withDestination(destinationResourceRequirements)
        .withOrchestrator(orchestratorResourceRequirements)

    val jobSyncConfig =
      JobSyncConfig()
        .withWorkspaceId(workspaceId)
        .withSyncResourceRequirements(syncResourceRequirements)

    val jobConfig =
      JobConfig()
        .withConfigType(JobConfig.ConfigType.SYNC)
        .withSync(jobSyncConfig)

    return Job(
      jobId,
      JobConfig.ConfigType.SYNC,
      workspaceId.toString(),
      jobConfig,
      emptyList(),
      JobStatus.PENDING,
      startedAtInSecond,
      System.currentTimeMillis() / 1000,
      updatedAtInSecond,
      false,
    )
  }
}
