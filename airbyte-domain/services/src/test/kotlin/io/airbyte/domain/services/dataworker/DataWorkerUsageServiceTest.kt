/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.domain.services.dataworker

import io.airbyte.commons.entitlements.EntitlementService
import io.airbyte.config.Job
import io.airbyte.config.JobConfig
import io.airbyte.config.JobStatus
import io.airbyte.config.JobSyncConfig
import io.airbyte.config.Organization
import io.airbyte.config.ResourceRequirements
import io.airbyte.config.StandardWorkspace
import io.airbyte.config.SyncResourceRequirements
import io.airbyte.data.repositories.entities.DataWorkerUsage
import io.airbyte.data.repositories.entities.DataplaneGroup
import io.airbyte.data.services.DataWorkerUsageDataService
import io.airbyte.data.services.DataplaneGroupService
import io.airbyte.data.services.OrganizationService
import io.airbyte.data.services.WorkspaceService
import io.airbyte.data.services.impls.data.mappers.DataplaneGroupMapper.toConfigModel
import io.airbyte.domain.models.EntitlementPlan
import io.airbyte.featureflag.FeatureFlagClient
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.time.LocalDate
import java.time.OffsetDateTime
import java.time.ZoneOffset
import java.util.Optional
import java.util.UUID

class DataWorkerUsageServiceTest {
  private lateinit var organizationService: OrganizationService
  private lateinit var dataplaneGroupService: DataplaneGroupService
  private lateinit var dataWorkerUsageDataService: DataWorkerUsageDataService
  private lateinit var workspaceService: WorkspaceService
  private lateinit var featureFlagClient: FeatureFlagClient
  private lateinit var entitlementService: EntitlementService

  private lateinit var service: DataWorkerUsageService

  @BeforeEach
  fun setup() {
    organizationService = mockk()
    dataplaneGroupService = mockk()
    dataWorkerUsageDataService = mockk()
    workspaceService = mockk()
    featureFlagClient = mockk()
    entitlementService = mockk()
    service =
      DataWorkerUsageService(
        organizationService,
        dataplaneGroupService,
        dataWorkerUsageDataService,
        workspaceService,
        featureFlagClient,
        entitlementService,
      )
  }

  @Test
  fun `insertUsageForCompletedJob should insert usage when job has workspace id and organization exists`() {
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
    every { featureFlagClient.boolVariation(any(), any()) } returns true
    every { entitlementService.getCurrentPlanId(any()) } returns EntitlementPlan.PRO.id

    service.insertUsageForCompletedJob(job)

    verify(exactly = 1) {
      dataWorkerUsageDataService.insertDataWorkerUsage(
        match {
          it.organizationId == organizationId &&
            it.workspaceId == workspaceId &&
            it.dataplaneGroupId == dataplaneGroupId &&
            it.sourceCpuRequest == 2.0 &&
            it.destinationCpuRequest == 3.0 &&
            it.orchestratorCpuRequest == 1.0
        },
      )
    }
  }

  @Test
  fun `insertUsageForCompletedJob should not insert usage when job has no workspace id`() {
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

    service.insertUsageForCompletedJob(job)

    verify(exactly = 0) {
      dataWorkerUsageDataService.insertDataWorkerUsage(any())
    }
  }

  @Test
  fun `insertUsageForCompletedJob should not insert usage when organization does not exist`() {
    val workspaceId = UUID.randomUUID()
    val jobId = 123L

    val job = buildTestJob(jobId, workspaceId, "2.0", "2.0", "2.0")

    every { organizationService.getOrganizationForWorkspaceId(workspaceId) } returns Optional.empty()

    service.insertUsageForCompletedJob(job)

    verify(exactly = 0) {
      dataWorkerUsageDataService.insertDataWorkerUsage(any())
    }
  }

  @Test
  fun `insertUsageForCompletedJob should correctly parse cpu request values`() {
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
    every { featureFlagClient.boolVariation(any(), any()) } returns true
    every { entitlementService.getCurrentPlanId(any()) } returns EntitlementPlan.PRO.id

    service.insertUsageForCompletedJob(job)

    verify(exactly = 1) {
      dataWorkerUsageDataService.insertDataWorkerUsage(
        match {
          it.organizationId == organizationId &&
            it.workspaceId == workspaceId &&
            it.dataplaneGroupId == dataplaneGroupId &&
            it.sourceCpuRequest == 1.5 &&
            it.destinationCpuRequest == 2.5 &&
            it.orchestratorCpuRequest == 0.5
        },
      )
    }
  }

  @Test
  fun `insertUsageForCompletedJob should not insert usage when job is not a sync`() {
    val workspaceId = UUID.randomUUID()
    val jobId = 123L

    val jobConfig =
      JobConfig()
        .withConfigType(JobConfig.ConfigType.CHECK_CONNECTION_SOURCE)

    val job =
      Job(
        jobId,
        JobConfig.ConfigType.CHECK_CONNECTION_SOURCE,
        workspaceId.toString(),
        jobConfig,
        emptyList(),
        JobStatus.PENDING,
        null,
        System.currentTimeMillis() / 1000,
        System.currentTimeMillis() / 1000,
        false,
      )

    service.insertUsageForCompletedJob(job)

    verify(exactly = 0) {
      dataWorkerUsageDataService.insertDataWorkerUsage(any())
    }
  }

  @Test
  fun `insertUsageForCompletedJob should not insert usage when feature flag is disabled`() {
    val workspaceId = UUID.randomUUID()
    val organizationId = UUID.randomUUID()
    val jobId = 123L

    val organization =
      Organization()
        .withOrganizationId(organizationId)
        .withEmail("test@example.com")
        .withName("Test Org")

    val job = buildTestJob(jobId, workspaceId, "2.0", "3.0", "1.0")

    every { organizationService.getOrganizationForWorkspaceId(workspaceId) } returns Optional.of(organization)
    every { featureFlagClient.boolVariation(any(), any()) } returns false
    // valid entitlement, but flag is off so we should still not call the insert method.
    every { entitlementService.getCurrentPlanId(any()) } returns EntitlementPlan.PRO.id

    service.insertUsageForCompletedJob(job)

    verify(exactly = 0) {
      dataWorkerUsageDataService.insertDataWorkerUsage(any())
    }
  }

  @Test
  fun `insertUsageForCompletedJob should not insert usage when organization plan is not pro, flex, or sme`() {
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
    every { featureFlagClient.boolVariation(any(), any()) } returns true
    every { entitlementService.getCurrentPlanId(any()) } returns EntitlementPlan.CORE.id

    service.insertUsageForCompletedJob(job)

    verify(exactly = 0) {
      dataWorkerUsageDataService.insertDataWorkerUsage(any())
    }
  }

  @Test
  fun `calculateDataWorkers should divide total CPU by 8`() {
    val dataWorkerUsage =
      DataWorkerUsage(
        organizationId = UUID.randomUUID(),
        workspaceId = UUID.randomUUID(),
        dataplaneGroupId = UUID.randomUUID(),
        sourceCpuRequest = 2.0,
        destinationCpuRequest = 3.0,
        orchestratorCpuRequest = 1.0,
        bucketStart = OffsetDateTime.now(),
        createdAt = OffsetDateTime.now(),
      )

    val result = dataWorkerUsage.calculateDataWorkers()
    assertEquals(0.75, result, 0.001)
  }

  @Test
  fun `getDataWorkerUsage should return organization usage grouped by dataplane and workspace`() {
    val organizationId = UUID.randomUUID()
    val workspaceId1 = UUID.randomUUID()
    val workspaceId2 = UUID.randomUUID()
    val dataplaneGroupId1 = UUID.randomUUID()
    val dataplaneGroupId2 = UUID.randomUUID()
    val startDate = LocalDate.of(2025, 1, 1)
    val endDate = LocalDate.of(2025, 1, 2)

    val hour1 = OffsetDateTime.of(2025, 1, 1, 10, 0, 0, 0, ZoneOffset.UTC)
    val hour2 = OffsetDateTime.of(2025, 1, 1, 11, 0, 0, 0, ZoneOffset.UTC)

    val usageRecords =
      listOf(
        DataWorkerUsage(
          organizationId = organizationId,
          workspaceId = workspaceId1,
          dataplaneGroupId = dataplaneGroupId1,
          sourceCpuRequest = 2.0,
          destinationCpuRequest = 3.0,
          orchestratorCpuRequest = 1.0,
          bucketStart = hour1,
          createdAt = OffsetDateTime.now(),
        ),
        DataWorkerUsage(
          organizationId = organizationId,
          workspaceId = workspaceId1,
          dataplaneGroupId = dataplaneGroupId1,
          sourceCpuRequest = 4.0,
          destinationCpuRequest = 2.0,
          orchestratorCpuRequest = 2.0,
          bucketStart = hour2,
          createdAt = OffsetDateTime.now(),
        ),
        DataWorkerUsage(
          organizationId = organizationId,
          workspaceId = workspaceId2,
          dataplaneGroupId = dataplaneGroupId2,
          sourceCpuRequest = 1.0,
          destinationCpuRequest = 1.0,
          orchestratorCpuRequest = 1.0,
          bucketStart = hour1,
          createdAt = OffsetDateTime.now(),
        ),
      )

    val dataplaneGroup1 =
      DataplaneGroup(
        id = dataplaneGroupId1,
        organizationId = organizationId,
        name = "Dataplane Group 1",
        enabled = true,
        tombstone = false,
      )

    val dataplaneGroup2 =
      DataplaneGroup(
        id = dataplaneGroupId2,
        organizationId = organizationId,
        name = "Dataplane Group 2",
        enabled = true,
        tombstone = false,
      )

    val workspace1 =
      StandardWorkspace()
        .withWorkspaceId(workspaceId1)
        .withName("Workspace 1")
        .withDataplaneGroupId(dataplaneGroupId1)

    val workspace2 =
      StandardWorkspace()
        .withWorkspaceId(workspaceId2)
        .withName("Workspace 2")
        .withDataplaneGroupId(dataplaneGroupId2)

    every {
      dataWorkerUsageDataService.getDataWorkerUsageByOrganizationAndTimeRange(
        organizationId,
        startDate.atStartOfDay(ZoneOffset.UTC).toOffsetDateTime(),
        endDate.atTime(23, 59, 59).atZone(ZoneOffset.UTC).toOffsetDateTime(),
      )
    } returns usageRecords

    every { dataplaneGroupService.getDataplaneGroup(dataplaneGroupId1) } returns dataplaneGroup1.toConfigModel()
    every { dataplaneGroupService.getDataplaneGroup(dataplaneGroupId2) } returns dataplaneGroup2.toConfigModel()
    every { workspaceService.getStandardWorkspaceNoSecrets(workspaceId1, false) } returns workspace1
    every { workspaceService.getStandardWorkspaceNoSecrets(workspaceId2, false) } returns workspace2

    val result = service.getDataWorkerUsage(organizationId, startDate, endDate)

    assertEquals(organizationId, result.organizationId.value)
    assertEquals(2, result.dataplaneGroups.size)

    val dp1 = result.dataplaneGroups.find { it.dataplaneGroupId.value == dataplaneGroupId1 }!!
    assertEquals("Dataplane Group 1", dp1.dataplaneGroupName)
    assertEquals(1, dp1.workspaces.size)
    assertEquals(workspaceId1, dp1.workspaces[0].workspaceId.value)
    assertEquals("Workspace 1", dp1.workspaces[0].workspaceName)
    assertEquals(2, dp1.workspaces[0].dataWorkers.size)
    assertEquals(0.75, dp1.workspaces[0].dataWorkers[0].usage, 0.001)
    assertEquals(1.0, dp1.workspaces[0].dataWorkers[1].usage, 0.001)

    val dp2 = result.dataplaneGroups.find { it.dataplaneGroupId.value == dataplaneGroupId2 }!!
    assertEquals("Dataplane Group 2", dp2.dataplaneGroupName)
    assertEquals(1, dp2.workspaces.size)
    assertEquals(workspaceId2, dp2.workspaces[0].workspaceId.value)
    assertEquals("Workspace 2", dp2.workspaces[0].workspaceName)
    assertEquals(1, dp2.workspaces[0].dataWorkers.size)
    assertEquals(0.375, dp2.workspaces[0].dataWorkers[0].usage, 0.001)
  }

  @Test
  fun `getDataWorkerUsage should exclude workspace that does not exist`() {
    val organizationId = UUID.randomUUID()
    val workspaceId1 = UUID.randomUUID()
    val workspaceId2 = UUID.randomUUID()
    val dataplaneGroupId = UUID.randomUUID()
    val startDate = LocalDate.of(2025, 1, 1)
    val endDate = LocalDate.of(2025, 1, 2)

    val hour1 = OffsetDateTime.of(2025, 1, 1, 10, 0, 0, 0, ZoneOffset.UTC)

    val usageRecords =
      listOf(
        DataWorkerUsage(
          organizationId = organizationId,
          workspaceId = workspaceId1,
          dataplaneGroupId = dataplaneGroupId,
          sourceCpuRequest = 2.0,
          destinationCpuRequest = 3.0,
          orchestratorCpuRequest = 1.0,
          bucketStart = hour1,
          createdAt = OffsetDateTime.now(),
        ),
        DataWorkerUsage(
          organizationId = organizationId,
          workspaceId = workspaceId2,
          dataplaneGroupId = dataplaneGroupId,
          sourceCpuRequest = 4.0,
          destinationCpuRequest = 2.0,
          orchestratorCpuRequest = 2.0,
          bucketStart = hour1,
          createdAt = OffsetDateTime.now(),
        ),
      )

    val dataplaneGroup =
      DataplaneGroup(
        id = dataplaneGroupId,
        organizationId = organizationId,
        name = "Dataplane Group 1",
        enabled = true,
        tombstone = false,
      )

    val workspace1 =
      StandardWorkspace()
        .withWorkspaceId(workspaceId1)
        .withName("Workspace 1")
        .withDataplaneGroupId(dataplaneGroupId)

    every {
      dataWorkerUsageDataService.getDataWorkerUsageByOrganizationAndTimeRange(
        organizationId,
        startDate.atStartOfDay(ZoneOffset.UTC).toOffsetDateTime(),
        endDate.atTime(23, 59, 59).atZone(ZoneOffset.UTC).toOffsetDateTime(),
      )
    } returns usageRecords

    every { dataplaneGroupService.getDataplaneGroup(dataplaneGroupId) } returns dataplaneGroup.toConfigModel()
    every { workspaceService.getStandardWorkspaceNoSecrets(workspaceId1, false) } returns workspace1
    every { workspaceService.getStandardWorkspaceNoSecrets(workspaceId2, false) } throws RuntimeException("Workspace not found")

    val result = service.getDataWorkerUsage(organizationId, startDate, endDate)

    assertEquals(organizationId, result.organizationId.value)
    assertEquals(1, result.dataplaneGroups.size)

    val dp = result.dataplaneGroups[0]
    assertEquals("Dataplane Group 1", dp.dataplaneGroupName)
    assertEquals(1, dp.workspaces.size)
    assertEquals(workspaceId1, dp.workspaces[0].workspaceId.value)
    assertEquals("Workspace 1", dp.workspaces[0].workspaceName)
    assertEquals(1, dp.workspaces[0].dataWorkers.size)
    assertEquals(0.75, dp.workspaces[0].dataWorkers[0].usage, 0.001)
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
