/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.services

import io.airbyte.config.ActorDefinitionVersion
import io.airbyte.config.Job
import io.airbyte.config.JobConfig
import io.airbyte.config.JobResetConnectionConfig
import io.airbyte.config.JobStatus
import io.airbyte.config.JobSyncConfig
import io.airbyte.config.RefreshConfig
import io.airbyte.config.StandardSync
import io.airbyte.data.repositories.entities.ObsJobsStats
import io.airbyte.data.repositories.entities.ObsStreamStats
import io.airbyte.data.repositories.entities.ObsStreamStatsId
import io.airbyte.data.services.ActorDefinitionService
import io.airbyte.data.services.ConnectionService
import io.airbyte.data.services.ObsStatsService
import io.airbyte.data.services.WorkspaceService
import io.airbyte.metrics.MetricClient
import io.airbyte.persistence.job.JobPersistence
import io.mockk.every
import io.mockk.mockk
import io.mockk.slot
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.math.BigDecimal
import java.util.Optional
import java.util.UUID

class JobObservabilityServiceTest {
  companion object {
    private val SYNC_DESTINATION_VERSION_ID = UUID.fromString("11111111-1111-1111-1111-111111111111")
    private val SYNC_SOURCE_VERSION_ID = UUID.fromString("22222222-2222-2222-2222-222222222222")
    private val SYNC_WORKSPACE_ID = UUID.fromString("33333333-3333-3333-3333-333333333333")

    private val RESET_DESTINATION_VERSION_ID = UUID.fromString("44444444-4444-4444-4444-444444444444")
    private val RESET_WORKSPACE_ID = UUID.fromString("55555555-5555-5555-5555-555555555555")

    private val REFRESH_DESTINATION_VERSION_ID = UUID.fromString("66666666-6666-6666-6666-666666666666")
    private val REFRESH_SOURCE_VERSION_ID = UUID.fromString("77777777-7777-7777-7777-777777777777")
    private val REFRESH_WORKSPACE_ID = UUID.fromString("88888888-8888-8888-8888-888888888888")

    private val UUID_ZERO = UUID.fromString("00000000-0000-0000-0000-000000000000")

    private const val JOB_ID = 1L
    private const val CONNECTION_ID_STR = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
    private val CONNECTION_ID = UUID.fromString(CONNECTION_ID_STR)
    private val WORKSPACE_ID = UUID.fromString("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb")
    private val ORG_ID = UUID.fromString("cccccccc-cccc-cccc-cccc-cccccccccccc")
    private val SOURCE_ID = UUID.fromString("dddddddd-dddd-dddd-dddd-dddddddddddd")
    private val SOURCE_DEF_ID = UUID.fromString("eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee")
    private val DEST_ID = UUID.fromString("ffffffff-ffff-ffff-ffff-ffffffffffff")
    private val DEST_DEF_ID = UUID.fromString("99999999-9999-9999-9999-999999999999")
    private const val SOURCE_IMAGE_TAG = "0.1.0"
    private const val DEST_IMAGE_TAG = "0.2.0"
  }

  private lateinit var actorDefinitionService: ActorDefinitionService
  private lateinit var connectionService: ConnectionService
  private lateinit var jobPersistence: JobPersistence
  private lateinit var obsStatsService: ObsStatsService
  private lateinit var workspaceService: WorkspaceService
  private lateinit var metricClient: MetricClient
  private lateinit var streamStatsService: StreamStatsService
  private lateinit var service: JobObservabilityService

  @BeforeEach
  fun setup() {
    actorDefinitionService = mockk()
    connectionService = mockk()
    jobPersistence = mockk()
    obsStatsService = mockk(relaxed = true)
    workspaceService = mockk()
    metricClient = mockk(relaxed = true)
    streamStatsService = mockk()

    service =
      JobObservabilityService(
        actorDefinitionService = actorDefinitionService,
        connectionService = connectionService,
        jobPersistence = jobPersistence,
        obsStatsService = obsStatsService,
        workspaceService = workspaceService,
        metricClient = metricClient,
        jobObservabilityReportingService = null,
        jobObservabilityRulesService = mockk(),
        streamStatsService = streamStatsService,
      )
  }

  @Test
  fun `test finalizeStats saves job stats and stream stats`() {
    // Setup mocks
    val job = createMockJob()
    val standardSync = createMockStandardSync()
    val sourceVersion = createMockActorDefinitionVersion(SOURCE_DEF_ID, SOURCE_IMAGE_TAG)
    val destVersion = createMockActorDefinitionVersion(DEST_DEF_ID, DEST_IMAGE_TAG)

    every { jobPersistence.getJob(JOB_ID) } returns job
    every { connectionService.getStandardSync(CONNECTION_ID) } returns standardSync
    every { actorDefinitionService.getActorDefinitionVersion(SOURCE_DEF_ID) } returns sourceVersion
    every { actorDefinitionService.getActorDefinitionVersion(DEST_DEF_ID) } returns destVersion
    every { workspaceService.getOrganizationIdFromWorkspaceId(WORKSPACE_ID) } returns Optional.of(ORG_ID)

    val aggregatedStats =
      listOf(
        StreamStatsService.AggregatedStreamStats(
          streamName = "users",
          streamNamespace = "public",
          recordsEmitted = 100,
          bytesEmitted = 1000,
          recordsCommitted = 90,
          bytesCommitted = 900,
          recordsRejected = 10,
          additionalStats = mapOf("metric1" to BigDecimal("1.5")),
          wasBackfilled = true,
          wasResumed = false,
        ),
      )
    every { streamStatsService.getAggregatedStatsForJob(JOB_ID) } returns aggregatedStats

    // Execute
    service.finalizeStats(JOB_ID)

    // Verify job stats were saved
    val jobStatsSlot = slot<ObsJobsStats>()
    verify { obsStatsService.saveJobsStats(capture(jobStatsSlot)) }

    val savedJobStats = jobStatsSlot.captured
    assertEquals(JOB_ID, savedJobStats.jobId)
    assertEquals(CONNECTION_ID, savedJobStats.connectionId)
    assertEquals(WORKSPACE_ID, savedJobStats.workspaceId)
    assertEquals(ORG_ID, savedJobStats.organizationId)
    assertEquals(SOURCE_ID, savedJobStats.sourceId)
    assertEquals(SOURCE_DEF_ID, savedJobStats.sourceDefinitionId)
    assertEquals(SOURCE_IMAGE_TAG, savedJobStats.sourceImageTag)
    assertEquals(DEST_ID, savedJobStats.destinationId)
    assertEquals(DEST_DEF_ID, savedJobStats.destinationDefinitionId)
    assertEquals(DEST_IMAGE_TAG, savedJobStats.destinationImageTag)
    assertEquals("sync", savedJobStats.jobType)
    assertEquals("SUCCEEDED", savedJobStats.status)
    assertEquals(0, savedJobStats.attemptCount)

    // Verify stream stats were saved
    val streamStatsSlot = slot<List<ObsStreamStats>>()
    verify { obsStatsService.saveStreamStats(capture(streamStatsSlot)) }

    val savedStreamStats = streamStatsSlot.captured
    assertEquals(1, savedStreamStats.size)

    val streamStat = savedStreamStats[0]
    assertEquals(JOB_ID, streamStat.id.jobId)
    assertEquals("public", streamStat.id.streamNamespace)
    assertEquals("users", streamStat.id.streamName)
    assertEquals(900, streamStat.bytesLoaded) // bytesCommitted -> bytesLoaded
    assertEquals(90, streamStat.recordsLoaded) // recordsCommitted -> recordsLoaded
    assertEquals(10, streamStat.recordsRejected)
    assertEquals(true, streamStat.wasBackfilled)
    assertEquals(false, streamStat.wasResumed)
    assertEquals(mapOf("metric1" to BigDecimal("1.5")), streamStat.additionalStats)
  }

  @Test
  fun `test finalizeStats handles multiple streams`() {
    // Setup mocks
    val job = createMockJob()
    val standardSync = createMockStandardSync()
    val sourceVersion = createMockActorDefinitionVersion(SOURCE_DEF_ID, SOURCE_IMAGE_TAG)
    val destVersion = createMockActorDefinitionVersion(DEST_DEF_ID, DEST_IMAGE_TAG)

    every { jobPersistence.getJob(JOB_ID) } returns job
    every { connectionService.getStandardSync(CONNECTION_ID) } returns standardSync
    every { actorDefinitionService.getActorDefinitionVersion(SOURCE_DEF_ID) } returns sourceVersion
    every { actorDefinitionService.getActorDefinitionVersion(DEST_DEF_ID) } returns destVersion
    every { workspaceService.getOrganizationIdFromWorkspaceId(WORKSPACE_ID) } returns Optional.of(ORG_ID)

    val aggregatedStats =
      listOf(
        StreamStatsService.AggregatedStreamStats(
          streamName = "users",
          streamNamespace = "public",
          recordsEmitted = 100,
          bytesEmitted = 1000,
          recordsCommitted = 90,
          bytesCommitted = 900,
          recordsRejected = 10,
          additionalStats = emptyMap(),
          wasBackfilled = true,
          wasResumed = false,
        ),
        StreamStatsService.AggregatedStreamStats(
          streamName = "orders",
          streamNamespace = "public",
          recordsEmitted = 200,
          bytesEmitted = 2000,
          recordsCommitted = 190,
          bytesCommitted = 1900,
          recordsRejected = 10,
          additionalStats = emptyMap(),
          wasBackfilled = false,
          wasResumed = true,
        ),
      )
    every { streamStatsService.getAggregatedStatsForJob(JOB_ID) } returns aggregatedStats

    // Execute
    service.finalizeStats(JOB_ID)

    // Verify stream stats were saved for both streams
    val streamStatsSlot = slot<List<ObsStreamStats>>()
    verify { obsStatsService.saveStreamStats(capture(streamStatsSlot)) }

    val savedStreamStats = streamStatsSlot.captured
    assertEquals(2, savedStreamStats.size)

    val usersStat = savedStreamStats.find { it.id.streamName == "users" }!!
    assertEquals(900, usersStat.bytesLoaded)
    assertEquals(90, usersStat.recordsLoaded)
    assertEquals(true, usersStat.wasBackfilled)
    assertEquals(false, usersStat.wasResumed)
    assertEquals(emptyMap<String, BigDecimal>(), usersStat.additionalStats)

    val ordersStat = savedStreamStats.find { it.id.streamName == "orders" }!!
    assertEquals(1900, ordersStat.bytesLoaded)
    assertEquals(190, ordersStat.recordsLoaded)
    assertEquals(false, ordersStat.wasBackfilled)
    assertEquals(true, ordersStat.wasResumed)
    assertEquals(emptyMap<String, BigDecimal>(), ordersStat.additionalStats)
  }

  @Test
  fun `test finalizeStats calls StreamStatsService with correct jobId`() {
    // Setup mocks
    val job = createMockJob()
    val standardSync = createMockStandardSync()
    val sourceVersion = createMockActorDefinitionVersion(SOURCE_DEF_ID, SOURCE_IMAGE_TAG)
    val destVersion = createMockActorDefinitionVersion(DEST_DEF_ID, DEST_IMAGE_TAG)

    every { jobPersistence.getJob(JOB_ID) } returns job
    every { connectionService.getStandardSync(CONNECTION_ID) } returns standardSync
    every { actorDefinitionService.getActorDefinitionVersion(SOURCE_DEF_ID) } returns sourceVersion
    every { actorDefinitionService.getActorDefinitionVersion(DEST_DEF_ID) } returns destVersion
    every { workspaceService.getOrganizationIdFromWorkspaceId(WORKSPACE_ID) } returns Optional.of(ORG_ID)
    every { streamStatsService.getAggregatedStatsForJob(JOB_ID) } returns emptyList()

    // Execute
    service.finalizeStats(JOB_ID)

    // Verify StreamStatsService was called with correct jobId
    verify(exactly = 1) { streamStatsService.getAggregatedStatsForJob(JOB_ID) }
  }

  private fun createMockJob(): Job {
    val syncConfig = JobSyncConfig()
    syncConfig.sourceDefinitionVersionId = SOURCE_DEF_ID
    syncConfig.destinationDefinitionVersionId = DEST_DEF_ID
    syncConfig.workspaceId = WORKSPACE_ID

    val jobConfig = JobConfig()
    jobConfig.configType = JobConfig.ConfigType.SYNC
    jobConfig.sync = syncConfig

    return Job(
      id = JOB_ID,
      configType = JobConfig.ConfigType.SYNC,
      scope = CONNECTION_ID_STR,
      config = jobConfig,
      attempts = listOf(), // Empty list is fine - we're testing finalizeStats, not attempt logic
      status = JobStatus.SUCCEEDED,
      startedAtInSecond = null,
      createdAtInSecond = 1000L,
      updatedAtInSecond = 1200L,
      isScheduled = false,
    )
  }

  private fun createMockStandardSync(): StandardSync {
    val sync = StandardSync()
    sync.sourceId = SOURCE_ID
    sync.destinationId = DEST_ID
    return sync
  }

  private fun createMockActorDefinitionVersion(
    actorDefinitionId: UUID,
    dockerImageTag: String,
  ): ActorDefinitionVersion {
    val version = ActorDefinitionVersion()
    version.actorDefinitionId = actorDefinitionId
    version.dockerImageTag = dockerImageTag
    return version
  }

  @Test
  fun `test getDestinationDefinitionVersionId with sync config`() {
    val syncConfig = JobSyncConfig()
    syncConfig.destinationDefinitionVersionId = SYNC_DESTINATION_VERSION_ID

    val jobConfig = JobConfig()
    jobConfig.sync = syncConfig

    val result = JobObservabilityService.getDestinationDefinitionVersionId(jobConfig)

    assertEquals(SYNC_DESTINATION_VERSION_ID, result)
  }

  @Test
  fun `test getDestinationDefinitionVersionId with resetConnection config`() {
    val resetConfig = JobResetConnectionConfig()
    resetConfig.destinationDefinitionVersionId = RESET_DESTINATION_VERSION_ID

    val jobConfig = JobConfig()
    jobConfig.resetConnection = resetConfig

    val result = JobObservabilityService.getDestinationDefinitionVersionId(jobConfig)

    assertEquals(RESET_DESTINATION_VERSION_ID, result)
  }

  @Test
  fun `test getDestinationDefinitionVersionId with refresh config`() {
    val refreshConfig = RefreshConfig()
    refreshConfig.destinationDefinitionVersionId = REFRESH_DESTINATION_VERSION_ID

    val jobConfig = JobConfig()
    jobConfig.refresh = refreshConfig

    val result = JobObservabilityService.getDestinationDefinitionVersionId(jobConfig)

    assertEquals(REFRESH_DESTINATION_VERSION_ID, result)
  }

  @Test
  fun `test getDestinationDefinitionVersionId with no config returns UUID_ZERO`() {
    val jobConfig = JobConfig()

    val result = JobObservabilityService.getDestinationDefinitionVersionId(jobConfig)

    assertEquals(UUID_ZERO, result)
  }

  @Test
  fun `test getDestinationDefinitionVersionId prioritizes sync over resetConnection`() {
    val syncConfig = JobSyncConfig()
    syncConfig.destinationDefinitionVersionId = SYNC_DESTINATION_VERSION_ID

    val resetConfig = JobResetConnectionConfig()
    resetConfig.destinationDefinitionVersionId = RESET_DESTINATION_VERSION_ID

    val jobConfig = JobConfig()
    jobConfig.sync = syncConfig
    jobConfig.resetConnection = resetConfig

    val result = JobObservabilityService.getDestinationDefinitionVersionId(jobConfig)

    assertEquals(SYNC_DESTINATION_VERSION_ID, result)
  }

  @Test
  fun `test getSourceDefinitionVersionId with sync config`() {
    val syncConfig = JobSyncConfig()
    syncConfig.sourceDefinitionVersionId = SYNC_SOURCE_VERSION_ID

    val jobConfig = JobConfig()
    jobConfig.sync = syncConfig

    val result = JobObservabilityService.getSourceDefinitionVersionId(jobConfig)

    assertEquals(SYNC_SOURCE_VERSION_ID, result)
  }

  @Test
  fun `test getSourceDefinitionVersionId with refresh config`() {
    val refreshConfig = RefreshConfig()
    refreshConfig.sourceDefinitionVersionId = REFRESH_SOURCE_VERSION_ID

    val jobConfig = JobConfig()
    jobConfig.refresh = refreshConfig

    val result = JobObservabilityService.getSourceDefinitionVersionId(jobConfig)

    assertEquals(REFRESH_SOURCE_VERSION_ID, result)
  }

  @Test
  fun `test getSourceDefinitionVersionId with resetConnection config returns null`() {
    val resetConfig = JobResetConnectionConfig()
    resetConfig.destinationDefinitionVersionId = RESET_DESTINATION_VERSION_ID

    val jobConfig = JobConfig()
    jobConfig.resetConnection = resetConfig

    val result = JobObservabilityService.getSourceDefinitionVersionId(jobConfig)

    assertNull(result)
  }

  @Test
  fun `test getSourceDefinitionVersionId with no config returns null`() {
    val jobConfig = JobConfig()

    val result = JobObservabilityService.getSourceDefinitionVersionId(jobConfig)

    assertNull(result)
  }

  @Test
  fun `test getSourceDefinitionVersionId prioritizes sync over refresh`() {
    val syncConfig = JobSyncConfig()
    syncConfig.sourceDefinitionVersionId = SYNC_SOURCE_VERSION_ID

    val refreshConfig = RefreshConfig()
    refreshConfig.sourceDefinitionVersionId = REFRESH_SOURCE_VERSION_ID

    val jobConfig = JobConfig()
    jobConfig.sync = syncConfig
    jobConfig.refresh = refreshConfig

    val result = JobObservabilityService.getSourceDefinitionVersionId(jobConfig)

    assertEquals(SYNC_SOURCE_VERSION_ID, result)
  }

  @Test
  fun `test getWorkspaceId with sync config`() {
    val syncConfig = JobSyncConfig()
    syncConfig.workspaceId = SYNC_WORKSPACE_ID

    val jobConfig = JobConfig()
    jobConfig.sync = syncConfig

    val result = JobObservabilityService.getWorkspaceId(jobConfig)

    assertEquals(SYNC_WORKSPACE_ID, result)
  }

  @Test
  fun `test getWorkspaceId with resetConnection config`() {
    val resetConfig = JobResetConnectionConfig()
    resetConfig.workspaceId = RESET_WORKSPACE_ID

    val jobConfig = JobConfig()
    jobConfig.resetConnection = resetConfig

    val result = JobObservabilityService.getWorkspaceId(jobConfig)

    assertEquals(RESET_WORKSPACE_ID, result)
  }

  @Test
  fun `test getWorkspaceId with refresh config`() {
    val refreshConfig = RefreshConfig()
    refreshConfig.workspaceId = REFRESH_WORKSPACE_ID

    val jobConfig = JobConfig()
    jobConfig.refresh = refreshConfig

    val result = JobObservabilityService.getWorkspaceId(jobConfig)

    assertEquals(REFRESH_WORKSPACE_ID, result)
  }

  @Test
  fun `test getWorkspaceId with no config returns UUID_ZERO`() {
    val jobConfig = JobConfig()

    val result = JobObservabilityService.getWorkspaceId(jobConfig)

    assertEquals(UUID_ZERO, result)
  }

  @Test
  fun `test getWorkspaceId prioritizes sync over resetConnection and refresh`() {
    val syncConfig = JobSyncConfig()
    syncConfig.workspaceId = SYNC_WORKSPACE_ID

    val resetConfig = JobResetConnectionConfig()
    resetConfig.workspaceId = RESET_WORKSPACE_ID

    val refreshConfig = RefreshConfig()
    refreshConfig.workspaceId = REFRESH_WORKSPACE_ID

    val jobConfig = JobConfig()
    jobConfig.sync = syncConfig
    jobConfig.resetConnection = resetConfig
    jobConfig.refresh = refreshConfig

    val result = JobObservabilityService.getWorkspaceId(jobConfig)

    assertEquals(SYNC_WORKSPACE_ID, result)
  }
}
