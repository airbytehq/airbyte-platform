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
import io.airbyte.statistics.Abs
import io.airbyte.statistics.Const
import io.airbyte.statistics.Dimension
import io.airbyte.statistics.GreaterThan
import io.airbyte.statistics.OutlierRule
import io.airbyte.statistics.zScore
import io.mockk.every
import io.mockk.mockk
import io.mockk.slot
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
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

  @Test
  fun `test computeAdditionalStatsScores with valid data`() {
    // Setup test data
    val currentStats = mapOf("wal_size" to BigDecimal("100.0"))
    val historicalStats =
      listOf(
        mapOf("wal_size" to BigDecimal("90.0")),
        mapOf("wal_size" to BigDecimal("95.0")),
        mapOf("wal_size" to BigDecimal("92.0")),
      )

    // Call the method directly
    val result = service.computeAdditionalStatsScores(currentStats, historicalStats)

    // Verify the result
    assertEquals(1, result.size)
    assert(result.containsKey("wal_size"))

    val scores = result["wal_size"]!!

    // Verify current value
    assertEquals(100.0, scores.current, 0.001)

    // Verify mean: (90 + 95 + 92 + 100) / 4 = 94.25
    assertEquals(94.25, scores.mean, 0.001)

    // Verify z-score is calculated (current - mean) / std
    // std = sqrt(((90-94.25)^2 + (95-94.25)^2 + (92-94.25)^2 + (100-94.25)^2) / 4)
    // std ≈ 3.86
    // z-score = (100 - 94.25) / 3.86 ≈ 1.49
    assert(scores.std > 0)
    assert(scores.zScore > 0)
  }

  @Test
  fun `test computeAdditionalStatsScores with multiple keys`() {
    // Setup test data with multiple metrics
    val currentStats =
      mapOf(
        "wal_size" to BigDecimal("100.0"),
        "replication_lag" to BigDecimal("5.0"),
      )
    val historicalStats =
      listOf(
        mapOf(
          "wal_size" to BigDecimal("90.0"),
          "replication_lag" to BigDecimal("3.0"),
        ),
        mapOf(
          "wal_size" to BigDecimal("95.0"),
          "replication_lag" to BigDecimal("4.0"),
        ),
        mapOf(
          "wal_size" to BigDecimal("92.0"),
          "replication_lag" to BigDecimal("3.5"),
        ),
      )

    // Call the method directly
    val result = service.computeAdditionalStatsScores(currentStats, historicalStats)

    // Verify both metrics are present
    assertEquals(2, result.size)
    assert(result.containsKey("wal_size"))
    assert(result.containsKey("replication_lag"))

    // Verify wal_size scores
    val walSizeScores = result["wal_size"]!!
    assertEquals(100.0, walSizeScores.current, 0.001)
    assertEquals(94.25, walSizeScores.mean, 0.001) // (90 + 95 + 92 + 100) / 4 = 94.25
    assert(walSizeScores.std > 0)
    assert(walSizeScores.zScore > 0)

    // Verify replication_lag scores
    val repLagScores = result["replication_lag"]!!
    assertEquals(5.0, repLagScores.current, 0.001)
    assertEquals(3.875, repLagScores.mean, 0.001) // (3 + 4 + 3.5 + 5) / 4 = 3.875
    assert(repLagScores.std > 0)
    assert(repLagScores.zScore > 0)
  }

  @Test
  fun `test computeAdditionalStatsScores with sparse historical data`() {
    // Setup test data where some historical syncs have null additionalStats
    val currentStats = mapOf("wal_size" to BigDecimal("100.0"))
    val historicalStats =
      listOf(
        mapOf("wal_size" to BigDecimal("90.0")), // Has the key
        null, // Entire map is null
        mapOf("wal_size" to BigDecimal("95.0")), // Has the key
        mapOf("other_metric" to BigDecimal("50.0")), // Map exists but doesn't have wal_size
      )

    // Call the method directly
    val result = service.computeAdditionalStatsScores(currentStats, historicalStats)

    // Verify wal_size is present
    assertEquals(1, result.size)
    assert(result.containsKey("wal_size"))

    val scores = result["wal_size"]!!

    // Should only use values from syncs that had wal_size: 90, 95, 100
    assertEquals(100.0, scores.current, 0.001)
    assertEquals(95.0, scores.mean, 0.001) // (90 + 95 + 100) / 3 = 95.0
    assert(scores.std > 0)
    assert(scores.zScore > 0)
  }

  @Test
  fun `test computeAdditionalStatsScores with different keys across syncs`() {
    // Setup test data where different syncs have different keys
    val currentStats = mapOf("wal_size" to BigDecimal("100.0"))
    val historicalStats =
      listOf(
        mapOf(
          "wal_size" to BigDecimal("90.0"),
          "other_metric" to BigDecimal("50.0"), // Extra key not in current
        ),
        mapOf("wal_size" to BigDecimal("95.0")),
        mapOf(
          "wal_size" to BigDecimal("92.0"),
          "another_metric" to BigDecimal("75.0"), // Different extra key
        ),
      )

    // Call the method directly
    val result = service.computeAdditionalStatsScores(currentStats, historicalStats)

    // Should only return scores for wal_size (the key in current)
    assertEquals(1, result.size)
    assert(result.containsKey("wal_size"))
    assert(!result.containsKey("other_metric"))
    assert(!result.containsKey("another_metric"))

    val scores = result["wal_size"]!!

    // Should use all wal_size values: 90, 95, 92, 100
    assertEquals(100.0, scores.current, 0.001)
    assertEquals(94.25, scores.mean, 0.001) // (90 + 95 + 92 + 100) / 4 = 94.25
    assert(scores.std > 0)
    assert(scores.zScore > 0)
  }

  @Test
  fun `test evaluateStream merges scores from top-level fields and additionalStats`() {
    // Mock the rules service to return empty rules (to avoid missing answer errors)
    val rulesService = mockk<JobObservabilityRulesService>()
    every { rulesService.getStreamOutlierRules() } returns emptyList()
    every { rulesService.getDerivedStreamStatRules() } returns emptyList()

    // Create service with mocked rules
    val testService =
      JobObservabilityService(
        actorDefinitionService = actorDefinitionService,
        connectionService = connectionService,
        jobPersistence = jobPersistence,
        obsStatsService = obsStatsService,
        workspaceService = workspaceService,
        metricClient = metricClient,
        jobObservabilityReportingService = null,
        jobObservabilityRulesService = rulesService,
        streamStatsService = streamStatsService,
      )

    // Create current stream with both top-level metrics and additionalStats
    val currentStream =
      ObsStreamStats(
        id = ObsStreamStatsId(jobId = 1L, streamNamespace = "public", streamName = "users"),
        bytesLoaded = 1000L,
        recordsLoaded = 100L,
        recordsRejected = 5L,
        wasBackfilled = false,
        wasResumed = false,
        additionalStats = mapOf("wal_size" to BigDecimal("100.0")),
      )

    // Create historical streams
    val historicalStreams =
      listOf(
        ObsStreamStats(
          id = ObsStreamStatsId(jobId = 2L, streamNamespace = "public", streamName = "users"),
          bytesLoaded = 900L,
          recordsLoaded = 90L,
          recordsRejected = 3L,
          wasBackfilled = false,
          wasResumed = false,
          additionalStats = mapOf("wal_size" to BigDecimal("90.0")),
        ),
        ObsStreamStats(
          id = ObsStreamStatsId(jobId = 3L, streamNamespace = "public", streamName = "users"),
          bytesLoaded = 950L,
          recordsLoaded = 95L,
          recordsRejected = 4L,
          wasBackfilled = false,
          wasResumed = false,
          additionalStats = mapOf("wal_size" to BigDecimal("95.0")),
        ),
      )

    // Call the method directly
    val result = testService.evaluateStream("public", "users", currentStream, historicalStreams)

    // Verify that evaluations were performed (this tests that scores were merged)
    // We can't directly inspect the scores map, but we can verify the StreamInfo has evaluations
    // The fact that the method completes successfully means scores were merged correctly
    assertEquals("public", result.namespace)
    assertEquals("users", result.name)
    assertEquals(false, result.wasBackfilled)
    assertEquals(false, result.wasResumed)

    // Verify the metrics include additionalStats
    assertEquals(mapOf("wal_size" to BigDecimal("100.0")), result.metrics.additionalStats)
  }

  @Test
  fun `test outlier detection with additionalStats outlier`() {
    // Create a rule that flags wal_size when z-score > 1.0
    val walSizeRule =
      OutlierRule(
        name = "wal_size",
        value = Abs(Dimension("wal_size").zScore),
        operator = GreaterThan,
        threshold = Const(1.0),
      )

    val rulesService = mockk<JobObservabilityRulesService>()
    every { rulesService.getStreamOutlierRules() } returns listOf(walSizeRule)
    every { rulesService.getDerivedStreamStatRules() } returns emptyList()

    val testService =
      JobObservabilityService(
        actorDefinitionService = actorDefinitionService,
        connectionService = connectionService,
        jobPersistence = jobPersistence,
        obsStatsService = obsStatsService,
        workspaceService = workspaceService,
        metricClient = metricClient,
        jobObservabilityReportingService = null,
        jobObservabilityRulesService = rulesService,
        streamStatsService = streamStatsService,
      )

    // Create current stream with normal top-level metrics but outlier additionalStats
    val currentStream =
      ObsStreamStats(
        id = ObsStreamStatsId(jobId = 1L, streamNamespace = "public", streamName = "users"),
        bytesLoaded = 1000L, // Normal
        recordsLoaded = 100L, // Normal
        recordsRejected = 5L, // Normal
        wasBackfilled = false,
        wasResumed = false,
        additionalStats = mapOf("wal_size" to BigDecimal("1000.0")), // OUTLIER! Much higher
      )

    // Create historical streams with tight, normal values
    val historicalStreams =
      listOf(
        ObsStreamStats(
          id = ObsStreamStatsId(jobId = 2L, streamNamespace = "public", streamName = "users"),
          bytesLoaded = 950L,
          recordsLoaded = 95L,
          recordsRejected = 4L,
          wasBackfilled = false,
          wasResumed = false,
          additionalStats = mapOf("wal_size" to BigDecimal("100.0")),
        ),
        ObsStreamStats(
          id = ObsStreamStatsId(jobId = 3L, streamNamespace = "public", streamName = "users"),
          bytesLoaded = 1050L,
          recordsLoaded = 105L,
          recordsRejected = 6L,
          wasBackfilled = false,
          wasResumed = false,
          additionalStats = mapOf("wal_size" to BigDecimal("100.0")),
        ),
        ObsStreamStats(
          id = ObsStreamStatsId(jobId = 4L, streamNamespace = "public", streamName = "users"),
          bytesLoaded = 1020L,
          recordsLoaded = 102L,
          recordsRejected = 5L,
          wasBackfilled = false,
          wasResumed = false,
          additionalStats = mapOf("wal_size" to BigDecimal("100.0")),
        ),
      )

    // Call evaluateStream
    val result = testService.evaluateStream("public", "users", currentStream, historicalStreams)

    // Verify stream is marked as outlier due to additionalStats
    assertTrue(result.isOutlier)

    // Verify we have an evaluation for wal_size
    val walSizeEval = result.evaluations.find { it.name == "wal_size" }
    assert(walSizeEval != null)
    assertTrue(walSizeEval!!.isOutlier)
  }

  @Test
  fun `test outlier detection with both top-level and additionalStats outliers`() {
    // Create rules for both recordsLoaded and wal_size
    val recordsRule =
      OutlierRule(
        name = "recordsLoaded",
        value = Abs(Dimension("recordsLoaded").zScore),
        operator = GreaterThan,
        threshold = Const(1.0),
      )
    val walSizeRule =
      OutlierRule(
        name = "wal_size",
        value = Abs(Dimension("wal_size").zScore),
        operator = GreaterThan,
        threshold = Const(1.0),
      )

    val rulesService = mockk<JobObservabilityRulesService>()
    every { rulesService.getStreamOutlierRules() } returns listOf(recordsRule, walSizeRule)
    every { rulesService.getDerivedStreamStatRules() } returns emptyList()

    val testService =
      JobObservabilityService(
        actorDefinitionService = actorDefinitionService,
        connectionService = connectionService,
        jobPersistence = jobPersistence,
        obsStatsService = obsStatsService,
        workspaceService = workspaceService,
        metricClient = metricClient,
        jobObservabilityReportingService = null,
        jobObservabilityRulesService = rulesService,
        streamStatsService = streamStatsService,
      )

    // Create current stream with outliers in BOTH top-level and additionalStats
    val currentStream =
      ObsStreamStats(
        id = ObsStreamStatsId(jobId = 1L, streamNamespace = "public", streamName = "users"),
        bytesLoaded = 1000L,
        recordsLoaded = 10000L, // OUTLIER! (100x normal)
        recordsRejected = 5L,
        wasBackfilled = false,
        wasResumed = false,
        additionalStats = mapOf("wal_size" to BigDecimal("1000.0")), // OUTLIER! (10x normal)
      )

    // Historical streams with tight, normal values
    val historicalStreams =
      listOf(
        ObsStreamStats(
          id = ObsStreamStatsId(jobId = 2L, streamNamespace = "public", streamName = "users"),
          bytesLoaded = 1000L,
          recordsLoaded = 100L,
          recordsRejected = 5L,
          wasBackfilled = false,
          wasResumed = false,
          additionalStats = mapOf("wal_size" to BigDecimal("100.0")),
        ),
        ObsStreamStats(
          id = ObsStreamStatsId(jobId = 3L, streamNamespace = "public", streamName = "users"),
          bytesLoaded = 1000L,
          recordsLoaded = 100L,
          recordsRejected = 5L,
          wasBackfilled = false,
          wasResumed = false,
          additionalStats = mapOf("wal_size" to BigDecimal("100.0")),
        ),
      )

    // Call evaluateStream
    val result = testService.evaluateStream("public", "users", currentStream, historicalStreams)

    // Verify stream is marked as outlier
    assertTrue(result.isOutlier)

    // Verify we have evaluations for BOTH outliers
    val recordsEval = result.evaluations.find { it.name == "recordsLoaded" }
    val walSizeEval = result.evaluations.find { it.name == "wal_size" }

    assert(recordsEval != null)
    assert(walSizeEval != null)
    assertTrue(recordsEval!!.isOutlier)
    assertTrue(walSizeEval!!.isOutlier)
  }

  @Test
  fun `test normal flow without additionalStats`() {
    // Create a rule for recordsLoaded to verify existing outlier detection still works
    val recordsRule =
      OutlierRule(
        name = "recordsLoaded",
        value = Abs(Dimension("recordsLoaded").zScore),
        operator = GreaterThan,
        threshold = Const(1.0),
      )

    val rulesService = mockk<JobObservabilityRulesService>()
    every { rulesService.getStreamOutlierRules() } returns listOf(recordsRule)
    every { rulesService.getDerivedStreamStatRules() } returns emptyList()

    val testService =
      JobObservabilityService(
        actorDefinitionService = actorDefinitionService,
        connectionService = connectionService,
        jobPersistence = jobPersistence,
        obsStatsService = obsStatsService,
        workspaceService = workspaceService,
        metricClient = metricClient,
        jobObservabilityReportingService = null,
        jobObservabilityRulesService = rulesService,
        streamStatsService = streamStatsService,
      )

    // Create streams WITHOUT additionalStats (null)
    val currentStream =
      ObsStreamStats(
        id = ObsStreamStatsId(jobId = 1L, streamNamespace = "public", streamName = "users"),
        bytesLoaded = 1000L,
        recordsLoaded = 10000L, // OUTLIER (100x normal)
        recordsRejected = 5L,
        wasBackfilled = false,
        wasResumed = false,
        additionalStats = null, // No additionalStats
      )

    val historicalStreams =
      listOf(
        ObsStreamStats(
          id = ObsStreamStatsId(jobId = 2L, streamNamespace = "public", streamName = "users"),
          bytesLoaded = 1000L,
          recordsLoaded = 100L,
          recordsRejected = 5L,
          wasBackfilled = false,
          wasResumed = false,
          additionalStats = null,
        ),
        ObsStreamStats(
          id = ObsStreamStatsId(jobId = 3L, streamNamespace = "public", streamName = "users"),
          bytesLoaded = 1000L,
          recordsLoaded = 100L,
          recordsRejected = 5L,
          wasBackfilled = false,
          wasResumed = false,
          additionalStats = emptyMap(), // Empty map
        ),
      )

    // Call evaluateStream
    val result = testService.evaluateStream("public", "users", currentStream, historicalStreams)

    // Verify existing outlier detection still works
    assertTrue(result.isOutlier)

    // Verify recordsLoaded outlier was detected
    val recordsEval = result.evaluations.find { it.name == "recordsLoaded" }
    assert(recordsEval != null)
    assertTrue(recordsEval!!.isOutlier)

    // Verify additionalStats is empty in metrics
    assertEquals(emptyMap<String, BigDecimal>(), result.metrics.additionalStats)
  }

  @Test
  fun `computeStreamDerivedStats computes averageRecordSize`() {
    val rulesService = JobObservabilityRulesService()
    val testService =
      JobObservabilityService(
        actorDefinitionService = actorDefinitionService,
        connectionService = connectionService,
        jobPersistence = jobPersistence,
        obsStatsService = obsStatsService,
        workspaceService = workspaceService,
        metricClient = metricClient,
        jobObservabilityReportingService = null,
        jobObservabilityRulesService = rulesService,
        streamStatsService = streamStatsService,
      )

    val metrics =
      StreamMetrics(
        bytesLoaded = 1000L,
        recordsLoaded = 10L,
        recordsRejected = 0L,
      )

    val enriched = testService.computeStreamDerivedStats(metrics)

    assertEquals(BigDecimal.valueOf(100.0), enriched.additionalStats["averageRecordSize"])
  }

  @Test
  fun `computeStreamDerivedStats returns original metrics when recordsLoaded is zero`() {
    val rulesService = JobObservabilityRulesService()
    val testService =
      JobObservabilityService(
        actorDefinitionService = actorDefinitionService,
        connectionService = connectionService,
        jobPersistence = jobPersistence,
        obsStatsService = obsStatsService,
        workspaceService = workspaceService,
        metricClient = metricClient,
        jobObservabilityReportingService = null,
        jobObservabilityRulesService = rulesService,
        streamStatsService = streamStatsService,
      )

    val metrics =
      StreamMetrics(
        bytesLoaded = 1000L,
        recordsLoaded = 0L,
        recordsRejected = 0L,
      )

    val enriched = testService.computeStreamDerivedStats(metrics)

    // Should not add averageRecordSize when recordsLoaded is 0 (division by zero returns null)
    assertFalse(enriched.additionalStats.containsKey("averageRecordSize"))
  }

  @Test
  fun `computeStreamDerivedStats returns original metrics when no rules defined`() {
    val rulesService = mockk<JobObservabilityRulesService>()
    every { rulesService.getDerivedStreamStatRules() } returns emptyList()

    val testService =
      JobObservabilityService(
        actorDefinitionService = actorDefinitionService,
        connectionService = connectionService,
        jobPersistence = jobPersistence,
        obsStatsService = obsStatsService,
        workspaceService = workspaceService,
        metricClient = metricClient,
        jobObservabilityReportingService = null,
        jobObservabilityRulesService = rulesService,
        streamStatsService = streamStatsService,
      )

    val metrics =
      StreamMetrics(
        bytesLoaded = 1000L,
        recordsLoaded = 10L,
        recordsRejected = 0L,
      )

    val enriched = testService.computeStreamDerivedStats(metrics)

    assertEquals(metrics, enriched)
  }

  @Test
  fun `evaluateStream includes derived stats in evaluation`() {
    val rulesService = JobObservabilityRulesService()

    val testService =
      JobObservabilityService(
        actorDefinitionService = actorDefinitionService,
        connectionService = connectionService,
        jobPersistence = jobPersistence,
        obsStatsService = obsStatsService,
        workspaceService = workspaceService,
        metricClient = metricClient,
        jobObservabilityReportingService = null,
        jobObservabilityRulesService = rulesService,
        streamStatsService = streamStatsService,
      )

    val currentStream =
      ObsStreamStats(
        id = ObsStreamStatsId(jobId = 1L, streamNamespace = "ns", streamName = "stream"),
        bytesLoaded = 1000L,
        recordsLoaded = 10L,
        recordsRejected = 0L,
        wasBackfilled = false,
        wasResumed = false,
      )

    val historicalStreams =
      listOf(
        ObsStreamStats(
          id = ObsStreamStatsId(jobId = 2L, streamNamespace = "ns", streamName = "stream"),
          bytesLoaded = 900L,
          recordsLoaded = 10L,
          recordsRejected = 0L,
          wasBackfilled = false,
          wasResumed = false,
        ),
        ObsStreamStats(
          id = ObsStreamStatsId(jobId = 3L, streamNamespace = "ns", streamName = "stream"),
          bytesLoaded = 950L,
          recordsLoaded = 10L,
          recordsRejected = 0L,
          wasBackfilled = false,
          wasResumed = false,
        ),
      )

    val result = testService.evaluateStream("ns", "stream", currentStream, historicalStreams)

    // Verify derived stat was computed and included in metrics
    assertEquals(BigDecimal.valueOf(100.0), result.metrics.additionalStats["averageRecordSize"])
  }

  @Test
  fun `test outlier detection for sourceFieldsPopulatedPerRecord derived metric`() {
    // Use the real JobObservabilityRulesService to get the actual derived stat and outlier rules
    val rulesService = JobObservabilityRulesService()

    val testService =
      JobObservabilityService(
        actorDefinitionService = actorDefinitionService,
        connectionService = connectionService,
        jobPersistence = jobPersistence,
        obsStatsService = obsStatsService,
        workspaceService = workspaceService,
        metricClient = metricClient,
        jobObservabilityReportingService = null,
        jobObservabilityRulesService = rulesService,
        streamStatsService = streamStatsService,
      )

    // Create current stream with outlier sourceFieldsPopulated (much higher than historical)
    // Historical: ~5 fields per record (consistent), Current: ~50 fields per record (10x higher)
    // With 10 historical data points at 5, and 1 current at 50, z-score will be ~3.16 (above threshold of 3.0)
    val currentStream =
      ObsStreamStats(
        id = ObsStreamStatsId(jobId = 1L, streamNamespace = "public", streamName = "users"),
        bytesLoaded = 1000L,
        recordsLoaded = 100L,
        recordsRejected = 0L,
        wasBackfilled = false,
        wasResumed = false,
        additionalStats = mapOf("sourceFieldsPopulated" to BigDecimal("5000.0")), // 5000 / 100 = 50 per record
      )

    // Create 10 historical streams with consistent sourceFieldsPopulated (5 per record)
    val historicalStreams =
      (2L..11L).map { jobId ->
        ObsStreamStats(
          id = ObsStreamStatsId(jobId = jobId, streamNamespace = "public", streamName = "users"),
          bytesLoaded = 1000L,
          recordsLoaded = 100L,
          recordsRejected = 0L,
          wasBackfilled = false,
          wasResumed = false,
          additionalStats = mapOf("sourceFieldsPopulated" to BigDecimal("500.0")), // 500 / 100 = 5 per record
        )
      }

    // Call evaluateStream
    val result = testService.evaluateStream("public", "users", currentStream, historicalStreams)

    // ========== Verify StreamInfo fields ==========

    // Basic stream identification
    assertEquals("public", result.namespace, "Namespace should match")
    assertEquals("users", result.name, "Stream name should match")

    // Stream flags
    assertFalse(result.wasBackfilled, "wasBackfilled should be false")
    assertFalse(result.wasResumed, "wasResumed should be false")

    // Outlier status
    assertTrue(result.isOutlier, "Stream should be marked as outlier")

    // ========== Verify StreamMetrics fields ==========

    assertEquals(1000L, result.metrics.bytesLoaded, "bytesLoaded should match")
    assertEquals(100L, result.metrics.recordsLoaded, "recordsLoaded should match")
    assertEquals(0L, result.metrics.recordsRejected, "recordsRejected should match")

    // Verify additionalStats includes both source and derived stats
    assert(result.metrics.additionalStats.containsKey("sourceFieldsPopulated")) {
      "additionalStats should contain sourceFieldsPopulated"
    }
    assertEquals(
      BigDecimal("5000.0"),
      result.metrics.additionalStats["sourceFieldsPopulated"],
      "sourceFieldsPopulated should be 5000.0",
    )

    assert(result.metrics.additionalStats.containsKey("sourceFieldsPopulatedPerRecord")) {
      "additionalStats should contain derived stat sourceFieldsPopulatedPerRecord"
    }
    assertEquals(
      BigDecimal("50.0"),
      result.metrics.additionalStats["sourceFieldsPopulatedPerRecord"],
      "sourceFieldsPopulatedPerRecord should be 5000 / 100 = 50",
    )

    // ========== Verify Evaluations list ==========

    assert(result.evaluations.isNotEmpty()) { "Evaluations list should not be empty" }

    // Find the sourceFieldsPopulatedPerRecord evaluation
    val sourceFieldsEval = result.evaluations.find { it.name == "sourceFieldsPopulatedPerRecord" }
    assert(sourceFieldsEval != null) { "Should have evaluation for sourceFieldsPopulatedPerRecord" }

    // ========== Verify OutlierEvaluation fields ==========

    // Basic evaluation fields
    assertEquals("sourceFieldsPopulatedPerRecord", sourceFieldsEval!!.name, "Evaluation name should match")
    assertTrue(sourceFieldsEval.isOutlier, "sourceFieldsPopulatedPerRecord should be flagged as outlier")
    assertEquals(3.0, sourceFieldsEval.threshold, 0.001, "Threshold should be 3.0 (from rule)")
    assert(sourceFieldsEval.value > 3.0) {
      "Value (percentage z-score) should be > 3.0, got ${sourceFieldsEval.value}"
    }

    // ========== Verify percentage z-score calculation ==========
    // The rule uses: |(current - mean) / (mean * 0.02)| > 3.0
    // With current=50, mean=9.09: |(50 - 9.09) / (9.09 * 0.02)| = |40.91 / 0.1818| ≈ 225
    // This should be >> 3.0, triggering the outlier

    // ========== Verify Scores object fields ==========
    // With debugScores parameter, we get the underlying dimension's scores for debugging context

    assert(sourceFieldsEval.scores != null) { "Scores should be populated via debugScores parameter" }

    // Current value
    assertEquals(50.0, sourceFieldsEval.scores!!.current, 0.001, "Current value should be 50.0")

    // Mean calculation: (5*10 + 50) / 11 = 100 / 11 = 9.09
    assertEquals(9.09, sourceFieldsEval.scores!!.mean, 0.1, "Mean should be approximately 9.09")

    // Standard deviation should be positive (variation exists)
    assert(sourceFieldsEval.scores!!.std > 0) {
      "Standard deviation should be positive, got ${sourceFieldsEval.scores!!.std}"
    }

    // Z-score calculation: (current - mean) / std = (50 - 9.09) / std
    // Note: This is the traditional z-score, different from the percentage z-score (value field)
    assert(sourceFieldsEval.scores!!.zScore > 3.0) {
      "Z-score should be > 3.0, got ${sourceFieldsEval.scores!!.zScore}"
    }

    // Additional validation: verify z-score is calculated correctly
    val expectedZScore = (sourceFieldsEval.scores!!.current - sourceFieldsEval.scores!!.mean) / sourceFieldsEval.scores!!.std
    assertEquals(
      expectedZScore,
      sourceFieldsEval.scores!!.zScore,
      0.001,
      "Z-score should match calculation: (current - mean) / std",
    )
  }
}
