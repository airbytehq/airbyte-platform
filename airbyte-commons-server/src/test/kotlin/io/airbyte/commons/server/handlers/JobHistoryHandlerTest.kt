/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers

import io.airbyte.api.server.generated.models.AttemptInfoRead
import io.airbyte.api.server.generated.models.AttemptRead
import io.airbyte.api.server.generated.models.AttemptStats
import io.airbyte.api.server.generated.models.AttemptStreamStats
import io.airbyte.api.server.generated.models.ConnectionIdRequestBody
import io.airbyte.api.server.generated.models.ConnectionSyncProgressRead
import io.airbyte.api.server.generated.models.DestinationIdRequestBody
import io.airbyte.api.server.generated.models.JobAggregatedStats
import io.airbyte.api.server.generated.models.JobConfigType
import io.airbyte.api.server.generated.models.JobDebugInfoRead
import io.airbyte.api.server.generated.models.JobDebugRead
import io.airbyte.api.server.generated.models.JobIdRequestBody
import io.airbyte.api.server.generated.models.JobInfoLightRead
import io.airbyte.api.server.generated.models.JobInfoRead
import io.airbyte.api.server.generated.models.JobListRequestBody
import io.airbyte.api.server.generated.models.JobRead
import io.airbyte.api.server.generated.models.JobReadList
import io.airbyte.api.server.generated.models.JobRefreshConfig
import io.airbyte.api.server.generated.models.JobWithAttemptsRead
import io.airbyte.api.server.generated.models.LogFormatType
import io.airbyte.api.server.generated.models.Pagination
import io.airbyte.api.server.generated.models.ResetConfig
import io.airbyte.api.server.generated.models.SourceIdRequestBody
import io.airbyte.api.server.generated.models.StreamDescriptor
import io.airbyte.api.server.generated.models.StreamStats
import io.airbyte.api.server.generated.models.StreamSyncProgressReadItem
import io.airbyte.commons.enums.convertTo
import io.airbyte.commons.enums.isCompatible
import io.airbyte.commons.json.Jsons.clone
import io.airbyte.commons.json.Jsons.emptyObject
import io.airbyte.commons.logging.LogClientManager
import io.airbyte.commons.logging.LogEvents
import io.airbyte.commons.logging.LogUtils
import io.airbyte.commons.server.converters.ApiPojoConverters
import io.airbyte.commons.server.converters.AttemptLogs
import io.airbyte.commons.server.converters.JobConverter
import io.airbyte.commons.server.converters.JobConverter.Companion.getJobRead
import io.airbyte.commons.server.converters.JobConverter.Companion.getJobWithAttemptsRead
import io.airbyte.commons.server.handlers.helpers.CatalogConverter
import io.airbyte.commons.server.helpers.ConnectionHelpers
import io.airbyte.commons.server.helpers.DestinationHelpers
import io.airbyte.commons.server.helpers.SourceHelpers
import io.airbyte.commons.temporal.TemporalClient
import io.airbyte.commons.version.AirbyteVersion
import io.airbyte.config.AirbyteStream
import io.airbyte.config.Attempt
import io.airbyte.config.AttemptStatus
import io.airbyte.config.ConfiguredAirbyteCatalog
import io.airbyte.config.ConfiguredAirbyteStream
import io.airbyte.config.DestinationSyncMode
import io.airbyte.config.Job
import io.airbyte.config.JobCheckConnectionConfig
import io.airbyte.config.JobConfig
import io.airbyte.config.JobConfig.ConfigType
import io.airbyte.config.JobResetConnectionConfig
import io.airbyte.config.JobStatus
import io.airbyte.config.JobSyncConfig
import io.airbyte.config.RefreshConfig
import io.airbyte.config.RefreshStream
import io.airbyte.config.ResetSourceConfiguration
import io.airbyte.config.StandardDestinationDefinition
import io.airbyte.config.StandardSourceDefinition
import io.airbyte.config.StreamSyncStats
import io.airbyte.config.SyncMode
import io.airbyte.config.SyncStats
import io.airbyte.config.helpers.FieldGenerator
import io.airbyte.data.services.ConnectionService
import io.airbyte.data.services.JobService
import io.airbyte.data.services.impls.jooq.ConnectionServiceJooqImpl
import io.airbyte.featureflag.ANONYMOUS
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.featureflag.HydrateAggregatedStats
import io.airbyte.featureflag.TestClient
import io.airbyte.featureflag.Workspace
import io.airbyte.persistence.job.JobPersistence
import io.airbyte.persistence.job.JobPersistence.JobAttemptPair
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.mockito.MockedStatic
import org.mockito.Mockito
import org.mockito.kotlin.anyOrNull
import org.mockito.kotlin.eq
import org.mockito.kotlin.whenever
import java.net.URI
import java.nio.file.Path
import java.util.Optional
import java.util.UUID
import java.util.function.Function
import java.util.stream.Collectors

@DisplayName("Job History Handler")
internal class JobHistoryHandlerTest {
  private val apiPojoConverters = ApiPojoConverters(CatalogConverter(FieldGenerator(), mutableListOf()))

  private lateinit var connectionService: ConnectionService
  private lateinit var sourceHandler: SourceHandler
  private lateinit var destinationHandler: DestinationHandler
  private lateinit var testJobAttempt: Attempt
  private lateinit var jobConverter: JobConverter
  private lateinit var jobPersistence: JobPersistence
  private lateinit var logClientManager: LogClientManager
  private lateinit var logUtils: LogUtils
  private lateinit var featureFlagClient: FeatureFlagClient
  private lateinit var jobHistoryHandler: JobHistoryHandler
  private lateinit var temporalClient: TemporalClient
  private lateinit var jobService: JobService
  private lateinit var sourceDefinitionsHandler: SourceDefinitionsHandler
  private lateinit var destinationDefinitionsHandler: DestinationDefinitionsHandler
  private lateinit var airbyteVersion: AirbyteVersion

  @BeforeEach
  fun setUp() {
    testJobAttempt = createAttempt(0, JOB_ID, CREATED_AT, AttemptStatus.SUCCEEDED)

    connectionService = Mockito.mock(ConnectionServiceJooqImpl::class.java)
    sourceHandler = Mockito.mock(SourceHandler::class.java)
    destinationHandler = Mockito.mock(DestinationHandler::class.java)
    jobPersistence = Mockito.mock(JobPersistence::class.java)
    logClientManager = Mockito.mock(LogClientManager::class.java)
    logUtils = Mockito.mock(LogUtils::class.java)
    featureFlagClient = Mockito.mock(TestClient::class.java)
    temporalClient = Mockito.mock(TemporalClient::class.java)
    jobConverter = JobConverter(logClientManager, logUtils)
    sourceDefinitionsHandler = Mockito.mock(SourceDefinitionsHandler::class.java)
    destinationDefinitionsHandler = Mockito.mock(DestinationDefinitionsHandler::class.java)
    airbyteVersion = Mockito.mock(AirbyteVersion::class.java)
    jobService = Mockito.mock(JobService::class.java)
    jobHistoryHandler =
      JobHistoryHandler(
        jobPersistence,
        connectionService,
        sourceHandler,
        sourceDefinitionsHandler,
        destinationHandler,
        destinationDefinitionsHandler,
        airbyteVersion,
        temporalClient,
        featureFlagClient,
        jobConverter,
        jobService,
        apiPojoConverters,
      )
  }

  @Nested
  @DisplayName("When listing jobs")
  internal inner class ListJobs {
    @Test
    @DisplayName("Should return jobs with/without attempts in descending order")
    fun testListJobs() {
      whenever(featureFlagClient.boolVariation(HydrateAggregatedStats, Workspace(ANONYMOUS))).thenReturn(true)

      val successfulJobAttempt2 = createAttempt(1, JOB_ID, CREATED_AT, AttemptStatus.SUCCEEDED)
      val successfulJob =
        Job(
          JOB_ID,
          JOB_CONFIG.configType,
          JOB_CONFIG_ID,
          JOB_CONFIG,
          listOf(testJobAttempt, successfulJobAttempt2),
          JOB_STATUS,
          null,
          CREATED_AT,
          CREATED_AT,
          true,
        )

      val pageSize = 25
      val rowOffset = 0

      val jobId2 = JOB_ID + 100
      val createdAt2 = CREATED_AT + 1000
      val latestJobNoAttempt =
        Job(
          jobId2,
          JOB_CONFIG.configType,
          JOB_CONFIG_ID,
          JOB_CONFIG,
          emptyList(),
          JobStatus.PENDING,
          null,
          createdAt2,
          createdAt2,
          true,
        )

      whenever(
        jobService.listJobs(
          eq(setOf(CONFIG_TYPE_FOR_API.convertTo<ConfigType>())),
          eq(JOB_CONFIG_ID),
          eq(pageSize),
          eq(rowOffset),
          anyOrNull(),
          anyOrNull(),
          anyOrNull(),
          anyOrNull(),
          anyOrNull(),
          anyOrNull(),
          anyOrNull(),
        ),
      ).thenReturn(listOf(latestJobNoAttempt, successfulJob))

      whenever(
        jobPersistence.getJobCount(
          eq(setOf(CONFIG_TYPE_FOR_API.convertTo<ConfigType>())),
          eq(JOB_CONFIG_ID),
          anyOrNull(),
          anyOrNull(),
          anyOrNull(),
          anyOrNull(),
          anyOrNull(),
        ),
      ).thenReturn(2L)

      whenever(
        jobPersistence.getAttemptStats(listOf(200L, 100L)),
      ).thenReturn(
        mapOf(
          JobAttemptPair(100, 0) to FIRST_ATTEMPT_STATS,
          JobAttemptPair(100, 1) to SECOND_ATTEMPT_STATS,
          JobAttemptPair(jobId2, 0) to FIRST_ATTEMPT_STATS,
        ),
      )

      val requestBody =
        JobListRequestBody(
          configTypes = listOf(CONFIG_TYPE_FOR_API),
          configId = JOB_CONFIG_ID,
          pagination = Pagination(pageSize = pageSize, rowOffset = rowOffset),
        )

      val jobReadList = jobHistoryHandler.listJobsFor(requestBody)

      val expectedAttemptRead1 =
        toAttemptRead(testJobAttempt).copy(totalStats = FIRST_ATTEMPT_STATS_API, streamStats = FIRST_ATTEMPT_STREAM_STATS)

      val expectedAttemptRead2 =
        toAttemptRead(successfulJobAttempt2).copy(totalStats = SECOND_ATTEMPT_STATS_API, streamStats = SECOND_ATTEMPT_STREAM_STATS)

      val successfulJobWithAttemptRead =
        JobWithAttemptsRead(
          job =
            toJobInfo(successfulJob).copy(
              aggregatedStats =
                JobAggregatedStats(
                  recordsEmitted = 5550L,
                  bytesEmitted = 2220L,
                  recordsCommitted = 5550L,
                  bytesCommitted = 2220L,
                  recordsRejected = 152L,
                ),
              streamAggregatedStats =
                listOf(
                  StreamStats(
                    streamName = "stream1",
                    streamNamespace = "ns1",
                    recordsEmitted = 500L,
                    bytesEmitted = 200L,
                    recordsCommitted = 500L,
                    bytesCommitted = 200L,
                    recordsRejected = 50L,
                  ),
                  StreamStats(
                    streamName = "stream2",
                    recordsEmitted = 5050L,
                    bytesEmitted = 2020L,
                    recordsCommitted = 5050L,
                    bytesCommitted = 2020L,
                    recordsRejected = 102L,
                  ),
                ),
            ),
          attempts = listOf(expectedAttemptRead1, expectedAttemptRead2),
        )

      val latestJobWithAttemptRead =
        JobWithAttemptsRead(
          job =
            toJobInfo(latestJobNoAttempt).copy(
              aggregatedStats =
                JobAggregatedStats(
                  recordsEmitted = 0L,
                  bytesEmitted = 0L,
                  recordsCommitted = 0L,
                  bytesCommitted = 0L,
                  recordsRejected = 0L,
                ),
              streamAggregatedStats = emptyList(),
            ),
          attempts = emptyList(),
        )

      val expectedJobReadList =
        JobReadList(
          jobs = listOf(latestJobWithAttemptRead, successfulJobWithAttemptRead),
          totalJobCount = 2L,
        )

      Assertions.assertEquals(expectedJobReadList, jobReadList)
    }

    @Test
    @DisplayName("Should return jobs in descending order regardless of type")
    fun testListJobsFor() {
      whenever(featureFlagClient.boolVariation(HydrateAggregatedStats, Workspace(ANONYMOUS))).thenReturn(true)

      val firstJob =
        Job(
          JOB_ID,
          JOB_CONFIG.configType,
          JOB_CONFIG_ID,
          JOB_CONFIG,
          listOf(testJobAttempt),
          JOB_STATUS,
          null,
          CREATED_AT,
          CREATED_AT,
          true,
        )

      val pageSize = 25
      val rowOffset = 0

      val secondJobId = JOB_ID + 100
      val createdAt2 = CREATED_AT + 1000
      val secondJobAttempt = createAttempt(0, secondJobId, createdAt2, AttemptStatus.SUCCEEDED)
      val secondJob =
        Job(
          secondJobId,
          ConfigType.SYNC,
          JOB_CONFIG_ID,
          JOB_CONFIG,
          listOf(secondJobAttempt),
          JobStatus.SUCCEEDED,
          null,
          createdAt2,
          createdAt2,
          true,
        )

      val configTypes =
        setOf(
          CONFIG_TYPE_FOR_API.convertTo<ConfigType>(),
          JobConfigType.SYNC.convertTo<ConfigType>(),
          JobConfigType.DISCOVER_SCHEMA.convertTo<ConfigType>(),
        )

      val latestJobId = secondJobId + 100
      val createdAt3 = createdAt2 + 1000
      val latestJob =
        Job(
          latestJobId,
          ConfigType.SYNC,
          JOB_CONFIG_ID,
          JOB_CONFIG,
          emptyList(),
          JobStatus.PENDING,
          null,
          createdAt3,
          createdAt3,
          true,
        )

      whenever(
        jobService.listJobs(
          eq(configTypes),
          eq(JOB_CONFIG_ID),
          eq(pageSize),
          eq(rowOffset),
          anyOrNull(),
          anyOrNull(),
          anyOrNull(),
          anyOrNull(),
          anyOrNull(),
          anyOrNull(),
          anyOrNull(),
        ),
      ).thenReturn(listOf(latestJob, secondJob, firstJob))

      whenever(
        jobPersistence.getJobCount(
          eq(configTypes),
          eq(JOB_CONFIG_ID),
          anyOrNull(),
          anyOrNull(),
          anyOrNull(),
          anyOrNull(),
          anyOrNull(),
        ),
      ).thenReturn(3L)

      whenever(
        jobPersistence.getAttemptStats(listOf(300L, 200L, 100L)),
      ).thenReturn(
        mapOf(
          JobAttemptPair(100, 0) to FIRST_ATTEMPT_STATS,
          JobAttemptPair(secondJobId, 0) to FIRST_ATTEMPT_STATS,
          JobAttemptPair(latestJobId, 0) to FIRST_ATTEMPT_STATS,
        ),
      )

      val requestBody =
        JobListRequestBody(
          configTypes = listOf(CONFIG_TYPE_FOR_API, JobConfigType.SYNC, JobConfigType.DISCOVER_SCHEMA),
          configId = JOB_CONFIG_ID,
          pagination = Pagination(pageSize = pageSize, rowOffset = rowOffset),
        )

      val jobReadList = jobHistoryHandler.listJobsFor(requestBody)

      val expectedStats =
        JobAggregatedStats(
          recordsEmitted = 55L,
          bytesEmitted = 22L,
          recordsCommitted = 55L,
          bytesCommitted = 22L,
          recordsRejected = 3L,
        )

      val expectedStreams =
        listOf(
          StreamStats(
            streamName = "stream1",
            streamNamespace = "ns1",
            recordsEmitted = 5L,
            bytesEmitted = 2L,
            recordsCommitted = 5L,
            bytesCommitted = 2L,
            recordsRejected = 1L,
          ),
          StreamStats(
            streamName = "stream2",
            recordsEmitted = 50L,
            bytesEmitted = 20L,
            recordsCommitted = 50L,
            bytesCommitted = 20L,
            recordsRejected = 2L,
          ),
        )

      val firstJobWithAttemptRead =
        JobWithAttemptsRead(
          job = toJobInfo(firstJob).copy(aggregatedStats = expectedStats, streamAggregatedStats = expectedStreams),
          attempts =
            listOf(
              toAttemptRead(testJobAttempt).copy(totalStats = FIRST_ATTEMPT_STATS_API, streamStats = FIRST_ATTEMPT_STREAM_STATS),
            ),
        )

      val secondJobWithAttemptRead =
        JobWithAttemptsRead(
          job = toJobInfo(secondJob).copy(aggregatedStats = expectedStats, streamAggregatedStats = expectedStreams),
          attempts =
            listOf(
              toAttemptRead(secondJobAttempt).copy(totalStats = FIRST_ATTEMPT_STATS_API, streamStats = FIRST_ATTEMPT_STREAM_STATS),
            ),
        )

      val latestJobWithAttemptRead =
        JobWithAttemptsRead(
          job =
            toJobInfo(latestJob).copy(
              aggregatedStats =
                JobAggregatedStats(
                  recordsEmitted = 0L,
                  bytesEmitted = 0L,
                  recordsCommitted = 0L,
                  bytesCommitted = 0L,
                  recordsRejected = 0L,
                ),
              streamAggregatedStats = emptyList(),
            ),
          attempts = emptyList(),
        )

      val expectedJobReadList =
        JobReadList(
          jobs = listOf(latestJobWithAttemptRead, secondJobWithAttemptRead, firstJobWithAttemptRead),
          totalJobCount = 3L,
        )

      Assertions.assertEquals(expectedJobReadList, jobReadList)
    }

    @Test
    @DisplayName("Should return jobs including specified job id")
    fun testListJobsIncludingJobId() {
      whenever(
        featureFlagClient.boolVariation(HydrateAggregatedStats, Workspace(ANONYMOUS)),
      ).thenReturn(true)

      val successfulJob =
        Job(
          JOB_ID,
          JOB_CONFIG.configType,
          JOB_CONFIG_ID,
          JOB_CONFIG,
          listOf(testJobAttempt),
          JOB_STATUS,
          null,
          CREATED_AT,
          CREATED_AT,
          true,
        )

      val pageSize = 25
      val rowOffset = 0

      val jobId2 = JOB_ID + 100
      val createdAt2 = CREATED_AT + 1000
      val latestJobNoAttempt =
        Job(
          jobId2,
          JOB_CONFIG.configType,
          JOB_CONFIG_ID,
          JOB_CONFIG,
          emptyList(),
          JobStatus.PENDING,
          null,
          createdAt2,
          createdAt2,
          true,
        )

      whenever(
        jobPersistence.listJobsIncludingId(
          setOf(CONFIG_TYPE_FOR_API.convertTo<ConfigType>()),
          JOB_CONFIG_ID,
          jobId2,
          pageSize,
        ),
      ).thenReturn(listOf(latestJobNoAttempt, successfulJob))

      whenever(
        jobPersistence.getJobCount(
          eq(setOf(CONFIG_TYPE_FOR_API.convertTo<ConfigType>())),
          eq(JOB_CONFIG_ID),
          anyOrNull(),
          anyOrNull(),
          anyOrNull(),
          anyOrNull(),
          anyOrNull(),
        ),
      ).thenReturn(2L)

      whenever(
        jobPersistence.getAttemptStats(listOf(200L, 100L)),
      ).thenReturn(
        mapOf(
          JobAttemptPair(100, 0) to FIRST_ATTEMPT_STATS,
          JobAttemptPair(jobId2, 0) to FIRST_ATTEMPT_STATS,
        ),
      )

      val requestBody =
        JobListRequestBody(
          configTypes = listOf(CONFIG_TYPE_FOR_API),
          configId = JOB_CONFIG_ID,
          includingJobId = jobId2,
          pagination = Pagination(pageSize = pageSize, rowOffset = rowOffset),
        )

      val jobReadList = jobHistoryHandler.listJobsFor(requestBody)

      val expectedStats =
        JobAggregatedStats(
          recordsEmitted = 55L,
          bytesEmitted = 22L,
          recordsCommitted = 55L,
          bytesCommitted = 22L,
          recordsRejected = 3L,
        )

      val expectedStreams =
        listOf(
          StreamStats(
            streamName = "stream1",
            streamNamespace = "ns1",
            recordsEmitted = 5L,
            bytesEmitted = 2L,
            recordsCommitted = 5L,
            bytesCommitted = 2L,
            recordsRejected = 1L,
          ),
          StreamStats(
            streamName = "stream2",
            recordsEmitted = 50L,
            bytesEmitted = 20L,
            recordsCommitted = 50L,
            bytesCommitted = 20L,
            recordsRejected = 2L,
          ),
        )

      val successfulJobWithAttemptRead =
        JobWithAttemptsRead(
          job = toJobInfo(successfulJob).copy(aggregatedStats = expectedStats, streamAggregatedStats = expectedStreams),
          attempts =
            listOf(
              toAttemptRead(testJobAttempt).copy(totalStats = FIRST_ATTEMPT_STATS_API, streamStats = FIRST_ATTEMPT_STREAM_STATS),
            ),
        )

      val latestJobWithAttemptRead =
        JobWithAttemptsRead(
          job =
            toJobInfo(latestJobNoAttempt).copy(
              aggregatedStats =
                JobAggregatedStats(
                  recordsEmitted = 0L,
                  bytesEmitted = 0L,
                  recordsCommitted = 0L,
                  bytesCommitted = 0L,
                  recordsRejected = 0L,
                ),
              streamAggregatedStats = emptyList(),
            ),
          attempts = emptyList(),
        )

      val expectedJobReadList =
        JobReadList(
          jobs = listOf(latestJobWithAttemptRead, successfulJobWithAttemptRead),
          totalJobCount = 2L,
        )

      Assertions.assertEquals(expectedJobReadList, jobReadList)
    }
  }

  @Test
  @DisplayName("Should return the right job info")
  fun testGetJobInfo() {
    val job =
      Job(
        JOB_ID,
        JOB_CONFIG.configType,
        JOB_CONFIG_ID,
        JOB_CONFIG,
        listOf(testJobAttempt),
        JOB_STATUS,
        null,
        CREATED_AT,
        CREATED_AT,
        true,
      )

    whenever(jobPersistence.getJob(JOB_ID)).thenReturn(job)
    whenever(logClientManager.getLogs(anyOrNull())).thenReturn(LogEvents(emptyList(), "1"))

    val jobInfoActual = jobHistoryHandler.getJobInfo(JOB_ID)

    val expected =
      JobInfoRead(job = toJobInfo(job), attempts = toAttemptInfoList(listOf(testJobAttempt)))

    Assertions.assertEquals(expected, jobInfoActual)
  }

  @Test
  @DisplayName("Should return the right job info without attempt information")
  fun testGetJobInfoLight() {
    val job =
      Job(
        JOB_ID,
        JOB_CONFIG.configType,
        JOB_CONFIG_ID,
        JOB_CONFIG,
        listOf(testJobAttempt),
        JOB_STATUS,
        null,
        CREATED_AT,
        CREATED_AT,
        true,
      )
    Mockito.`when`(jobPersistence.getJob(JOB_ID)).thenReturn(job)

    val requestBody = JobIdRequestBody(id = JOB_ID)
    val jobInfoLightActual = jobHistoryHandler.getJobInfoLight(requestBody)

    val exp = JobInfoLightRead(job = toJobInfo(job))

    Assertions.assertEquals(exp, jobInfoLightActual)
  }

  @Test
  @DisplayName("Should return the right info to debug this job")
  fun testGetDebugJobInfo() {
    val job =
      Job(
        JOB_ID,
        JOB_CONFIG.configType,
        JOB_CONFIG_ID,
        JOB_CONFIG,
        listOf(testJobAttempt),
        JOB_STATUS,
        null,
        CREATED_AT,
        CREATED_AT,
        true,
      )

    val standardSourceDefinition =
      StandardSourceDefinition()
        .withSourceDefinitionId(UUID.randomUUID())
        .withName("marketo")
    val source = SourceHelpers.generateSource(UUID.randomUUID())
    val sourceRead = SourceHelpers.getSourceRead(source, standardSourceDefinition)

    val standardDestinationDefinition =
      StandardDestinationDefinition()
        .withDestinationDefinitionId(UUID.randomUUID())
        .withName("db2")
    val destination = DestinationHelpers.generateDestination(UUID.randomUUID())
    val destinationRead = DestinationHelpers.getDestinationRead(destination, standardDestinationDefinition)

    val standardSync = ConnectionHelpers.generateSyncWithSourceId(source.sourceId)
    val connectionRead = ConnectionHelpers.generateExpectedConnectionRead(standardSync)

    whenever(connectionService.getStandardSync(UUID.fromString(job.scope))).thenReturn(standardSync)
    whenever(sourceHandler.getSource(SourceIdRequestBody(sourceId = connectionRead.sourceId))).thenReturn(sourceRead)
    whenever(destinationHandler.getDestination(DestinationIdRequestBody(destinationId = connectionRead.destinationId))).thenReturn(destinationRead)
    whenever(jobPersistence.getJob(JOB_ID)).thenReturn(job)
    whenever(jobPersistence.getAttemptStats(anyOrNull(), anyOrNull())).thenReturn(FIRST_ATTEMPT_STATS)
    whenever(logClientManager.getLogs(anyOrNull())).thenReturn(LogEvents(emptyList(), "1"))
    whenever(sourceDefinitionsHandler.getSourceDefinition(sourceRead.sourceDefinitionId, true)).thenReturn(SOURCE_DEFINITION_API)
    whenever(destinationDefinitionsHandler.getDestinationDefinition(destinationRead.destinationDefinitionId, true))
      .thenReturn(DESTINATION_DEFINITION_API)
    whenever(airbyteVersion.serialize()).thenReturn(AIRBYTE_VERSION)

    val jobDebugInfoActual = jobHistoryHandler.getJobDebugInfo(JOB_ID)

    val attemptInfoReads =
      toAttemptInfoList(listOf(testJobAttempt)).map {
        it.copy(attempt = it.attempt.copy(totalStats = FIRST_ATTEMPT_STATS_API, streamStats = FIRST_ATTEMPT_STREAM_STATS))
      }

    val expected =
      JobDebugInfoRead(job = toDebugJobInfo(job), attempts = attemptInfoReads)

    Assertions.assertEquals(expected, jobDebugInfoActual)
  }

  @Test
  @DisplayName("Should return the latest running sync job")
  fun testGetLatestRunningSyncJob() {
    val connectionId = UUID.randomUUID()

    val olderRunningJobId: Long = JOB_ID + 100
    val olderRunningCreatedAt: Long = CREATED_AT + 1000
    val olderRunningJobAttempt: Attempt = createAttempt(0, olderRunningJobId, olderRunningCreatedAt, AttemptStatus.RUNNING)
    val olderRunningJob =
      Job(
        olderRunningJobId,
        ConfigType.SYNC,
        JOB_CONFIG_ID,
        JOB_CONFIG,
        listOf(olderRunningJobAttempt),
        JobStatus.RUNNING,
        null,
        olderRunningCreatedAt,
        olderRunningCreatedAt,
        true,
      )

    // expect that we return the newer of the two running jobs. this should not happen in the real
    // world but might as
    // well test that we handle it properly.
    val newerRunningJobId: Long = JOB_ID + 200
    val newerRunningCreatedAt: Long = CREATED_AT + 2000
    val newerRunningJobAttempt: Attempt = createAttempt(0, newerRunningJobId, newerRunningCreatedAt, AttemptStatus.RUNNING)
    val newerRunningJob =
      Job(
        newerRunningJobId,
        ConfigType.SYNC,
        JOB_CONFIG_ID,
        JOB_CONFIG,
        listOf(newerRunningJobAttempt),
        JobStatus.RUNNING,
        null,
        newerRunningCreatedAt,
        newerRunningCreatedAt,
        true,
      )

    Mockito
      .`when`(
        jobPersistence.listJobsForConnectionWithStatuses(
          connectionId,
          Job.Companion.SYNC_REPLICATION_TYPES,
          JobStatus.NON_TERMINAL_STATUSES,
        ),
      ).thenReturn(listOf(newerRunningJob, olderRunningJob))

    val expectedJob: Optional<JobRead> = Optional.of<JobRead?>(getJobRead(newerRunningJob))
    val actualJob: Optional<JobRead> = jobHistoryHandler.getLatestRunningSyncJob(connectionId)

    Assertions.assertEquals(expectedJob, actualJob)
  }

  @Test
  @DisplayName("Should return an empty optional if no running sync job")
  fun testGetLatestRunningSyncJobWhenNone() {
    val connectionId = UUID.randomUUID()

    Mockito
      .`when`(
        jobPersistence.listJobsForConnectionWithStatuses(
          connectionId,
          Job.Companion.SYNC_REPLICATION_TYPES,
          JobStatus.NON_TERMINAL_STATUSES,
        ),
      ).thenReturn(mutableListOf())

    val actual: Optional<JobRead> = jobHistoryHandler.getLatestRunningSyncJob(connectionId)

    Assertions.assertTrue(actual.isEmpty())
  }

  @Nested
  @DisplayName("Sync progress")
  internal inner class ConnectionSyncProgressTests {
    @Test
    @DisplayName("Should not throw with no running sync")
    fun testGetConnectionSyncProgressNoJobs() {
      val connectionId = UUID.randomUUID()

      val request = ConnectionIdRequestBody(connectionId = connectionId)

      val expectedSyncProgress =
        ConnectionSyncProgressRead(
          connectionId = connectionId,
          streams = emptyList<StreamSyncProgressReadItem>(),
        )

      val actual = jobHistoryHandler.getConnectionSyncProgress(request)

      Assertions.assertEquals(expectedSyncProgress, actual)
    }

    @Test
    @DisplayName("Should return data for a running sync")
    fun testGetConnectionSyncProgressWithRunningJob() {
      val connectionId = UUID.randomUUID()
      val request = ConnectionIdRequestBody(connectionId = connectionId)

      val firstJob =
        Job(
          JOB_ID,
          JOB_CONFIG.configType,
          JOB_CONFIG_ID,
          JOB_CONFIG,
          listOf(testJobAttempt),
          JobStatus.RUNNING,
          CREATED_AT,
          CREATED_AT,
          CREATED_AT,
          true,
        )

      val jobAggregatedStats =
        JobAggregatedStats(
          bytesCommitted = 17L,
          recordsCommitted = 50L,
          bytesEmitted = 22L,
          recordsEmitted = 55L,
          recordsRejected = 2L,
        )
      val jobRead: JobRead =
        toJobInfo(firstJob).copy(
          enabledStreams =
            listOf(
              StreamDescriptor(name = "stream1", namespace = "ns1"),
              StreamDescriptor(name = "stream2"),
              StreamDescriptor(name = "stream3"),
            ),
          streamAggregatedStats =
            listOf(
              StreamStats(
                streamName = "stream2",
                recordsEmitted = 50L,
                bytesEmitted = 20L,
                recordsCommitted = 45L,
                bytesCommitted = 15L,
                recordsRejected = 2L,
              ),
              StreamStats(
                streamName = "stream1",
                streamNamespace = "ns1",
                recordsEmitted = 5L,
                bytesEmitted = 2L,
                recordsCommitted = 5L,
                bytesCommitted = 2L,
                recordsRejected = 0L,
              ),
            ),
          aggregatedStats = jobAggregatedStats,
        )

      val firstJobWithAttemptRead =
        JobWithAttemptsRead(job = jobRead, attempts = listOf(toAttemptRead(testJobAttempt)))

      Mockito.`when`(jobPersistence.getRunningJobForConnection(connectionId)).thenReturn(listOf(firstJob))

      val expected =
        ConnectionSyncProgressRead(
          jobId = JOB_ID,
          connectionId = connectionId,
          bytesCommitted = jobAggregatedStats.bytesCommitted,
          recordsCommitted = jobAggregatedStats.recordsCommitted,
          recordsRejected = jobAggregatedStats.recordsRejected,
          bytesEmitted = jobAggregatedStats.bytesEmitted,
          recordsEmitted = jobAggregatedStats.recordsEmitted,
          configType = io.airbyte.api.server.generated.models.JobConfigType.SYNC,
          syncStartedAt = CREATED_AT,
          streams =
            listOf<StreamSyncProgressReadItem>(
              StreamSyncProgressReadItem(
                streamName = "stream1",
                streamNamespace = "ns1",
                recordsEmitted = 5L,
                bytesEmitted = 2L,
                recordsCommitted = 5L,
                bytesCommitted = 2L,
                recordsRejected = 0L,
                configType = io.airbyte.api.server.generated.models.JobConfigType.SYNC,
              ),
              StreamSyncProgressReadItem(
                streamName = "stream2",
                recordsEmitted = 50L,
                bytesEmitted = 20L,
                recordsCommitted = 45L,
                bytesCommitted = 15L,
                recordsRejected = 2L,
                configType = io.airbyte.api.server.generated.models.JobConfigType.SYNC,
              ),
              StreamSyncProgressReadItem(
                streamName = "stream3",
                configType = io.airbyte.api.server.generated.models.JobConfigType.SYNC,
              ),
            ),
        )

      val actual =
        jobHistoryHandler.getConnectionSyncProgressInternal(
          request,
          listOf(firstJob),
          listOf(firstJobWithAttemptRead),
        )

      Assertions.assertEquals(expected, actual)
    }

    @Test
    @DisplayName("Should return data for a running refresh")
    fun testGetConnectionSyncProgressWithRefresh() {
      val connectionId = UUID.randomUUID()
      val request = ConnectionIdRequestBody(connectionId = connectionId)

      val jobConfig = clone(JOB_CONFIG).withConfigType(ConfigType.REFRESH)

      val firstJob =
        Job(
          JOB_ID,
          ConfigType.REFRESH,
          JOB_CONFIG_ID,
          jobConfig,
          listOf(testJobAttempt),
          JobStatus.RUNNING,
          CREATED_AT,
          CREATED_AT,
          CREATED_AT,
          true,
        )

      val jobAggregatedStats =
        JobAggregatedStats(
          recordsEmitted = 55L,
          bytesEmitted = 22L,
          recordsCommitted = 50L,
          bytesCommitted = 17L,
          recordsRejected = 0L,
        )
      val jobRead: JobRead =
        toJobInfo(firstJob).copy(
          enabledStreams =
            listOf(
              StreamDescriptor(name = "stream2"),
              StreamDescriptor(name = "stream3"),
            ),
          refreshConfig =
            JobRefreshConfig(
              streamsToRefresh = listOf(StreamDescriptor(name = "stream2")),
            ),
          streamAggregatedStats =
            listOf(
              StreamStats(
                streamName = "stream2",
                recordsEmitted = 50L,
                bytesEmitted = 20L,
                recordsCommitted = 45L,
                bytesCommitted = 15L,
                recordsRejected = 0L,
              ),
              StreamStats(
                streamName = "stream1",
                streamNamespace = "ns1",
                recordsEmitted = 5L,
                bytesEmitted = 2L,
                recordsCommitted = 5L,
                bytesCommitted = 2L,
                recordsRejected = 0L,
              ),
            ),
          aggregatedStats = jobAggregatedStats,
        )

      val firstJobWithAttemptRead =
        JobWithAttemptsRead(job = jobRead, attempts = listOf(toAttemptRead(testJobAttempt)))

      Mockito.`when`(jobPersistence.getRunningJobForConnection(connectionId)).thenReturn(listOf(firstJob))

      val expected =
        ConnectionSyncProgressRead(
          jobId = JOB_ID,
          connectionId = connectionId,
          bytesCommitted = jobAggregatedStats.bytesCommitted,
          recordsCommitted = jobAggregatedStats.recordsCommitted,
          bytesEmitted = jobAggregatedStats.bytesEmitted,
          recordsEmitted = jobAggregatedStats.recordsEmitted,
          configType = io.airbyte.api.server.generated.models.JobConfigType.REFRESH,
          syncStartedAt = CREATED_AT,
          streams =
            listOf<StreamSyncProgressReadItem>(
              StreamSyncProgressReadItem(
                streamName = "stream3",
                configType = io.airbyte.api.server.generated.models.JobConfigType.SYNC,
              ),
              StreamSyncProgressReadItem(
                streamName = "stream2",
                recordsEmitted = 50L,
                bytesEmitted = 20L,
                recordsCommitted = 45L,
                bytesCommitted = 15L,
                recordsRejected = 0L,
                configType = io.airbyte.api.server.generated.models.JobConfigType.REFRESH,
              ),
            ),
        )

      // test an internal version of the method to avoid having to mock JobConverter.
      val actual =
        jobHistoryHandler.getConnectionSyncProgressInternal(
          request,
          listOf(firstJob),
          listOf(firstJobWithAttemptRead),
        )

      Assertions.assertEquals(expected.streams, actual.streams)
    }

    // todo (cgardens) - get rid of mockstatic
    @Test
    @DisplayName("Should return data for a running clear")
    @Disabled
    fun testGetConnectionSyncProgressWithClear() {
      val connectionId = UUID.randomUUID()
      val request = ConnectionIdRequestBody(connectionId = connectionId)

      val firstJob =
        Job(
          JOB_ID,
          ConfigType.RESET_CONNECTION,
          JOB_CONFIG_ID,
          JOB_CONFIG,
          listOf(testJobAttempt),
          JobStatus.RUNNING,
          CREATED_AT,
          CREATED_AT,
          CREATED_AT,
          true,
        )

      val jobRead: JobRead =
        toJobInfo(firstJob).copy(
          resetConfig =
            ResetConfig(
              streamsToReset = listOf(StreamDescriptor(name = "stream1", namespace = "ns1")),
            ),
          enabledStreams =
            listOf(
              StreamDescriptor(name = "stream1", namespace = "ns1"),
              StreamDescriptor(name = "stream2"),
              StreamDescriptor(name = "stream3"),
            ),
        )

      val firstJobWithAttemptRead =
        JobWithAttemptsRead(job = jobRead, attempts = listOf(toAttemptRead(testJobAttempt)))

      Mockito.`when`(jobPersistence.getRunningJobForConnection(connectionId)).thenReturn(listOf(firstJob))
      Mockito.mockStatic(JobConverter::class.java).use { mockedConverter ->
        mockedConverter.`when`<Any?>(MockedStatic.Verification { getJobWithAttemptsRead(firstJob) }).thenReturn(firstJobWithAttemptRead)
        val expected =
          ConnectionSyncProgressRead(
            connectionId = connectionId,
            jobId = JOB_ID,
            configType = io.airbyte.api.server.generated.models.JobConfigType.RESET_CONNECTION,
            syncStartedAt = CREATED_AT,
            streams =
              listOf<StreamSyncProgressReadItem>(
                StreamSyncProgressReadItem(
                  streamName = "stream1",
                  streamNamespace = "ns1",
                  configType = io.airbyte.api.server.generated.models.JobConfigType.RESET_CONNECTION,
                ),
              ),
          )

        val actual = jobHistoryHandler.getConnectionSyncProgress(request)
        Assertions.assertEquals(expected, actual)
      }
    }
  }

  @Test
  @DisplayName("Should return the latest sync job")
  fun testGetLatestSyncJob() {
    val connectionId = UUID.randomUUID()

    // expect the newest job overall to be returned, even if it is failed
    val newerFailedJobId: Long = JOB_ID + 200
    val newerFailedCreatedAt: Long = CREATED_AT + 2000
    val newerFailedJobAttempt: Attempt = createAttempt(0, newerFailedJobId, newerFailedCreatedAt, AttemptStatus.FAILED)
    val newerFailedJob =
      Job(
        newerFailedJobId,
        ConfigType.SYNC,
        JOB_CONFIG_ID,
        JOB_CONFIG,
        listOf(newerFailedJobAttempt),
        JobStatus.RUNNING,
        null,
        newerFailedCreatedAt,
        newerFailedCreatedAt,
        true,
      )

    Mockito.`when`(jobPersistence.getLastSyncJob(connectionId)).thenReturn(Optional.of<Job>(newerFailedJob))

    val expectedJob: Optional<JobRead> = Optional.of<JobRead?>(getJobRead(newerFailedJob))
    val actualJob: Optional<JobRead> = jobHistoryHandler.getLatestSyncJob(connectionId)

    Assertions.assertEquals(expectedJob, actualJob)
  }

  @Test
  fun testEnumCompatibility() {
    Assertions.assertTrue(isCompatible<ConfigType, JobConfigType>())
    Assertions.assertTrue(isCompatible<JobStatus, io.airbyte.api.server.generated.models.JobStatus>())
    Assertions.assertTrue(isCompatible<AttemptStatus, io.airbyte.api.server.generated.models.AttemptStatus>())
  }

  @Test
  @DisplayName("Should test to ensure that JobInfoReadWithoutLogs includes the bytes and records committed")
  fun testGetJobInfoWithoutLogs() {
    Mockito
      .`when`(jobPersistence.getJob(JOB_ID))
      .thenReturn(
        Job(
          JOB_ID,
          JOB_CONFIG.configType,
          JOB_CONFIG_ID,
          JOB_CONFIG,
          listOf(testJobAttempt),
          JOB_STATUS,
          null,
          CREATED_AT,
          CREATED_AT,
          true,
        ),
      )
    Mockito
      .`when`(jobPersistence.getAttemptStats(listOf(JOB_ID)))
      .thenReturn(
        mapOf(
          JobAttemptPair(JOB_ID, testJobAttempt.attemptNumber) to FIRST_ATTEMPT_STATS,
        ),
      )

    val resultingJobInfo = jobHistoryHandler.getJobInfoWithoutLogs(JOB_ID)
    Assertions.assertEquals(
      resultingJobInfo.job.aggregatedStats!!.bytesCommitted,
      FIRST_ATTEMPT_STATS.combinedStats!!.bytesCommitted,
    )
    Assertions.assertEquals(
      resultingJobInfo.job.aggregatedStats!!.recordsCommitted,
      FIRST_ATTEMPT_STATS.combinedStats!!.recordsCommitted,
    )
    Assertions.assertEquals(
      resultingJobInfo.job.aggregatedStats!!.recordsRejected,
      FIRST_ATTEMPT_STATS.combinedStats!!.recordsRejected,
    )
  }

  companion object {
    private const val JOB_ID = 100L
    private const val JOB_CONFIG_ID = "ef296385-6796-413f-ac1b-49c4caba3f2b"
    private val JOB_STATUS = JobStatus.SUCCEEDED
    private val CONFIG_TYPE = ConfigType.SYNC
    private val CONFIG_TYPE_FOR_API = JobConfigType.CHECK_CONNECTION_SOURCE
    private val JOB_CONFIG: JobConfig =
      JobConfig()
        .withConfigType(CONFIG_TYPE)
        .withCheckConnection(JobCheckConnectionConfig())
        .withResetConnection(
          JobResetConnectionConfig().withResetSourceConfiguration(
            ResetSourceConfiguration()
              .withStreamsToReset(
                listOf<io.airbyte.config.StreamDescriptor?>(
                  io.airbyte.config
                    .StreamDescriptor()
                    .withName("stream1")
                    .withNamespace("ns1"),
                ),
              ),
          ),
        ).withRefresh(
          RefreshConfig()
            .withStreamsToRefresh(
              listOf<RefreshStream?>(
                RefreshStream().withStreamDescriptor(
                  io.airbyte.config
                    .StreamDescriptor()
                    .withName("stream2"),
                ),
              ),
            ).withConfiguredAirbyteCatalog(
              ConfiguredAirbyteCatalog().withStreams(
                listOf<ConfiguredAirbyteStream>(
                  ConfiguredAirbyteStream(
                    AirbyteStream("stream2", emptyObject(), listOf(SyncMode.INCREMENTAL)),
                    SyncMode.INCREMENTAL,
                    DestinationSyncMode.APPEND,
                  ),
                  ConfiguredAirbyteStream(
                    AirbyteStream("stream3", emptyObject(), listOf(SyncMode.INCREMENTAL)),
                    SyncMode.INCREMENTAL,
                    DestinationSyncMode.APPEND,
                  ),
                ),
              ),
            ),
        ).withSync(
          JobSyncConfig().withConfiguredAirbyteCatalog(
            ConfiguredAirbyteCatalog().withStreams(
              listOf<ConfiguredAirbyteStream>(
                ConfiguredAirbyteStream(
                  AirbyteStream("stream1", emptyObject(), listOf(SyncMode.FULL_REFRESH)).withNamespace("ns1"),
                  SyncMode.FULL_REFRESH,
                  DestinationSyncMode.APPEND,
                ),
                ConfiguredAirbyteStream(
                  AirbyteStream("stream2", emptyObject(), listOf(SyncMode.INCREMENTAL)),
                  SyncMode.INCREMENTAL,
                  DestinationSyncMode.APPEND,
                ),
              ),
            ),
          ),
        )
    private const val AIRBYTE_VERSION = "0.33.4"
    private val SOURCE_DEFINITION_ID = UUID.randomUUID()
    private val DESTINATION_DEFINITION_ID = UUID.randomUUID()

    private val SOURCE_DEFINITION_API =
      io.airbyte.api.server.generated.models.SourceDefinitionRead(
        sourceDefinitionId = SOURCE_DEFINITION_ID,
        name = "marketo",
        dockerRepository = "airbyte/source-marketo",
        dockerImageTag = "1.0.0",
        documentationUrl = URI.create("https://docs.airbyte.com/source"),
      )

    private val DESTINATION_DEFINITION_API =
      io.airbyte.api.server.generated.models.DestinationDefinitionRead(
        destinationDefinitionId = DESTINATION_DEFINITION_ID,
        name = "db2",
        dockerRepository = "airbyte/destination-db2",
        dockerImageTag = "1.0.0",
        documentationUrl = URI.create("https://docs.airbyte.com/destination"),
      )

    private val LOG_PATH: Path = Path.of("log_path")
    private val EMPTY_LOG_READ = AttemptLogs()
    private val CREATED_AT = System.currentTimeMillis() / 1000

    private val FIRST_ATTEMPT_STATS =
      JobPersistence.AttemptStats(
        SyncStats()
          .withRecordsEmitted(55L)
          .withBytesEmitted(22L)
          .withRecordsCommitted(55L)
          .withBytesCommitted(22L)
          .withRecordsRejected(3L),
        listOf(
          StreamSyncStats()
            .withStreamNamespace("ns1")
            .withStreamName("stream1")
            .withStats(
              SyncStats()
                .withRecordsEmitted(5L)
                .withBytesEmitted(2L)
                .withRecordsCommitted(5L)
                .withBytesCommitted(2L)
                .withRecordsRejected(1L),
            ),
          StreamSyncStats()
            .withStreamName("stream2")
            .withStats(
              SyncStats()
                .withRecordsEmitted(50L)
                .withBytesEmitted(20L)
                .withRecordsCommitted(50L)
                .withBytesCommitted(20L)
                .withRecordsRejected(2L),
            ),
        ),
      )

    private val SECOND_ATTEMPT_STATS =
      JobPersistence.AttemptStats(
        SyncStats()
          .withRecordsEmitted(5500L)
          .withBytesEmitted(2200L)
          .withRecordsCommitted(5500L)
          .withBytesCommitted(2200L)
          .withRecordsRejected(150L),
        listOf(
          StreamSyncStats()
            .withStreamNamespace("ns1")
            .withStreamName("stream1")
            .withStats(
              SyncStats()
                .withRecordsEmitted(500L)
                .withBytesEmitted(200L)
                .withRecordsCommitted(500L)
                .withBytesCommitted(200L)
                .withRecordsRejected(50L),
            ),
          StreamSyncStats()
            .withStreamName("stream2")
            .withStats(
              SyncStats()
                .withRecordsEmitted(5000L)
                .withBytesEmitted(2000L)
                .withRecordsCommitted(5000L)
                .withBytesCommitted(2000L)
                .withRecordsRejected(100L),
            ),
        ),
      )

    private val FIRST_ATTEMPT_STATS_API: AttemptStats? =
      AttemptStats(recordsEmitted = 55L, bytesEmitted = 22L, recordsCommitted = 55L, recordsRejected = 3L)

    private val FIRST_ATTEMPT_STREAM_STATS: List<AttemptStreamStats> =
      listOf(
        AttemptStreamStats(
          streamNamespace = "ns1",
          streamName = "stream1",
          stats = AttemptStats(recordsEmitted = 5L, bytesEmitted = 2L, recordsCommitted = 5L, recordsRejected = 1L),
        ),
        AttemptStreamStats(
          streamName = "stream2",
          stats = AttemptStats(recordsEmitted = 50L, bytesEmitted = 20L, recordsCommitted = 50L, recordsRejected = 2L),
        ),
      )

    private val SECOND_ATTEMPT_STATS_API: AttemptStats? =
      AttemptStats(
        recordsEmitted = 5500L,
        bytesEmitted = 2200L,
        recordsCommitted = 5500L,
        recordsRejected = 150L,
      )

    private val SECOND_ATTEMPT_STREAM_STATS: List<AttemptStreamStats> =
      listOf(
        AttemptStreamStats(
          streamNamespace = "ns1",
          streamName = "stream1",
          stats =
            AttemptStats(
              recordsEmitted = 500L,
              bytesEmitted = 200L,
              recordsCommitted = 500L,
              recordsRejected = 50L,
            ),
        ),
        AttemptStreamStats(
          streamName = "stream2",
          stats =
            AttemptStats(
              recordsEmitted = 5000L,
              bytesEmitted = 2000L,
              recordsCommitted = 5000L,
              recordsRejected = 100L,
            ),
        ),
      )

    private fun toJobInfo(job: Job): JobRead =
      JobRead(
        id = job.id,
        configId = job.scope,
        enabledStreams =
          job.config
            .getSync()
            .getConfiguredAirbyteCatalog()
            .streams
            .stream()
            .map { s: ConfiguredAirbyteStream? ->
              StreamDescriptor(name = s!!.stream.name, namespace = s.stream.namespace)
            }.collect(Collectors.toList()),
        status = job.status.convertTo<io.airbyte.api.server.generated.models.JobStatus>(),
        configType = job.configType.convertTo<JobConfigType>(),
        createdAt = job.createdAtInSecond,
        updatedAt = job.updatedAtInSecond,
      )

    private fun toDebugJobInfo(job: Job): JobDebugRead =
      JobDebugRead(
        id = job.id,
        configId = job.scope,
        status = job.status.convertTo<io.airbyte.api.server.generated.models.JobStatus>(),
        configType = job.configType.convertTo<JobConfigType>(),
        airbyteVersion = AIRBYTE_VERSION,
        sourceDefinition = SOURCE_DEFINITION_API,
        destinationDefinition = DESTINATION_DEFINITION_API,
      )

    private fun toAttemptInfoList(attempts: List<Attempt?>): MutableList<AttemptInfoRead> {
      val attemptReads = attempts.stream().map { a: Attempt? -> toAttemptRead(a!!) }.toList()

      val toAttemptInfoRead =
        Function { a: AttemptRead -> AttemptInfoRead(attempt = a, logType = LogFormatType.FORMATTED, logs = EMPTY_LOG_READ) }
      return attemptReads.stream().map(toAttemptInfoRead).collect(Collectors.toList())
    }

    private fun toAttemptRead(a: Attempt): AttemptRead =
      AttemptRead(
        id = a.attemptNumber.toLong(),
        status = a.status.convertTo<io.airbyte.api.server.generated.models.AttemptStatus>(),
        streamStats = null,
        createdAt = a.createdAtInSecond,
        updatedAt = a.updatedAtInSecond,
        endedAt = a.endedAtInSecond,
      )

    private fun createAttempt(
      attemptNumber: Int,
      jobId: Long,
      timestamps: Long,
      status: AttemptStatus,
    ): Attempt = Attempt(attemptNumber, jobId, LOG_PATH, null, null, status, null, null, timestamps, timestamps, timestamps)
  }
}
