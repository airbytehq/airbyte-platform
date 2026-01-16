/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.handlers

import io.airbyte.api.model.generated.ConnectionIdRequestBody
import io.airbyte.api.model.generated.ConnectionSyncResultRead
import io.airbyte.api.model.generated.ConnectionUptimeHistoryRequestBody
import io.airbyte.api.model.generated.JobConfigType
import io.airbyte.api.model.generated.JobSyncResultRead
import io.airbyte.api.model.generated.StreamStatusCreateRequestBody
import io.airbyte.api.model.generated.StreamStatusListRequestBody
import io.airbyte.api.model.generated.StreamStatusRead
import io.airbyte.api.model.generated.StreamStatusReadList
import io.airbyte.api.model.generated.StreamStatusUpdateRequestBody
import io.airbyte.commons.json.Jsons
import io.airbyte.commons.server.handlers.JobHistoryHandler
import io.airbyte.config.AirbyteStream
import io.airbyte.config.Attempt
import io.airbyte.config.AttemptStatus
import io.airbyte.config.ConfiguredAirbyteCatalog
import io.airbyte.config.ConfiguredAirbyteStream
import io.airbyte.config.Job
import io.airbyte.config.JobConfig
import io.airbyte.config.JobStatus
import io.airbyte.config.JobSyncConfig
import io.airbyte.config.StreamSyncStats
import io.airbyte.config.SyncStats
import io.airbyte.db.instance.jobs.jooq.generated.enums.JobStreamStatusJobType
import io.airbyte.db.instance.jobs.jooq.generated.enums.JobStreamStatusRunState
import io.airbyte.persistence.job.JobPersistence
import io.airbyte.server.handlers.apidomainmapping.StreamStatusesMapper
import io.airbyte.server.repositories.StreamStatusesRepository
import io.airbyte.server.repositories.StreamStatusesRepository.FilterParams
import io.airbyte.server.repositories.domain.StreamStatus
import io.airbyte.server.repositories.domain.StreamStatus.StreamStatusBuilder
import io.micronaut.data.model.Page
import io.mockk.every
import io.mockk.mockk
import jakarta.validation.Valid
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.time.OffsetDateTime
import java.util.UUID

internal class StreamStatusesHandlerTest {
  private lateinit var repo: StreamStatusesRepository
  private lateinit var mapper: StreamStatusesMapper
  private lateinit var handler: StreamStatusesHandler
  private lateinit var jobPersistence: JobPersistence
  private lateinit var jobHistoryHandler: JobHistoryHandler

  @BeforeEach
  fun setup() {
    repo = mockk()
    mapper = mockk()
    jobHistoryHandler = mockk()
    jobPersistence = mockk()
    handler = StreamStatusesHandler(repo, mapper, jobHistoryHandler, jobPersistence)
  }

  @Test
  fun testCreate() {
    val apiReq = StreamStatusCreateRequestBody()
    val domain = StreamStatusBuilder().build()
    val apiResp = StreamStatusRead()

    every { mapper.map(apiReq) } returns domain
    every { repo.save(domain) } returns domain
    every { mapper.map(domain) } returns apiResp

    Assertions.assertSame(apiResp, handler.createStreamStatus(apiReq))
  }

  @Test
  fun testUpdate() {
    val apiReq = StreamStatusUpdateRequestBody()
    val domain = StreamStatusBuilder().build()
    val apiResp = StreamStatusRead()

    every { mapper.map(apiReq) } returns domain
    every { repo.update(domain) } returns domain
    every { mapper.map(domain) } returns apiResp

    Assertions.assertSame(apiResp, handler.updateStreamStatus(apiReq))
  }

  @Test
  fun testList() {
    val apiReq = StreamStatusListRequestBody()
    val domainFilters = FilterParams(null, null, null, null, null, null, null, null)
    val domainItem = StreamStatusBuilder().build()
    val apiItem = StreamStatusRead()
    val apiResp = StreamStatusReadList().streamStatuses(listOf<@Valid StreamStatusRead?>(apiItem))

    every { mapper.map(any<StreamStatusListRequestBody>()) } returns domainFilters

    @Suppress("UNCHECKED_CAST")
    val page: Page<StreamStatus> = mockk()
    every { page.content } returns listOf(domainItem)
    every { repo.findAllFiltered(domainFilters) } returns page
    every { mapper.map(domainItem) } returns apiItem

    Assertions.assertEquals(apiResp, handler.listStreamStatus(apiReq))
  }

  @Test
  fun testListPerRunState() {
    val connectionId = UUID.randomUUID()
    val apiReq = ConnectionIdRequestBody().connectionId(connectionId)
    val domainItem = StreamStatusBuilder().build()
    val apiItem = StreamStatusRead()
    val apiResp = StreamStatusReadList().streamStatuses(listOf<@Valid StreamStatusRead?>(apiItem))

    every { repo.findAllPerRunStateByConnectionId(connectionId) } returns listOf(domainItem)
    every { mapper.map(domainItem) } returns apiItem

    Assertions.assertEquals(apiResp, handler.listStreamStatusPerRunState(apiReq))
  }

  @Test
  fun testUptimeHistory() {
    val connectionId = UUID.randomUUID()
    val numJobs = 10
    val apiReq = ConnectionUptimeHistoryRequestBody().numberOfJobs(numJobs).connectionId(connectionId)
    val jobOneId = 1L
    val ssOne =
      StreamStatusBuilder()
        .id(UUID.randomUUID())
        .connectionId(connectionId)
        .attemptNumber(0)
        .streamName("streamOne")
        .streamNamespace("streamOneNamespace")
        .jobId(jobOneId)
        .jobType(JobStreamStatusJobType.sync)
        .transitionedAt(OffsetDateTime.now())
        .runState(JobStreamStatusRunState.complete)
        .build()
    val ssTwo =
      StreamStatusBuilder()
        .id(UUID.randomUUID())
        .connectionId(connectionId)
        .attemptNumber(0)
        .streamName("streamTwo")
        .streamNamespace("streamTwoNamespace")
        .jobId(jobOneId)
        .jobType(JobStreamStatusJobType.sync)
        .transitionedAt(OffsetDateTime.now())
        .runState(JobStreamStatusRunState.complete)
        .build()
    val jobTwoId = 2L
    val ssThree =
      StreamStatusBuilder()
        .id(UUID.randomUUID())
        .connectionId(connectionId)
        .attemptNumber(0)
        .streamName("streamThree")
        .streamNamespace("streamThreeNamespace")
        .jobId(jobTwoId)
        .jobType(JobStreamStatusJobType.sync)
        .transitionedAt(OffsetDateTime.now())
        .runState(JobStreamStatusRunState.complete)
        .build()

    val jobOneBytesCommitted = 12345L
    val jobOneBytesEmitted = 23456L
    val jobOneRecordsCommitted = 19L
    val jobOneRecordsEmitted = 20L
    val jobOneCreatedAt = 1000L
    val jobOneUpdatedAt = 2000L
    val jobTwoCreatedAt = 3000L
    val jobTwoUpdatedAt = 4000L
    val jobTwoBytesCommitted = 98765L
    val jobTwoBytesEmitted = 87654L
    val jobTwoRecordsCommitted = 50L
    val jobTwoRecordsEmittted = 60L
    val jobTwoRecordsRejected = 10L

    val jobOne =
      Job(
        id = jobOneId,
        configType = JobConfig.ConfigType.SYNC,
        scope = connectionId.toString(),
        config =
          JobConfig()
            .withConfigType(JobConfig.ConfigType.SYNC)
            .withSync(
              JobSyncConfig()
                .withConfiguredAirbyteCatalog(
                  ConfiguredAirbyteCatalog(
                    listOf(
                      ConfiguredAirbyteStream(
                        AirbyteStream(
                          "",
                          Jsons.emptyObject(),
                          emptyList(),
                          null,
                          null,
                          null,
                          null,
                          null,
                          null,
                        ),
                      ),
                    ),
                  ),
                ),
            ),
        attempts =
          listOf(
            Attempt(
              attemptNumber = 0,
              jobId = jobOneId,
              logPath = null,
              syncConfig = null,
              output = null,
              status = AttemptStatus.SUCCEEDED,
              processingTaskQueue = null,
              failureSummary = null,
              createdAtInSecond = jobOneCreatedAt,
              updatedAtInSecond = jobOneUpdatedAt,
              endedAtInSecond = null,
            ),
          ),
        status = JobStatus.SUCCEEDED,
        startedAtInSecond = 0L,
        createdAtInSecond = jobOneCreatedAt,
        updatedAtInSecond = jobOneUpdatedAt,
        isScheduled = true,
      )
    val jobTwo =
      Job(
        id = jobTwoId,
        configType = JobConfig.ConfigType.SYNC,
        scope = connectionId.toString(),
        config =
          JobConfig()
            .withConfigType(JobConfig.ConfigType.SYNC)
            .withSync(
              JobSyncConfig()
                .withConfiguredAirbyteCatalog(
                  ConfiguredAirbyteCatalog(
                    listOf(
                      ConfiguredAirbyteStream(
                        AirbyteStream(
                          "",
                          Jsons.emptyObject(),
                          emptyList(),
                          null,
                          null,
                          null,
                          null,
                          null,
                          null,
                        ),
                      ),
                    ),
                  ),
                ),
            ),
        attempts =
          listOf(
            Attempt(
              attemptNumber = 0,
              jobId = jobTwoId,
              logPath = null,
              syncConfig = null,
              output = null,
              status = AttemptStatus.SUCCEEDED,
              processingTaskQueue = null,
              failureSummary = null,
              createdAtInSecond = jobTwoCreatedAt,
              updatedAtInSecond = jobTwoUpdatedAt,
              endedAtInSecond = null,
            ),
          ),
        status = JobStatus.SUCCEEDED,
        startedAtInSecond = 0L,
        createdAtInSecond = jobTwoCreatedAt,
        updatedAtInSecond = jobTwoUpdatedAt,
        isScheduled = true,
      )

    val expected =
      listOf(
        JobSyncResultRead()
          .configType(JobConfigType.SYNC)
          .jobId(jobOneId)
          .bytesCommitted(jobOneBytesCommitted)
          .bytesEmitted(jobOneBytesEmitted)
          .recordsCommitted(jobOneRecordsCommitted)
          .recordsEmitted(jobOneRecordsEmitted)
          .recordsRejected(0)
          .jobCreatedAt(jobOneCreatedAt)
          .jobUpdatedAt(jobOneUpdatedAt)
          .streamStatuses(
            listOf<@Valid ConnectionSyncResultRead?>(
              ConnectionSyncResultRead()
                .status(io.airbyte.api.model.generated.JobStatus.SUCCEEDED)
                .streamName("streamOne")
                .streamNamespace("streamOneNamespace"),
              ConnectionSyncResultRead()
                .status(io.airbyte.api.model.generated.JobStatus.SUCCEEDED)
                .streamName("streamTwo")
                .streamNamespace("streamTwoNamespace"),
            ),
          ),
        JobSyncResultRead()
          .configType(JobConfigType.SYNC)
          .jobId(jobTwoId)
          .bytesCommitted(jobTwoBytesCommitted)
          .bytesEmitted(jobTwoBytesEmitted)
          .recordsCommitted(jobTwoRecordsCommitted)
          .recordsEmitted(jobTwoRecordsEmittted)
          .recordsRejected(jobTwoRecordsRejected)
          .jobCreatedAt(jobTwoCreatedAt)
          .jobUpdatedAt(jobTwoUpdatedAt)
          .streamStatuses(
            listOf<@Valid ConnectionSyncResultRead?>(
              ConnectionSyncResultRead()
                .status(io.airbyte.api.model.generated.JobStatus.SUCCEEDED)
                .streamName("streamThree")
                .streamNamespace("streamThreeNamespace"),
            ),
          ),
      )

    every { repo.findLastAttemptsOfLastXJobsForConnection(connectionId, numJobs) } returns listOf(ssOne, ssTwo, ssThree)
    every { jobPersistence.listJobsLight(setOf(jobOneId, jobTwoId)) } returns listOf(jobOne, jobTwo)
    every { jobPersistence.getAttemptStats(any()) } returns
      mapOf(
        JobPersistence.JobAttemptPair(jobOneId, 0) to
          JobPersistence.AttemptStats(
            SyncStats()
              .withBytesCommitted(jobOneBytesCommitted)
              .withBytesEmitted(jobOneBytesEmitted)
              .withRecordsCommitted(jobOneRecordsCommitted)
              .withRecordsEmitted(jobOneRecordsEmitted),
            listOf(
              StreamSyncStats()
                .withStreamName("")
                .withStats(
                  SyncStats()
                    .withBytesCommitted(jobOneBytesCommitted)
                    .withBytesEmitted(jobOneBytesEmitted)
                    .withRecordsCommitted(jobOneRecordsCommitted)
                    .withRecordsEmitted(jobOneRecordsEmitted),
                ),
            ),
          ),
        JobPersistence.JobAttemptPair(jobTwoId, 0) to
          JobPersistence.AttemptStats(
            SyncStats()
              .withBytesCommitted(jobTwoBytesCommitted)
              .withBytesEmitted(jobTwoBytesEmitted)
              .withRecordsCommitted(jobTwoRecordsCommitted)
              .withRecordsEmitted(jobTwoRecordsEmittted)
              .withRecordsRejected(jobTwoRecordsRejected),
            listOf(
              StreamSyncStats()
                .withStreamName("")
                .withStats(
                  SyncStats()
                    .withBytesCommitted(jobTwoBytesCommitted)
                    .withBytesEmitted(jobTwoBytesEmitted)
                    .withRecordsCommitted(jobTwoRecordsCommitted)
                    .withRecordsEmitted(jobTwoRecordsEmittted)
                    .withRecordsRejected(jobTwoRecordsRejected),
                ),
            ),
          ),
      )

    val handlerWithRealMapper =
      StreamStatusesHandler(
        repo,
        StreamStatusesMapper(),
        jobHistoryHandler,
        jobPersistence,
      )
    Assertions.assertEquals(expected, handlerWithRealMapper.getConnectionUptimeHistory(apiReq))
  }
}
