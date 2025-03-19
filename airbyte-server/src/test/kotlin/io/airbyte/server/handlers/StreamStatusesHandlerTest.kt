/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.handlers

import io.airbyte.api.model.generated.ConnectionIdRequestBody
import io.airbyte.api.model.generated.ConnectionSyncResultRead
import io.airbyte.api.model.generated.ConnectionUptimeHistoryRequestBody
import io.airbyte.api.model.generated.JobAggregatedStats
import io.airbyte.api.model.generated.JobConfigType
import io.airbyte.api.model.generated.JobRead
import io.airbyte.api.model.generated.JobSyncResultRead
import io.airbyte.api.model.generated.JobWithAttemptsRead
import io.airbyte.api.model.generated.StreamStatusCreateRequestBody
import io.airbyte.api.model.generated.StreamStatusListRequestBody
import io.airbyte.api.model.generated.StreamStatusRead
import io.airbyte.api.model.generated.StreamStatusReadList
import io.airbyte.api.model.generated.StreamStatusUpdateRequestBody
import io.airbyte.commons.server.handlers.JobHistoryHandler
import io.airbyte.commons.server.handlers.helpers.StatsAggregationHelper
import io.airbyte.config.Job
import io.airbyte.config.JobConfig
import io.airbyte.config.JobStatus
import io.airbyte.db.instance.jobs.jooq.generated.enums.JobStreamStatusJobType
import io.airbyte.db.instance.jobs.jooq.generated.enums.JobStreamStatusRunState
import io.airbyte.persistence.job.JobPersistence
import io.airbyte.server.handlers.apidomainmapping.StreamStatusesMapper
import io.airbyte.server.repositories.StreamStatusesRepository
import io.airbyte.server.repositories.StreamStatusesRepository.FilterParams
import io.airbyte.server.repositories.domain.StreamStatus
import io.airbyte.server.repositories.domain.StreamStatus.StreamStatusBuilder
import io.micronaut.data.model.Page
import jakarta.validation.Valid
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.Mockito
import java.io.IOException
import java.time.OffsetDateTime
import java.util.List
import java.util.Map
import java.util.Set
import java.util.UUID

internal class StreamStatusesHandlerTest {
  var repo: StreamStatusesRepository? = null

  var mapper: StreamStatusesMapper? = null

  var handler: StreamStatusesHandler? = null
  var jobPersistence: JobPersistence? = null
  var jobHistoryHandler: JobHistoryHandler? = null

  @BeforeEach
  fun setup() {
    repo = Mockito.mock(StreamStatusesRepository::class.java)
    mapper = Mockito.mock(StreamStatusesMapper::class.java)
    jobHistoryHandler = Mockito.mock(JobHistoryHandler::class.java)
    jobPersistence = Mockito.mock(JobPersistence::class.java)
    handler = StreamStatusesHandler(repo!!, mapper!!, jobHistoryHandler!!, jobPersistence!!)
  }

  @Test
  fun testCreate() {
    val apiReq = StreamStatusCreateRequestBody()
    val domain = StreamStatusBuilder().build()
    val apiResp = StreamStatusRead()

    Mockito
      .`when`(mapper!!.map(apiReq))
      .thenReturn(domain)
    Mockito
      .`when`(repo!!.save(domain))
      .thenReturn(domain)
    Mockito
      .`when`(mapper!!.map(domain))
      .thenReturn(apiResp)

    Assertions.assertSame(apiResp, handler!!.createStreamStatus(apiReq))
  }

  @Test
  fun testUpdate() {
    val apiReq = StreamStatusUpdateRequestBody()
    val domain = StreamStatusBuilder().build()
    val apiResp = StreamStatusRead()

    Mockito
      .`when`(mapper!!.map(apiReq))
      .thenReturn(domain)
    Mockito
      .`when`(repo!!.update(domain))
      .thenReturn(domain)
    Mockito
      .`when`(mapper!!.map(domain))
      .thenReturn(apiResp)

    Assertions.assertSame(apiResp, handler!!.updateStreamStatus(apiReq))
  }

  @Test
  fun testList() {
    val apiReq = StreamStatusListRequestBody()
    val domainFilters = FilterParams(null, null, null, null, null, null, null, null)
    val domainItem = StreamStatusBuilder().build()
    val apiItem = StreamStatusRead()
    val apiResp = StreamStatusReadList().streamStatuses(List.of<@Valid StreamStatusRead?>(apiItem))

    Mockito
      .`when`(mapper!!.map(org.mockito.kotlin.any<StreamStatusListRequestBody>()))
      .thenReturn(domainFilters)

    @Suppress("UNCHECKED_CAST")
    val page: Page<StreamStatus> = Mockito.mock(Page::class.java) as Page<StreamStatus>
    Mockito.`when`(page.content).thenReturn(List.of(domainItem))
    Mockito
      .`when`(repo!!.findAllFiltered(domainFilters))
      .thenReturn(page)
    Mockito
      .`when`(mapper!!.map(domainItem))
      .thenReturn(apiItem)

    Assertions.assertEquals(apiResp, handler!!.listStreamStatus(apiReq))
  }

  @Test
  fun testListPerRunState() {
    val connectionId = UUID.randomUUID()
    val apiReq = ConnectionIdRequestBody().connectionId(connectionId)
    val domainItem = StreamStatusBuilder().build()
    val apiItem = StreamStatusRead()
    val apiResp = StreamStatusReadList().streamStatuses(List.of<@Valid StreamStatusRead?>(apiItem))

    Mockito
      .`when`(repo!!.findAllPerRunStateByConnectionId(connectionId))
      .thenReturn(List.of(domainItem))
    Mockito
      .`when`(mapper!!.map(domainItem))
      .thenReturn(apiItem)

    Assertions.assertEquals(apiResp, handler!!.listStreamStatusPerRunState(apiReq))
  }

  @Test
  @Throws(IOException::class)
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

    val jobOne = Job(1, JobConfig.ConfigType.SYNC, connectionId.toString(), null, listOf(), JobStatus.SUCCEEDED, 0L, 0L, 0L, true)
    val jobTwo = Job(2, JobConfig.ConfigType.SYNC, connectionId.toString(), null, listOf(), JobStatus.SUCCEEDED, 0L, 0L, 0L, true)

    val jobOneBytesCommitted = 12345L
    val jobOneBytesEmitted = 23456L
    val jobOneRecordsCommitted = 19L
    val jobOneRecordsEmmitted = 20L
    val jobOneCreatedAt = 1000L
    val jobOneUpdatedAt = 2000L
    val jobTwoCreatedAt = 3000L
    val jobTwoUpdatedAt = 4000L
    val jobTwoBytesCommitted = 98765L
    val jobTwoBytesEmmitted = 87654L
    val jobTwoRecordsCommitted = 50L
    val jobTwoRecordsEmittted = 60L
    Mockito.mockStatic(StatsAggregationHelper::class.java).use { mockStatsAggregationHelper ->
      mockStatsAggregationHelper
        .`when`<Any> {
          StatsAggregationHelper.getJobIdToJobWithAttemptsReadMap(
            Mockito.any(),
            Mockito.any(),
          )
        }.thenReturn(
          Map.of(
            jobOneId,
            JobWithAttemptsRead().job(
              JobRead().createdAt(jobOneCreatedAt).updatedAt(jobOneUpdatedAt).configType(JobConfigType.SYNC).aggregatedStats(
                JobAggregatedStats()
                  .bytesCommitted(jobOneBytesCommitted)
                  .bytesEmitted(jobOneBytesEmitted)
                  .recordsCommitted(jobOneRecordsCommitted)
                  .recordsEmitted(jobOneRecordsEmmitted),
              ),
            ),
            jobTwoId,
            JobWithAttemptsRead().job(
              JobRead().createdAt(jobTwoCreatedAt).updatedAt(jobTwoUpdatedAt).configType(JobConfigType.SYNC).aggregatedStats(
                JobAggregatedStats()
                  .bytesCommitted(jobTwoBytesCommitted)
                  .bytesEmitted(jobTwoBytesEmmitted)
                  .recordsCommitted(jobTwoRecordsCommitted)
                  .recordsEmitted(jobTwoRecordsEmittted),
              ),
            ),
          ),
        )
      val expected =
        List.of(
          JobSyncResultRead()
            .configType(JobConfigType.SYNC)
            .jobId(jobOneId)
            .bytesCommitted(jobOneBytesCommitted)
            .bytesEmitted(jobOneBytesEmitted)
            .recordsCommitted(jobOneRecordsCommitted)
            .recordsEmitted(jobOneRecordsEmmitted)
            .jobCreatedAt(jobOneCreatedAt)
            .jobUpdatedAt(jobOneUpdatedAt)
            .streamStatuses(
              List.of<@Valid ConnectionSyncResultRead?>(
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
            .bytesEmitted(jobTwoBytesEmmitted)
            .recordsCommitted(jobTwoRecordsCommitted)
            .recordsEmitted(jobTwoRecordsEmittted)
            .jobCreatedAt(jobTwoCreatedAt)
            .jobUpdatedAt(jobTwoUpdatedAt)
            .streamStatuses(
              List.of<@Valid ConnectionSyncResultRead?>(
                ConnectionSyncResultRead()
                  .status(io.airbyte.api.model.generated.JobStatus.SUCCEEDED)
                  .streamName("streamThree")
                  .streamNamespace("streamThreeNamespace"),
              ),
            ),
        )

      Mockito.`when`(repo!!.findLastAttemptsOfLastXJobsForConnection(connectionId, numJobs)).thenReturn(List.of(ssOne, ssTwo, ssThree))
      Mockito.`when`(jobPersistence!!.listJobsLight(Set.of(jobOneId, jobTwoId))).thenReturn(List.of(jobOne, jobTwo))
      val handlerWithRealMapper =
        StreamStatusesHandler(
          repo!!,
          StreamStatusesMapper(),
          jobHistoryHandler!!,
          jobPersistence!!,
        )
      Assertions.assertEquals(expected, handlerWithRealMapper.getConnectionUptimeHistory(apiReq))
    }
  }
}
