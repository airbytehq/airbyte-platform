/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers

import io.airbyte.api.model.generated.CheckDataWorkerCapacityRead
import io.airbyte.api.model.generated.InternalOperationResult
import io.airbyte.api.model.generated.JobSuccessWithAttemptNumberRequest
import io.airbyte.commons.json.Jsons
import io.airbyte.commons.server.errors.BadRequestException
import io.airbyte.commons.server.handlers.helpers.ConnectionTimelineEventHelper
import io.airbyte.commons.server.handlers.helpers.JobCreationAndStatusUpdateHelper
import io.airbyte.config.AirbyteStream
import io.airbyte.config.Attempt
import io.airbyte.config.AttemptFailureSummary
import io.airbyte.config.AttemptStatus
import io.airbyte.config.ConfiguredAirbyteCatalog
import io.airbyte.config.ConfiguredAirbyteStream
import io.airbyte.config.DestinationSyncMode
import io.airbyte.config.FailureReason
import io.airbyte.config.Job
import io.airbyte.config.JobConfig
import io.airbyte.config.JobOutput
import io.airbyte.config.JobStatus
import io.airbyte.config.JobSyncConfig
import io.airbyte.config.RefreshConfig
import io.airbyte.config.ResourceRequirements
import io.airbyte.config.StandardSync
import io.airbyte.config.StandardSyncOutput
import io.airbyte.config.StandardSyncSummary
import io.airbyte.config.SyncMode
import io.airbyte.config.SyncResourceRequirements
import io.airbyte.config.SyncStats
import io.airbyte.data.services.ConnectionService
import io.airbyte.domain.models.OrganizationId
import io.airbyte.domain.services.dataworker.CapacityCheckResult
import io.airbyte.domain.services.dataworker.DataWorkerCapacityService
import io.airbyte.domain.services.dataworker.DataWorkerUsageService
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.featureflag.TestClient
import io.airbyte.metrics.MetricClient
import io.airbyte.persistence.job.JobNotifier
import io.airbyte.persistence.job.JobPersistence
import io.airbyte.persistence.job.errorreporter.JobErrorReporter
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import org.junit.jupiter.params.provider.ValueSource
import org.mockito.kotlin.any
import org.mockito.kotlin.anyOrNull
import org.mockito.kotlin.doAnswer
import org.mockito.kotlin.eq
import org.mockito.kotlin.mock
import org.mockito.kotlin.never
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import java.io.IOException
import java.nio.file.Path
import java.util.Optional
import java.util.UUID

class JobsHandlerTest {
  private lateinit var jobPersistence: JobPersistence
  private lateinit var jobNotifier: JobNotifier
  private lateinit var jobsHandler: JobsHandler
  private lateinit var helper: JobCreationAndStatusUpdateHelper
  private lateinit var jobErrorReporter: JobErrorReporter
  private lateinit var connectionTimelineEventHelper: ConnectionTimelineEventHelper
  private lateinit var featureFlagClient: FeatureFlagClient
  private lateinit var metricClient: MetricClient
  private lateinit var dataWorkerUsageService: DataWorkerUsageService
  private lateinit var connectionService: ConnectionService
  private lateinit var dataWorkerCapacityService: DataWorkerCapacityService

  private val jobId = 12L
  private val attemptNumber = 1
  private val connectionId = UUID.randomUUID()
  private val standardSyncOutput =
    StandardSyncOutput().withStandardSyncSummary(
      StandardSyncSummary().withStatus(StandardSyncSummary.ReplicationStatus.COMPLETED),
    )
  private val jobOutput = JobOutput().withSync(standardSyncOutput)
  private val catalog =
    ConfiguredAirbyteCatalog().withStreams(
      listOf(
        ConfiguredAirbyteStream(
          AirbyteStream("stream", Jsons.emptyObject(), listOf(SyncMode.FULL_REFRESH)),
          SyncMode.FULL_REFRESH,
          DestinationSyncMode.APPEND,
        ),
      ),
    )
  private val simpleConfig = JobConfig().withConfigType(JobConfig.ConfigType.SYNC).withSync(JobSyncConfig().withConfiguredAirbyteCatalog(catalog))
  private val failureSummary =
    AttemptFailureSummary().withFailures(
      listOf(
        FailureReason()
          .withFailureOrigin(FailureReason.FailureOrigin.SOURCE),
      ),
    )

  @BeforeEach
  fun setup() {
    jobPersistence = mock()
    jobNotifier = mock()
    helper = mock()
    jobErrorReporter = mock()
    connectionTimelineEventHelper = mock()
    featureFlagClient = mock<TestClient>()
    metricClient = mock()
    dataWorkerUsageService = mock()
    connectionService = mock()
    dataWorkerCapacityService = mock()

    jobsHandler =
      JobsHandler(
        jobPersistence,
        helper,
        jobNotifier,
        jobErrorReporter,
        connectionTimelineEventHelper,
        featureFlagClient,
        metricClient,
        dataWorkerUsageService,
        connectionService,
        dataWorkerCapacityService,
      )
  }

  @ParameterizedTest
  @ValueSource(booleans = [true, false])
  fun jobSuccessWithAttemptNumber(mergeStatsWithMetadata: Boolean) {
    val request =
      JobSuccessWithAttemptNumberRequest()
        .attemptNumber(attemptNumber)
        .jobId(jobId)
        .connectionId(connectionId)
        .standardSyncOutput(standardSyncOutput)

    val job =
      Job(
        jobId,
        JobConfig.ConfigType.SYNC,
        connectionId.toString(),
        simpleConfig,
        listOf(Attempt(attemptNumber, jobId, Path.of(""), null, null, AttemptStatus.SUCCEEDED, null, null, 1, 2, null)),
        JobStatus.SUCCEEDED,
        0L,
        0,
        0,
        true,
      )

    whenever(jobPersistence.getJob(jobId)).thenReturn(job)
    whenever(featureFlagClient.boolVariation(any(), any())).thenReturn(mergeStatsWithMetadata)

    val attemptStats = JobPersistence.AttemptStats(SyncStats().withBytesEmitted(1234L), listOf())
    whenever(
      if (mergeStatsWithMetadata) {
        jobPersistence.getAttemptStatsWithStreamMetadata(jobId, attemptNumber)
      } else {
        jobPersistence.getAttemptStats(jobId, attemptNumber)
      },
    ).thenReturn(attemptStats)

    jobsHandler.jobSuccessWithAttemptNumber(request)

    verify(jobPersistence).writeOutput(jobId, attemptNumber, jobOutput)
    verify(jobPersistence).succeedAttempt(jobId, attemptNumber)
    verify(jobNotifier).successJob(anyOrNull(), anyOrNull())
    verify(helper).trackCompletion(anyOrNull(), eq(io.airbyte.commons.server.JobStatus.SUCCEEDED))
    verify(connectionTimelineEventHelper).logJobSuccessEventInConnectionTimeline(job, connectionId, listOf(attemptStats))
    verify(dataWorkerUsageService).releaseReservedUsageForJob(eq(jobId))
  }

  @Test
  fun resetJobNoNotification() {
    val request =
      JobSuccessWithAttemptNumberRequest()
        .attemptNumber(attemptNumber)
        .jobId(jobId)
        .connectionId(UUID.randomUUID())
        .standardSyncOutput(standardSyncOutput)

    val job = Job(jobId, JobConfig.ConfigType.RESET_CONNECTION, "", simpleConfig, listOf(), JobStatus.SUCCEEDED, 0L, 0, 0, true)
    whenever(jobPersistence.getJob(jobId)).thenReturn(job)

    jobsHandler.jobSuccessWithAttemptNumber(request)

    verify(jobNotifier, never()).successJob(anyOrNull(), anyOrNull())
  }

  @Test
  fun setJobSuccessWrapException() {
    val exception = IOException("oops")
    doAnswer { throw exception }.whenever(jobPersistence).succeedAttempt(jobId, attemptNumber)

    assertThatThrownBy {
      jobsHandler.jobSuccessWithAttemptNumber(
        JobSuccessWithAttemptNumberRequest()
          .attemptNumber(attemptNumber)
          .jobId(jobId)
          .connectionId(connectionId)
          .standardSyncOutput(standardSyncOutput),
      )
    }.isInstanceOf(RuntimeException::class.java)
      .hasCauseInstanceOf(IOException::class.java)

    verify(helper).trackCompletionForInternalFailure(jobId, connectionId, attemptNumber, io.airbyte.commons.server.JobStatus.SUCCEEDED, exception)
  }

  @Test
  fun didPreviousJobSucceedReturnsFalseIfNoPreviousJob() {
    whenever(jobPersistence.listJobsIncludingId(anyOrNull(), anyOrNull(), anyOrNull(), anyOrNull())).thenReturn(listOf(mock(), mock(), mock()))
    whenever(helper.findPreviousJob(anyOrNull(), anyOrNull())).thenReturn(Optional.empty())
    whenever(helper.didJobSucceed(anyOrNull())).thenReturn(true)

    val result = jobsHandler.didPreviousJobSucceed(UUID.randomUUID(), 123)
    assertFalse(result.value)
  }

  @Test
  fun didPreviousJobSucceedReturnsTrueIfPreviousJobSucceeded() {
    whenever(jobPersistence.listJobsIncludingId(anyOrNull(), anyOrNull(), anyOrNull(), anyOrNull())).thenReturn(listOf(mock(), mock(), mock()))
    whenever(helper.findPreviousJob(anyOrNull(), anyOrNull())).thenReturn(Optional.of(mock()))
    whenever(helper.didJobSucceed(anyOrNull())).thenReturn(true)

    val result = jobsHandler.didPreviousJobSucceed(UUID.randomUUID(), 123)
    assertTrue(result.value)
  }

  @Test
  fun didPreviousJobSucceedReturnsFalseIfPreviousJobNotInSucceededState() {
    whenever(jobPersistence.listJobsIncludingId(anyOrNull(), anyOrNull(), anyOrNull(), anyOrNull())).thenReturn(listOf(mock(), mock(), mock()))
    whenever(helper.findPreviousJob(anyOrNull(), anyOrNull())).thenReturn(Optional.of(mock()))
    whenever(helper.didJobSucceed(anyOrNull())).thenReturn(false)

    val result = jobsHandler.didPreviousJobSucceed(UUID.randomUUID(), 123)
    assertFalse(result.value)
  }

  @Test
  fun checkDataWorkerCapacityUsesOnDemandWhenAvailable() {
    val organizationId = UUID.randomUUID()
    val job =
      Job(
        jobId,
        JobConfig.ConfigType.SYNC,
        connectionId.toString(),
        JobConfig().withConfigType(JobConfig.ConfigType.SYNC).withSync(
          JobSyncConfig()
            .withUsedOnDemandCapacity(false)
            .withSyncResourceRequirements(
              SyncResourceRequirements()
                .withSource(ResourceRequirements().withCpuRequest("2.0"))
                .withDestination(ResourceRequirements().withCpuRequest("3.0"))
                .withOrchestrator(ResourceRequirements().withCpuRequest("1.5")),
            ),
        ),
        listOf(),
        JobStatus.PENDING,
        null,
        0,
        0,
        false,
      )
    whenever(jobPersistence.getJob(jobId)).thenReturn(job)
    whenever(connectionService.getStandardSync(connectionId)).thenReturn(StandardSync().withOnDemandEnabled(true))
    whenever(
      dataWorkerCapacityService.checkCapacityAndReserve(
        OrganizationId(organizationId),
        job,
        0.8125,
        true,
      ),
    ).thenReturn(
      CapacityCheckResult(
        hasAvailableCapacity = true,
        currentDataWorkers = 5.0,
        committedDataWorkers = 5,
        requiredDataWorkers = 0.8125,
        usedOnDemandCapacity = true,
      ),
    )

    val result = jobsHandler.checkDataWorkerCapacity(jobId, connectionId, organizationId)

    assertEquals(CheckDataWorkerCapacityRead().capacityAvailable(true).useOnDemandCapacity(true), result)
    verify(dataWorkerCapacityService).checkCapacityAndReserve(
      OrganizationId(organizationId),
      job,
      0.8125,
      true,
    )
    verify(jobPersistence).updateSyncJobOnDemandCapacity(jobId, true)
  }

  @ParameterizedTest
  @MethodSource("nonSyncJobConfigs")
  fun checkDataWorkerCapacitySkipsNonSyncJobs(nonSyncJobConfig: JobConfig) {
    val organizationId = UUID.randomUUID()
    val nonSyncJob =
      Job(
        jobId,
        nonSyncJobConfig.configType,
        connectionId.toString(),
        nonSyncJobConfig,
        listOf(),
        JobStatus.PENDING,
        null,
        0,
        0,
        false,
      )
    whenever(jobPersistence.getJob(jobId)).thenReturn(nonSyncJob)

    val result = jobsHandler.checkDataWorkerCapacity(jobId, connectionId, organizationId)

    assertEquals(CheckDataWorkerCapacityRead().capacityAvailable(true).useOnDemandCapacity(false), result)
    verify(connectionService, never()).getStandardSync(any())
    verify(dataWorkerCapacityService, never()).checkCapacityAndReserve(any(), any(), any(), any())
    verify(jobPersistence, never()).updateSyncJobOnDemandCapacity(any(), any())
  }

  @Test
  fun persistJobCancellationSuccess() {
    val mockJob =
      Job(jobId, JobConfig.ConfigType.SYNC, connectionId.toString(), simpleConfig, listOf(), JobStatus.RUNNING, 0L, 0, 0, true)
    whenever(jobPersistence.getJob(jobId)).thenReturn(mockJob)

    jobsHandler.persistJobCancellation(connectionId, jobId, attemptNumber, failureSummary)

    verify(jobPersistence).failAttempt(jobId, attemptNumber)
    verify(jobPersistence).writeAttemptFailureSummary(jobId, attemptNumber, failureSummary)
    verify(jobPersistence).cancelJob(jobId)
    verify(helper).trackCompletion(anyOrNull(), eq(io.airbyte.commons.server.JobStatus.FAILED))
    verify(dataWorkerUsageService).releaseReservedUsageForJob(eq(jobId))
  }

  @Test
  fun persistJobCancellationIOException() {
    val exception = IOException("bang.")
    whenever(jobPersistence.getJob(jobId)).thenAnswer { throw exception }

    assertThrows(RuntimeException::class.java) {
      jobsHandler.persistJobCancellation(connectionId, jobId, attemptNumber, failureSummary)
    }
    verify(helper).trackCompletionForInternalFailure(jobId, connectionId, attemptNumber, io.airbyte.commons.server.JobStatus.FAILED, exception)
  }

  @ParameterizedTest
  @MethodSource("randomObjects")
  fun persistJobCancellationValidatesFailureSummary(thing: Any) {
    assertThrows(BadRequestException::class.java) {
      jobsHandler.persistJobCancellation(connectionId, jobId, attemptNumber, thing)
    }
  }

  @Test
  fun testReportJobStart() {
    val result = jobsHandler.reportJobStart(5L)
    assertEquals(InternalOperationResult().succeeded(true), result)
    verify(helper).reportJobStart(5L)
  }

  @Test
  fun testSetJobQueued() {
    val result = jobsHandler.setJobQueued(5L)

    assertEquals(InternalOperationResult().succeeded(true), result)
    verify(helper).setJobQueued(5L)
  }

  @Test
  fun testCancelQueuedJob() {
    val result = jobsHandler.cancelQueuedJob(5L)

    assertEquals(InternalOperationResult().succeeded(true), result)
    verify(helper).cancelQueuedJob(5L)
  }

  companion object {
    @JvmStatic
    fun randomObjects() =
      listOf(
        Arguments.of(123L),
        Arguments.of(true),
        Arguments.of(listOf("123", "123")),
        Arguments.of("a string"),
        Arguments.of(543.0),
      )

    @JvmStatic
    fun nonSyncJobConfigs() =
      listOf(
        Arguments.of(
          JobConfig()
            .withConfigType(JobConfig.ConfigType.REFRESH)
            .withRefresh(RefreshConfig()),
        ),
        Arguments.of(
          JobConfig()
            .withConfigType(JobConfig.ConfigType.RESET_CONNECTION),
        ),
        Arguments.of(
          JobConfig()
            .withConfigType(JobConfig.ConfigType.CLEAR),
        ),
      )
  }
}
