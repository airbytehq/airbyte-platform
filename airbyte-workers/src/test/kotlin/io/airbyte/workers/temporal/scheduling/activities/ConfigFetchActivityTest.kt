/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.scheduling.activities

import io.airbyte.api.client.AirbyteApiClient
import io.airbyte.api.client.generated.ConnectionApi
import io.airbyte.api.client.generated.JobsApi
import io.airbyte.api.client.generated.WorkspaceApi
import io.airbyte.api.client.model.generated.AirbyteCatalog
import io.airbyte.api.client.model.generated.AirbyteStreamAndConfiguration
import io.airbyte.api.client.model.generated.ConnectionContextRead
import io.airbyte.api.client.model.generated.ConnectionIdRequestBody
import io.airbyte.api.client.model.generated.ConnectionRead
import io.airbyte.api.client.model.generated.ConnectionSchedule
import io.airbyte.api.client.model.generated.ConnectionScheduleData
import io.airbyte.api.client.model.generated.ConnectionScheduleDataBasicSchedule
import io.airbyte.api.client.model.generated.ConnectionScheduleDataCron
import io.airbyte.api.client.model.generated.ConnectionScheduleType
import io.airbyte.api.client.model.generated.ConnectionStatus
import io.airbyte.api.client.model.generated.JobOptionalRead
import io.airbyte.api.client.model.generated.JobRead
import io.airbyte.api.client.model.generated.WorkspaceRead
import io.airbyte.commons.converters.toInternal
import io.airbyte.commons.temporal.exception.RetryableException
import io.airbyte.config.ConnectionContext
import io.airbyte.featureflag.Connection
import io.airbyte.featureflag.Context
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.featureflag.LoadShedSchedulerBackoffMinutes
import io.airbyte.featureflag.Multi
import io.airbyte.featureflag.TestClient
import io.airbyte.featureflag.UseNewCronScheduleCalculation
import io.airbyte.featureflag.Workspace
import io.airbyte.workers.helpers.ScheduleJitterHelper
import io.airbyte.workers.input.InputFeatureFlagContextMapper
import io.airbyte.workers.temporal.activities.GetConnectionContextInput
import io.airbyte.workers.temporal.activities.GetConnectionContextOutput
import io.airbyte.workers.temporal.activities.GetLoadShedBackoffInput
import io.airbyte.workers.temporal.scheduling.activities.ConfigFetchActivity.ScheduleRetrieverInput
import org.assertj.core.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.junit.jupiter.api.function.Executable
import org.mockito.Mock
import org.mockito.junit.jupiter.MockitoExtension
import org.mockito.junit.jupiter.MockitoSettings
import org.mockito.kotlin.any
import org.mockito.kotlin.eq
import org.mockito.kotlin.whenever
import org.mockito.quality.Strictness
import java.io.IOException
import java.time.Duration
import java.time.Instant
import java.util.Calendar
import java.util.List
import java.util.TimeZone
import java.util.UUID
import java.util.function.Supplier

@MockitoSettings(strictness = Strictness.LENIENT)
@ExtendWith(MockitoExtension::class)
internal class ConfigFetchActivityTest {
  @Mock
  private val mAirbyteApiClient: AirbyteApiClient? = null

  @Mock
  private val mJobsApi: JobsApi? = null

  @Mock
  private val mWorkspaceApi: WorkspaceApi? = null

  @Mock
  private val mJobRead: JobRead? = null

  @Mock
  private val mConnectionApi: ConnectionApi? = null

  @Mock
  private val mScheduleJitterHelper: ScheduleJitterHelper? = null

  @Mock
  private val mFfContextMapper: InputFeatureFlagContextMapper? = null

  private var mFeatureFlagClient: FeatureFlagClient? = null

  private var configFetchActivity: ConfigFetchActivityImpl? = null

  private val currentSecondsSupplier: Supplier<Long> = Supplier { Instant.now().getEpochSecond() }

  @BeforeEach
  fun setup() {
    mFeatureFlagClient = org.mockito.Mockito.mock<TestClient>(TestClient::class.java)
  }

  @Nested
  internal inner class TimeToWaitTest {
    @BeforeEach
    @Throws(IOException::class)
    fun setup() {
      whenever(mWorkspaceApi!!.getWorkspaceByConnectionId(any<ConnectionIdRequestBody>())).thenReturn(
        WorkspaceRead(
          UUID.randomUUID(),
          UUID.randomUUID(),
          "name",
          "slug",
          false,
          UUID.randomUUID(),
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
        ),
      )
    }

    @Nested
    internal inner class TestNotCron {
      @BeforeEach
      fun setup() {
        whenever(mAirbyteApiClient!!.connectionApi).thenReturn(mConnectionApi)
        whenever(mAirbyteApiClient.workspaceApi).thenReturn(mWorkspaceApi)
        configFetchActivity =
          ConfigFetchActivityImpl(
            mAirbyteApiClient,
            SYNC_JOB_MAX_ATTEMPTS,
            currentSecondsSupplier,
            mFeatureFlagClient!!,
            mScheduleJitterHelper!!,
            mFfContextMapper!!,
          )
      }

      @Test
      @DisplayName("Test that the job will wait for a long time if it is disabled")
      @Throws(IOException::class)
      fun testDisable() {
        whenever(mConnectionApi!!.getConnection(any<ConnectionIdRequestBody>()))
          .thenReturn(connectionReadWithScheduleDisable)

        val input = ScheduleRetrieverInput(CONNECTION_ID)

        val output = configFetchActivity!!.getTimeToWait(input)

        Assertions
          .assertThat(output.timeToWait)
          .hasDays((100 * 365).toLong())
      }

      @Test
      @DisplayName("Test that the connection will wait for a long time if it is deleted")
      @Throws(IOException::class)
      fun testDeleted() {
        whenever(mConnectionApi!!.getConnection(any<ConnectionIdRequestBody>()))
          .thenReturn(connectionReadWithScheduleDeleted)

        val input = ScheduleRetrieverInput(CONNECTION_ID)

        val output = configFetchActivity!!.getTimeToWait(input)

        Assertions
          .assertThat(output.timeToWait)
          .hasDays((100 * 365).toLong())
      }

      @Test
      @DisplayName("Test that the job will wait a long time if it is MANUAL scheduleType")
      @Throws(IOException::class)
      fun testManualScheduleType() {
        whenever(mConnectionApi!!.getConnection(any<ConnectionIdRequestBody>()))
          .thenReturn(connectionReadWithManualScheduleType)

        val input = ScheduleRetrieverInput(CONNECTION_ID)

        val output = configFetchActivity!!.getTimeToWait(input)

        Assertions
          .assertThat(output.timeToWait)
          .hasDays((100 * 365).toLong())
      }

      @Test
      @DisplayName("Test that the job will be immediately scheduled if it is a BASIC_SCHEDULE type on the first run")
      @Throws(IOException::class)
      fun testBasicScheduleTypeFirstRun() {
        whenever(mAirbyteApiClient!!.jobsApi).thenReturn(mJobsApi)
        whenever(mJobsApi!!.getLastReplicationJobWithCancel(any<ConnectionIdRequestBody>()))
          .thenReturn(JobOptionalRead())

        whenever(mConnectionApi!!.getConnection(any<ConnectionIdRequestBody>()))
          .thenReturn(connectionReadWithBasicScheduleType)

        val input = ScheduleRetrieverInput(CONNECTION_ID)

        val output = configFetchActivity!!.getTimeToWait(input)

        Assertions
          .assertThat(output.timeToWait)
          .isZero()
      }

      @Test
      @DisplayName("Test that we will wait the required amount of time with a BASIC_SCHEDULE type on a subsequent run")
      @Throws(
        IOException::class,
      )
      fun testBasicScheduleSubsequentRun() {
        whenever(mAirbyteApiClient!!.jobsApi).thenReturn(mJobsApi)
        configFetchActivity =
          ConfigFetchActivityImpl(
            mAirbyteApiClient,
            SYNC_JOB_MAX_ATTEMPTS,
            Supplier { 60L * 3 },
            mFeatureFlagClient!!,
            mScheduleJitterHelper!!,
            mFfContextMapper!!,
          )

        whenever(mJobRead!!.createdAt)
          .thenReturn(60L)

        whenever(mJobsApi!!.getLastReplicationJobWithCancel(any<ConnectionIdRequestBody>()))
          .thenReturn(JobOptionalRead(mJobRead))

        whenever(mConnectionApi!!.getConnection(any<ConnectionIdRequestBody>()))
          .thenReturn(connectionReadWithBasicScheduleType)

        val input = ScheduleRetrieverInput(CONNECTION_ID)

        val output = configFetchActivity!!.getTimeToWait(input)

        Assertions
          .assertThat(output.timeToWait)
          .hasMinutes(3)
      }
    }

    @Nested
    internal inner class TestCronSchedule {
      @BeforeEach
      fun setup() {
        whenever(mAirbyteApiClient!!.connectionApi).thenReturn(mConnectionApi)
        whenever(mAirbyteApiClient.jobsApi).thenReturn(mJobsApi)
        whenever(mAirbyteApiClient.workspaceApi).thenReturn(mWorkspaceApi)
      }

      @Test
      @DisplayName("Test that the job will wait to be scheduled if it is a CRON type, and the prior job ran recently")
      @Throws(IOException::class)
      fun testCronScheduleSubsequentRunPriorJobRanRecently() {
        val mockRightNow: Calendar = Calendar.getInstance(TimeZone.getTimeZone(UTC))
        mockRightNow.set(Calendar.HOUR_OF_DAY, 0)
        mockRightNow.set(Calendar.MINUTE, 0)
        mockRightNow.set(Calendar.SECOND, 0)
        mockRightNow.set(Calendar.MILLISECOND, 0)
        val currentSecondsSupplier: Supplier<Long> = Supplier { mockRightNow.getTimeInMillis() / 1000L }

        configFetchActivity =
          ConfigFetchActivityImpl(
            mAirbyteApiClient!!,
            SYNC_JOB_MAX_ATTEMPTS,
            currentSecondsSupplier,
            mFeatureFlagClient!!,
            mScheduleJitterHelper!!,
            mFfContextMapper!!,
          )

        whenever(mJobsApi!!.getLastReplicationJobWithCancel(any<ConnectionIdRequestBody>()))
          .thenReturn(JobOptionalRead(mJobRead))

        // prior job completed 3 hours ago, so expect the next job to be scheduled
        // according to the next cron run time.
        val threeHoursAgoSeconds = currentSecondsSupplier.get()!! - Duration.ofHours(3).toSeconds()
        whenever(mJobRead!!.startedAt).thenReturn(null)
        whenever(mJobRead.createdAt).thenReturn(threeHoursAgoSeconds)

        whenever(mConnectionApi!!.getConnection(any<ConnectionIdRequestBody>()))
          .thenReturn(connectionReadWithCronScheduleType)

        val input = ScheduleRetrieverInput(CONNECTION_ID)

        val output = configFetchActivity!!.getTimeToWait(input)

        Assertions
          .assertThat(output.timeToWait)
          .hasHours(12)
      }

      @Test
      @DisplayName("Test that the job will run immediately if it is CRON type, and the expected interval has elapsed since the prior job")
      @Throws(
        IOException::class,
      )
      fun testCronScheduleSubsequentRunPriorJobRanLongAgo() {
        val mockRightNow: Calendar = Calendar.getInstance(TimeZone.getTimeZone(UTC))
        mockRightNow.set(Calendar.HOUR_OF_DAY, 0)
        mockRightNow.set(Calendar.MINUTE, 0)
        mockRightNow.set(Calendar.SECOND, 0)
        mockRightNow.set(Calendar.MILLISECOND, 0)
        val currentSecondsSupplier: Supplier<Long> = Supplier { mockRightNow.getTimeInMillis() / 1000L }

        configFetchActivity =
          ConfigFetchActivityImpl(
            mAirbyteApiClient!!,
            SYNC_JOB_MAX_ATTEMPTS,
            currentSecondsSupplier,
            mFeatureFlagClient!!,
            mScheduleJitterHelper!!,
            mFfContextMapper!!,
          )

        whenever(mJobsApi!!.getLastReplicationJobWithCancel(any<ConnectionIdRequestBody>()))
          .thenReturn(JobOptionalRead(mJobRead))

        // Behavior is currently behind a feature flag
        whenever(
          mFeatureFlagClient!!.boolVariation(
            eq(UseNewCronScheduleCalculation),
            any<Context>(),
          ),
        ).thenReturn(true)

        // prior job completed over 24 hours ago, so expect the next job to be scheduled immediately
        val twentyFiveHoursAgoSeconds = currentSecondsSupplier.get()!! - Duration.ofHours(25).toSeconds()
        whenever(mJobRead!!.createdAt).thenReturn(twentyFiveHoursAgoSeconds)

        whenever(mConnectionApi!!.getConnection(any<ConnectionIdRequestBody>()))
          .thenReturn(connectionReadWithCronScheduleType)

        val input = ScheduleRetrieverInput(CONNECTION_ID)

        val output = configFetchActivity!!.getTimeToWait(input)

        Assertions.assertThat(output.timeToWait).isZero()
      }

      @Test
      @DisplayName("Test that the job will only be scheduled once per minimum cron interval")
      @Throws(IOException::class)
      fun testCronScheduleMinimumInterval() {
        val mockRightNow: Calendar = Calendar.getInstance(TimeZone.getTimeZone(UTC))
        mockRightNow.set(Calendar.HOUR_OF_DAY, 12)
        mockRightNow.set(Calendar.MINUTE, 0)
        mockRightNow.set(Calendar.SECOND, 0)
        mockRightNow.set(Calendar.MILLISECOND, 0)
        val currentSecondsSupplier: Supplier<Long> = Supplier { mockRightNow.getTimeInMillis() / 1000L }

        configFetchActivity =
          ConfigFetchActivityImpl(
            mAirbyteApiClient!!,
            SYNC_JOB_MAX_ATTEMPTS,
            currentSecondsSupplier,
            mFeatureFlagClient!!,
            mScheduleJitterHelper!!,
            mFfContextMapper!!,
          )

        whenever(mJobRead!!.startedAt).thenReturn(null)
        whenever(mJobRead.createdAt).thenReturn(mockRightNow.getTimeInMillis() / 1000L)
        whenever(mJobsApi!!.getLastReplicationJobWithCancel(any<ConnectionIdRequestBody>()))
          .thenReturn(JobOptionalRead(mJobRead))

        whenever(mConnectionApi!!.getConnection(any<ConnectionIdRequestBody>()))
          .thenReturn(connectionReadWithCronScheduleType)

        val input = ScheduleRetrieverInput(CONNECTION_ID)

        val output = configFetchActivity!!.getTimeToWait(input)

        Assertions
          .assertThat(output.timeToWait)
          .hasHours(24)
      }

      @Test
      @DisplayName("Test that for specific workspace ids, we add some noise in the cron scheduling")
      @Throws(IOException::class)
      fun testCronSchedulingNoise() {
        val mockRightNow: Calendar = Calendar.getInstance(TimeZone.getTimeZone(UTC))
        mockRightNow.set(Calendar.HOUR_OF_DAY, 0)
        mockRightNow.set(Calendar.MINUTE, 0)
        mockRightNow.set(Calendar.SECOND, 0)
        mockRightNow.set(Calendar.MILLISECOND, 0)
        val currentSecondsSupplier: Supplier<Long> = Supplier { mockRightNow.getTimeInMillis() / 1000L }

        whenever(mWorkspaceApi!!.getWorkspaceByConnectionId(any<ConnectionIdRequestBody>()))
          .thenReturn(
            WorkspaceRead(
              UUID.fromString("226edbc1-4a9c-4401-95a9-90435d667d9d"),
              UUID.randomUUID(),
              "name",
              "slug",
              false,
              UUID.randomUUID(),
              null,
              null,
              null,
              null,
              null,
              null,
              null,
              null,
              null,
              null,
              null,
              null,
              null,
            ),
          )

        configFetchActivity =
          ConfigFetchActivityImpl(
            mAirbyteApiClient!!,
            SYNC_JOB_MAX_ATTEMPTS,
            currentSecondsSupplier,
            mFeatureFlagClient!!,
            mScheduleJitterHelper!!,
            mFfContextMapper!!,
          )

        whenever(mJobRead!!.startedAt).thenReturn(null)
        whenever(mJobRead.createdAt).thenReturn(mockRightNow.getTimeInMillis() / 1000L)
        whenever(mJobsApi!!.getLastReplicationJobWithCancel(any<ConnectionIdRequestBody>()))
          .thenReturn(JobOptionalRead(mJobRead))

        whenever(mConnectionApi!!.getConnection(any<ConnectionIdRequestBody>()))
          .thenReturn(connectionReadWithCronScheduleType)

        val input = ScheduleRetrieverInput(CONNECTION_ID)

        val output = configFetchActivity!!.getTimeToWait(input)

        // Note: compareTo returns positive if the left side is greater than the right.
        Assertions.assertThat(output.timeToWait!!.compareTo(Duration.ofHours(12)) > 0).isTrue()
      }
    }
  }

  @Nested
  internal inner class TestGetMaxAttempt {
    @Test
    @DisplayName("Test that we are using to right service to get the maximum amount of attempt")
    fun testGetMaxAttempt() {
      val maxAttempt = 15031990
      configFetchActivity =
        ConfigFetchActivityImpl(
          mAirbyteApiClient!!,
          maxAttempt,
          Supplier { Instant.now().getEpochSecond() },
          mFeatureFlagClient!!,
          mScheduleJitterHelper!!,
          mFfContextMapper!!,
        )
      Assertions
        .assertThat(configFetchActivity!!.getMaxAttempt().maxAttempt)
        .isEqualTo(maxAttempt)
    }
  }

  @Nested
  internal inner class TestGetConnectionContext {
    private val connectionId: UUID = UUID.randomUUID()
    private val request = ConnectionIdRequestBody(connectionId)

    @BeforeEach
    fun setup() {
      whenever(mAirbyteApiClient!!.connectionApi).thenReturn(mConnectionApi)

      configFetchActivity =
        ConfigFetchActivityImpl(
          mAirbyteApiClient,
          SYNC_JOB_MAX_ATTEMPTS,
          currentSecondsSupplier,
          mFeatureFlagClient!!,
          mScheduleJitterHelper!!,
          mFfContextMapper!!,
        )
    }

    @Test
    @DisplayName("Retrieves connection context from server")
    @Throws(IOException::class)
    fun happyPath() {
      val contextRead =
        ConnectionContextRead(
          connectionId,
          UUID.randomUUID(),
          UUID.randomUUID(),
          UUID.randomUUID(),
          UUID.randomUUID(),
          UUID.randomUUID(),
          UUID.randomUUID(),
        )

      whenever(mConnectionApi!!.getConnectionContext(request))
        .thenReturn(contextRead)

      val result = configFetchActivity!!.getConnectionContext(GetConnectionContextInput(connectionId))
      val expected = GetConnectionContextOutput(contextRead.toInternal())
      org.junit.jupiter.api.Assertions
        .assertEquals(expected, result)
    }

    @MockitoSettings(strictness = Strictness.LENIENT)
    @Test
    @DisplayName("Propagates API exception as Retryable")
    @Throws(IOException::class)
    fun exceptionalPath() {
      whenever(mConnectionApi!!.getConnectionContext(request))
        .thenThrow(IOException("bang"))

      org.junit.jupiter.api.Assertions.assertThrows<RetryableException?>(
        RetryableException::class.java,
        Executable {
          configFetchActivity!!.getConnectionContext(
            GetConnectionContextInput(
              connectionId,
            ),
          )
        },
      )
    }
  }

  @Nested
  internal inner class TestLoadShedBackoff {
    private val connectionId: UUID = UUID.randomUUID()
    private val request = ConnectionIdRequestBody(connectionId)
    private val connectionContext: ConnectionContext =
      ConnectionContext()
        .withConnectionId(connectionId)
        .withSourceId(UUID.randomUUID())
        .withDestinationId(UUID.randomUUID())
        .withSourceDefinitionId(UUID.randomUUID())
        .withDestinationDefinitionId(UUID.randomUUID())
        .withWorkspaceId(UUID.randomUUID())
        .withOrganizationId(UUID.randomUUID())
    private val ffContext: Context = Multi(List.of<Context?>(Workspace(UUID.randomUUID()), Connection(UUID.randomUUID())))

    @BeforeEach
    @Throws(IOException::class)
    fun setup() {
      whenever(mFfContextMapper!!.map(connectionContext)).thenReturn(ffContext)

      configFetchActivity =
        ConfigFetchActivityImpl(
          mAirbyteApiClient!!,
          SYNC_JOB_MAX_ATTEMPTS,
          currentSecondsSupplier,
          mFeatureFlagClient!!,
          mScheduleJitterHelper!!,
          mFfContextMapper,
        )
    }

    @Test
    @DisplayName("Retrieves connection context from server and returns result of flag checked against the context")
    fun happyPath() {
      val backoff = 15
      whenever(mFeatureFlagClient!!.intVariation(LoadShedSchedulerBackoffMinutes, ffContext)).thenReturn(backoff)

      val result = configFetchActivity!!.getLoadShedBackoff(GetLoadShedBackoffInput(connectionContext))

      org.junit.jupiter.api.Assertions
        .assertEquals(Duration.ofMinutes(backoff.toLong()), result.duration)
    }

    @Test
    @DisplayName("Ensures time to wait is greater than 0")
    fun clampLowerBound() {
      val backoff = -1
      whenever(mFeatureFlagClient!!.intVariation(LoadShedSchedulerBackoffMinutes, ffContext)).thenReturn(backoff)

      val result = configFetchActivity!!.getLoadShedBackoff(GetLoadShedBackoffInput(connectionContext))

      org.junit.jupiter.api.Assertions
        .assertEquals(Duration.ZERO, result.duration)
    }

    @Test
    @DisplayName("Ensures time to wait is at most 1 hour")
    fun clampUpperBound() {
      val backoff = 1242
      whenever(mFeatureFlagClient!!.intVariation(LoadShedSchedulerBackoffMinutes, ffContext)).thenReturn(backoff)

      val result = configFetchActivity!!.getLoadShedBackoff(GetLoadShedBackoffInput(connectionContext))

      org.junit.jupiter.api.Assertions
        .assertEquals(Duration.ofHours(1), result.duration)
    }
  }

  companion object {
    private val CONNECTION_ID: UUID = UUID.randomUUID()
    private const val CONNECTION_NAME = "connection-name"
    private val SOURCE_ID: UUID = UUID.randomUUID()
    private val DESTINATION_ID: UUID = UUID.randomUUID()

    private const val SYNC_JOB_MAX_ATTEMPTS = 3

    private val connectionReadWithManualScheduleType =
      ConnectionRead(
        CONNECTION_ID,
        CONNECTION_NAME,
        SOURCE_ID,
        DESTINATION_ID,
        AirbyteCatalog(mutableListOf<AirbyteStreamAndConfiguration>()),
        ConnectionStatus.ACTIVE,
        false,
        null,
        null,
        null,
        null,
        null,
        ConnectionScheduleType.MANUAL,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
      )

    private val connectionReadWithBasicScheduleType =
      ConnectionRead(
        CONNECTION_ID,
        CONNECTION_NAME,
        SOURCE_ID,
        DESTINATION_ID,
        AirbyteCatalog(mutableListOf<AirbyteStreamAndConfiguration>()),
        ConnectionStatus.ACTIVE,
        false,
        null,
        null,
        null,
        null,
        null,
        ConnectionScheduleType.BASIC,
        ConnectionScheduleData(ConnectionScheduleDataBasicSchedule(ConnectionScheduleDataBasicSchedule.TimeUnit.MINUTES, 5L), null),
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
      )

    const val UTC: String = "UTC"
    private val connectionReadWithCronScheduleType =
      ConnectionRead(
        CONNECTION_ID,
        CONNECTION_NAME,
        SOURCE_ID,
        DESTINATION_ID,
        AirbyteCatalog(mutableListOf<AirbyteStreamAndConfiguration>()),
        ConnectionStatus.ACTIVE,
        false,
        null,
        null,
        null,
        null,
        null,
        ConnectionScheduleType.CRON,
        ConnectionScheduleData(null, ConnectionScheduleDataCron("0 0 12 * * ?", UTC)),
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
      )

    private val connectionReadWithScheduleDisable =
      ConnectionRead(
        CONNECTION_ID,
        CONNECTION_NAME,
        SOURCE_ID,
        DESTINATION_ID,
        AirbyteCatalog(mutableListOf<AirbyteStreamAndConfiguration>()),
        ConnectionStatus.INACTIVE,
        false,
        null,
        null,
        null,
        null,
        ConnectionSchedule(5L, ConnectionSchedule.TimeUnit.MINUTES),
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
      )

    private val connectionReadWithScheduleDeleted =
      ConnectionRead(
        CONNECTION_ID,
        CONNECTION_NAME,
        SOURCE_ID,
        DESTINATION_ID,
        AirbyteCatalog(mutableListOf<AirbyteStreamAndConfiguration>()),
        ConnectionStatus.DEPRECATED,
        false,
        null,
        null,
        null,
        null,
        ConnectionSchedule(5L, ConnectionSchedule.TimeUnit.MINUTES),
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
      )
  }
}
