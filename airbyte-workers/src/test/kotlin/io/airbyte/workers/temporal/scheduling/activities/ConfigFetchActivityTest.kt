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
import io.mockk.every
import io.mockk.mockk
import org.assertj.core.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import java.io.IOException
import java.time.Duration
import java.time.Instant
import java.util.Calendar
import java.util.TimeZone
import java.util.UUID
import java.util.function.Supplier

internal class ConfigFetchActivityTest {
  private lateinit var mAirbyteApiClient: AirbyteApiClient
  private lateinit var mJobsApi: JobsApi
  private lateinit var mWorkspaceApi: WorkspaceApi
  private lateinit var mJobRead: JobRead
  private lateinit var mConnectionApi: ConnectionApi
  private lateinit var mScheduleJitterHelper: ScheduleJitterHelper
  private lateinit var mFfContextMapper: InputFeatureFlagContextMapper
  private lateinit var mFeatureFlagClient: FeatureFlagClient
  private lateinit var configFetchActivity: ConfigFetchActivityImpl
  private val currentSecondsSupplier: Supplier<Long> = Supplier { Instant.now().epochSecond }

  @BeforeEach
  fun setup() {
    mAirbyteApiClient = mockk()
    mJobsApi = mockk()
    mWorkspaceApi = mockk()
    mJobRead = mockk()
    mConnectionApi = mockk()
    mScheduleJitterHelper = mockk()
    mFfContextMapper = mockk()
    mFeatureFlagClient = mockk<TestClient>(relaxed = true)
  }

  @Nested
  internal inner class TimeToWaitTest {
    @BeforeEach
    fun setup() {
      every { mWorkspaceApi.getWorkspaceByConnectionId(any<ConnectionIdRequestBody>()) } returns
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
        )
    }

    @Nested
    internal inner class TestNotCron {
      @BeforeEach
      fun setup() {
        every { mAirbyteApiClient.connectionApi } returns mConnectionApi
        every { mAirbyteApiClient.workspaceApi } returns mWorkspaceApi
        configFetchActivity =
          ConfigFetchActivityImpl(
            mAirbyteApiClient,
            SYNC_JOB_MAX_ATTEMPTS,
            currentSecondsSupplier,
            mFeatureFlagClient,
            mScheduleJitterHelper,
            mFfContextMapper,
          )
      }

      @Test
      @DisplayName("Test that the job will wait for a long time if it is disabled")
      fun testDisable() {
        every { mConnectionApi.getConnection(any<ConnectionIdRequestBody>()) } returns
          connectionReadWithScheduleDisable

        val input = ScheduleRetrieverInput(CONNECTION_ID)

        val output = configFetchActivity.getTimeToWait(input)

        Assertions
          .assertThat(output.timeToWait)
          .hasDays((100 * 365).toLong())
      }

      @Test
      @DisplayName("Test that the connection will wait for a long time if it is deleted")
      fun testDeleted() {
        every { mConnectionApi.getConnection(any<ConnectionIdRequestBody>()) } returns
          connectionReadWithScheduleDeleted

        val input = ScheduleRetrieverInput(CONNECTION_ID)

        val output = configFetchActivity.getTimeToWait(input)

        Assertions
          .assertThat(output.timeToWait)
          .hasDays((100 * 365).toLong())
      }

      @Test
      @DisplayName("Test that the job will wait a long time if it is MANUAL scheduleType")
      fun testManualScheduleType() {
        every { mConnectionApi.getConnection(any<ConnectionIdRequestBody>()) } returns connectionReadWithManualScheduleType

        val input = ScheduleRetrieverInput(CONNECTION_ID)

        val output = configFetchActivity.getTimeToWait(input)

        Assertions
          .assertThat(output.timeToWait)
          .hasDays((100 * 365).toLong())
      }

      @Test
      @DisplayName("Test that the job will be immediately scheduled if it is a BASIC_SCHEDULE type on the first run")
      fun testBasicScheduleTypeFirstRun() {
        every { mAirbyteApiClient.jobsApi } returns mJobsApi
        every { mJobsApi.getLastReplicationJobWithCancel(any<ConnectionIdRequestBody>()) } returns JobOptionalRead()

        every { mConnectionApi.getConnection(any<ConnectionIdRequestBody>()) } returns connectionReadWithBasicScheduleType

        val input = ScheduleRetrieverInput(CONNECTION_ID)

        val output = configFetchActivity.getTimeToWait(input)

        Assertions
          .assertThat(output.timeToWait)
          .isZero()
      }

      @Test
      @DisplayName("Test that we will wait the required amount of time with a BASIC_SCHEDULE type on a subsequent run")
      fun testBasicScheduleSubsequentRun() {
        every { mAirbyteApiClient.jobsApi } returns mJobsApi
        configFetchActivity =
          ConfigFetchActivityImpl(
            mAirbyteApiClient,
            SYNC_JOB_MAX_ATTEMPTS,
            { 60L * 3 },
            mFeatureFlagClient,
            mScheduleJitterHelper,
            mFfContextMapper,
          )

        every { mJobRead.createdAt } returns 60L

        every { mJobsApi.getLastReplicationJobWithCancel(any<ConnectionIdRequestBody>()) } returns JobOptionalRead(mJobRead)

        every { mConnectionApi.getConnection(any<ConnectionIdRequestBody>()) } returns connectionReadWithBasicScheduleType

        val input = ScheduleRetrieverInput(CONNECTION_ID)

        val output = configFetchActivity.getTimeToWait(input)

        Assertions
          .assertThat(output.timeToWait)
          .hasMinutes(3)
      }
    }

    @Nested
    internal inner class TestCronSchedule {
      @BeforeEach
      fun setup() {
        every { mAirbyteApiClient.connectionApi } returns mConnectionApi
        every { mAirbyteApiClient.jobsApi } returns mJobsApi
        every { mAirbyteApiClient.workspaceApi } returns mWorkspaceApi
      }

      @Test
      @DisplayName("Test that the job will wait to be scheduled if it is a CRON type, and the prior job ran recently")
      fun testCronScheduleSubsequentRunPriorJobRanRecently() {
        val mockRightNow: Calendar = Calendar.getInstance(TimeZone.getTimeZone(UTC))
        mockRightNow.set(Calendar.HOUR_OF_DAY, 0)
        mockRightNow.set(Calendar.MINUTE, 0)
        mockRightNow.set(Calendar.SECOND, 0)
        mockRightNow.set(Calendar.MILLISECOND, 0)
        val currentSecondsSupplier: Supplier<Long> = Supplier { mockRightNow.getTimeInMillis() / 1000L }

        configFetchActivity =
          ConfigFetchActivityImpl(
            mAirbyteApiClient,
            SYNC_JOB_MAX_ATTEMPTS,
            currentSecondsSupplier,
            mFeatureFlagClient,
            mScheduleJitterHelper,
            mFfContextMapper,
          )

        every { mJobsApi.getLastReplicationJobWithCancel(any<ConnectionIdRequestBody>()) } returns JobOptionalRead(mJobRead)
        // prior job completed 3 hours ago, so expect the next job to be scheduled
        // according to the next cron run time.
        val threeHoursAgoSeconds = currentSecondsSupplier.get() - Duration.ofHours(3).toSeconds()
        every { mJobRead.startedAt } returns null
        every { mJobRead.createdAt } returns threeHoursAgoSeconds
        every { mConnectionApi.getConnection(any<ConnectionIdRequestBody>()) } returns connectionReadWithCronScheduleType

        val input = ScheduleRetrieverInput(CONNECTION_ID)

        val output = configFetchActivity.getTimeToWait(input)

        Assertions
          .assertThat(output.timeToWait)
          .hasHours(12)
      }

      @Test
      @DisplayName("Test that the job will run immediately if it is CRON type, and the expected interval has elapsed since the prior job")
      fun testCronScheduleSubsequentRunPriorJobRanLongAgo() {
        val mockRightNow: Calendar = Calendar.getInstance(TimeZone.getTimeZone(UTC))
        mockRightNow.set(Calendar.HOUR_OF_DAY, 0)
        mockRightNow.set(Calendar.MINUTE, 0)
        mockRightNow.set(Calendar.SECOND, 0)
        mockRightNow.set(Calendar.MILLISECOND, 0)
        val currentSecondsSupplier: Supplier<Long> = Supplier { mockRightNow.getTimeInMillis() / 1000L }

        configFetchActivity =
          ConfigFetchActivityImpl(
            mAirbyteApiClient,
            SYNC_JOB_MAX_ATTEMPTS,
            currentSecondsSupplier,
            mFeatureFlagClient,
            mScheduleJitterHelper,
            mFfContextMapper,
          )

        every { mJobsApi.getLastReplicationJobWithCancel(any<ConnectionIdRequestBody>()) } returns JobOptionalRead(mJobRead)
        // Behavior is currently behind a feature flag
        every {
          mFeatureFlagClient.boolVariation(
            eq(UseNewCronScheduleCalculation),
            any<Context>(),
          )
        } returns true
        // prior job completed over 24 hours ago, so expect the next job to be scheduled immediately
        val twentyFiveHoursAgoSeconds = currentSecondsSupplier.get() - Duration.ofHours(25).toSeconds()
        every { mJobRead.createdAt } returns twentyFiveHoursAgoSeconds
        every { mConnectionApi.getConnection(any<ConnectionIdRequestBody>()) } returns connectionReadWithCronScheduleType

        val input = ScheduleRetrieverInput(CONNECTION_ID)

        val output = configFetchActivity.getTimeToWait(input)

        Assertions.assertThat(output.timeToWait).isZero()
      }

      @Test
      @DisplayName("Test that the job will only be scheduled once per minimum cron interval")
      fun testCronScheduleMinimumInterval() {
        val mockRightNow: Calendar = Calendar.getInstance(TimeZone.getTimeZone(UTC))
        mockRightNow.set(Calendar.HOUR_OF_DAY, 12)
        mockRightNow.set(Calendar.MINUTE, 0)
        mockRightNow.set(Calendar.SECOND, 0)
        mockRightNow.set(Calendar.MILLISECOND, 0)
        val currentSecondsSupplier: Supplier<Long> = Supplier { mockRightNow.getTimeInMillis() / 1000L }

        configFetchActivity =
          ConfigFetchActivityImpl(
            mAirbyteApiClient,
            SYNC_JOB_MAX_ATTEMPTS,
            currentSecondsSupplier,
            mFeatureFlagClient,
            mScheduleJitterHelper,
            mFfContextMapper,
          )

        every { mJobRead.startedAt } returns null
        every { mJobRead.createdAt } returns mockRightNow.getTimeInMillis() / 1000L
        every { mJobsApi.getLastReplicationJobWithCancel(any<ConnectionIdRequestBody>()) } returns JobOptionalRead(mJobRead)

        every { mConnectionApi.getConnection(any<ConnectionIdRequestBody>()) } returns connectionReadWithCronScheduleType

        val input = ScheduleRetrieverInput(CONNECTION_ID)

        val output = configFetchActivity.getTimeToWait(input)

        Assertions
          .assertThat(output.timeToWait)
          .hasHours(24)
      }

      @Test
      @DisplayName("Test that for specific workspace ids, we add some noise in the cron scheduling")
      fun testCronSchedulingNoise() {
        val mockRightNow: Calendar = Calendar.getInstance(TimeZone.getTimeZone(UTC))
        mockRightNow.set(Calendar.HOUR_OF_DAY, 0)
        mockRightNow.set(Calendar.MINUTE, 0)
        mockRightNow.set(Calendar.SECOND, 0)
        mockRightNow.set(Calendar.MILLISECOND, 0)
        val currentSecondsSupplier: Supplier<Long> = Supplier { mockRightNow.getTimeInMillis() / 1000L }

        every { mWorkspaceApi.getWorkspaceByConnectionId(any<ConnectionIdRequestBody>()) } returns
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
          )

        configFetchActivity =
          ConfigFetchActivityImpl(
            mAirbyteApiClient,
            SYNC_JOB_MAX_ATTEMPTS,
            currentSecondsSupplier,
            mFeatureFlagClient,
            mScheduleJitterHelper,
            mFfContextMapper,
          )

        every { mJobRead.startedAt } returns null
        every { mJobRead.createdAt } returns mockRightNow.getTimeInMillis() / 1000L
        every { mJobsApi.getLastReplicationJobWithCancel(any<ConnectionIdRequestBody>()) } returns JobOptionalRead(mJobRead)

        every { mConnectionApi.getConnection(any<ConnectionIdRequestBody>()) } returns connectionReadWithCronScheduleType

        val input = ScheduleRetrieverInput(CONNECTION_ID)

        val output = configFetchActivity.getTimeToWait(input)

        // Note: compareTo returns positive if the left side is greater than the right.
        Assertions.assertThat(output.timeToWait!! > Duration.ofHours(12)).isTrue()
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
          mAirbyteApiClient,
          maxAttempt,
          { Instant.now().epochSecond },
          mFeatureFlagClient,
          mScheduleJitterHelper,
          mFfContextMapper,
        )
      Assertions
        .assertThat(configFetchActivity.getMaxAttempt().maxAttempt)
        .isEqualTo(maxAttempt)
    }
  }

  @Nested
  internal inner class TestGetConnectionContext {
    private val connectionId: UUID = UUID.randomUUID()
    private val request = ConnectionIdRequestBody(connectionId)

    @BeforeEach
    fun setup() {
      every { mAirbyteApiClient.connectionApi } returns mConnectionApi
      configFetchActivity =
        ConfigFetchActivityImpl(
          mAirbyteApiClient,
          SYNC_JOB_MAX_ATTEMPTS,
          currentSecondsSupplier,
          mFeatureFlagClient,
          mScheduleJitterHelper,
          mFfContextMapper,
        )
    }

    @Test
    @DisplayName("Retrieves connection context from server")
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

      every { mConnectionApi.getConnectionContext(request) } returns contextRead

      val result = configFetchActivity.getConnectionContext(GetConnectionContextInput(connectionId))
      val expected = GetConnectionContextOutput(contextRead.toInternal())
      org.junit.jupiter.api.Assertions
        .assertEquals(expected, result)
    }

    @Test
    @DisplayName("Propagates API exception as Retryable")
    fun exceptionalPath() {
      every { mConnectionApi.getConnectionContext(request) } answers { throw IOException("bang") }

      org.junit.jupiter.api.Assertions.assertThrows(
        RetryableException::class.java,
      ) {
        configFetchActivity.getConnectionContext(
          GetConnectionContextInput(
            connectionId,
          ),
        )
      }
    }
  }

  @Nested
  internal inner class TestLoadShedBackoff {
    private val connectionId: UUID = UUID.randomUUID()
    private val connectionContext: ConnectionContext =
      ConnectionContext()
        .withConnectionId(connectionId)
        .withSourceId(UUID.randomUUID())
        .withDestinationId(UUID.randomUUID())
        .withSourceDefinitionId(UUID.randomUUID())
        .withDestinationDefinitionId(UUID.randomUUID())
        .withWorkspaceId(UUID.randomUUID())
        .withOrganizationId(UUID.randomUUID())
    private val ffContext: Context = Multi(listOf(Workspace(UUID.randomUUID()), Connection(UUID.randomUUID())))

    @BeforeEach
    fun setup() {
      every { mFfContextMapper.map(connectionContext) } returns ffContext
      configFetchActivity =
        ConfigFetchActivityImpl(
          mAirbyteApiClient,
          SYNC_JOB_MAX_ATTEMPTS,
          currentSecondsSupplier,
          mFeatureFlagClient,
          mScheduleJitterHelper,
          mFfContextMapper,
        )
    }

    @Test
    @DisplayName("Retrieves connection context from server and returns result of flag checked against the context")
    fun happyPath() {
      val backoff = 15
      every { mFeatureFlagClient.intVariation(LoadShedSchedulerBackoffMinutes, ffContext) } returns backoff
      val result = configFetchActivity.getLoadShedBackoff(GetLoadShedBackoffInput(connectionContext))

      org.junit.jupiter.api.Assertions
        .assertEquals(Duration.ofMinutes(backoff.toLong()), result.duration)
    }

    @Test
    @DisplayName("Ensures time to wait is greater than 0")
    fun clampLowerBound() {
      val backoff = -1
      every { mFeatureFlagClient.intVariation(LoadShedSchedulerBackoffMinutes, ffContext) } returns backoff
      val result = configFetchActivity.getLoadShedBackoff(GetLoadShedBackoffInput(connectionContext))

      org.junit.jupiter.api.Assertions
        .assertEquals(Duration.ZERO, result.duration)
    }

    @Test
    @DisplayName("Ensures time to wait is at most 1 hour")
    fun clampUpperBound() {
      val backoff = 1242
      every { mFeatureFlagClient.intVariation(LoadShedSchedulerBackoffMinutes, ffContext) } returns backoff
      val result = configFetchActivity.getLoadShedBackoff(GetLoadShedBackoffInput(connectionContext))

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
