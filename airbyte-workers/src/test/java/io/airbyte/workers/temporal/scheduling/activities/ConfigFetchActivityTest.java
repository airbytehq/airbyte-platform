/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.scheduling.activities;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.airbyte.api.client.AirbyteApiClient;
import io.airbyte.api.client.generated.ConnectionApi;
import io.airbyte.api.client.generated.JobsApi;
import io.airbyte.api.client.generated.WorkspaceApi;
import io.airbyte.api.client.model.generated.AirbyteCatalog;
import io.airbyte.api.client.model.generated.ConnectionContextRead;
import io.airbyte.api.client.model.generated.ConnectionIdRequestBody;
import io.airbyte.api.client.model.generated.ConnectionRead;
import io.airbyte.api.client.model.generated.ConnectionSchedule;
import io.airbyte.api.client.model.generated.ConnectionScheduleData;
import io.airbyte.api.client.model.generated.ConnectionScheduleDataBasicSchedule;
import io.airbyte.api.client.model.generated.ConnectionScheduleDataCron;
import io.airbyte.api.client.model.generated.ConnectionScheduleType;
import io.airbyte.api.client.model.generated.ConnectionStatus;
import io.airbyte.api.client.model.generated.JobOptionalRead;
import io.airbyte.api.client.model.generated.JobRead;
import io.airbyte.api.client.model.generated.WorkspaceRead;
import io.airbyte.commons.converters.CommonConvertersKt;
import io.airbyte.commons.temporal.exception.RetryableException;
import io.airbyte.config.ConnectionContext;
import io.airbyte.featureflag.Connection;
import io.airbyte.featureflag.Context;
import io.airbyte.featureflag.FeatureFlagClient;
import io.airbyte.featureflag.LoadShedSchedulerBackoffMinutes;
import io.airbyte.featureflag.Multi;
import io.airbyte.featureflag.TestClient;
import io.airbyte.featureflag.UseNewCronScheduleCalculation;
import io.airbyte.featureflag.Workspace;
import io.airbyte.workers.helpers.ScheduleJitterHelper;
import io.airbyte.workers.input.InputFeatureFlagContextMapper;
import io.airbyte.workers.temporal.activities.GetConnectionContextInput;
import io.airbyte.workers.temporal.activities.GetConnectionContextOutput;
import io.airbyte.workers.temporal.activities.GetLoadShedBackoffInput;
import io.airbyte.workers.temporal.scheduling.activities.ConfigFetchActivity.ScheduleRetrieverInput;
import io.airbyte.workers.temporal.scheduling.activities.ConfigFetchActivity.ScheduleRetrieverOutput;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Calendar;
import java.util.List;
import java.util.TimeZone;
import java.util.UUID;
import java.util.function.Supplier;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

@ExtendWith(MockitoExtension.class)
class ConfigFetchActivityTest {

  private static final UUID CONNECTION_ID = UUID.randomUUID();
  private static final String CONNECTION_NAME = "connection-name";
  private static final UUID SOURCE_ID = UUID.randomUUID();
  private static final UUID DESTINATION_ID = UUID.randomUUID();

  private static final Integer SYNC_JOB_MAX_ATTEMPTS = 3;

  @Mock
  private AirbyteApiClient mAirbyteApiClient;

  @Mock
  private JobsApi mJobsApi;

  @Mock
  private WorkspaceApi mWorkspaceApi;

  @Mock
  private JobRead mJobRead;

  @Mock
  private ConnectionApi mConnectionApi;

  @Mock
  private ScheduleJitterHelper mScheduleJitterHelper;

  @Mock
  private InputFeatureFlagContextMapper mFfContextMapper;

  private FeatureFlagClient mFeatureFlagClient;

  private ConfigFetchActivityImpl configFetchActivity;

  private final Supplier<Long> currentSecondsSupplier = () -> Instant.now().getEpochSecond();

  private static final ConnectionRead connectionReadWithLegacySchedule = new ConnectionRead(
      CONNECTION_ID, CONNECTION_NAME, SOURCE_ID, DESTINATION_ID, new AirbyteCatalog(List.of()), ConnectionStatus.ACTIVE, false, null, null, null,
      null, new ConnectionSchedule(5L, ConnectionSchedule.TimeUnit.MINUTES), null, null, null, null, null, null, null, null, null, null, null, null,
      null);

  private static final ConnectionRead connectionReadWithManualScheduleType = new ConnectionRead(
      CONNECTION_ID, CONNECTION_NAME, SOURCE_ID, DESTINATION_ID, new AirbyteCatalog(List.of()), ConnectionStatus.ACTIVE, false, null, null, null,
      null, null, ConnectionScheduleType.MANUAL, null, null, null, null, null, null, null, null, null, null, null, null);

  private static final ConnectionRead connectionReadWithBasicScheduleType = new ConnectionRead(
      CONNECTION_ID, CONNECTION_NAME, SOURCE_ID, DESTINATION_ID, new AirbyteCatalog(List.of()), ConnectionStatus.ACTIVE, false, null, null, null,
      null,
      null, ConnectionScheduleType.BASIC,
      new ConnectionScheduleData(new ConnectionScheduleDataBasicSchedule(ConnectionScheduleDataBasicSchedule.TimeUnit.MINUTES, 5L), null), null, null,
      null, null, null, null, null, null, null, null, null);

  public static final String UTC = "UTC";
  private static final ConnectionRead connectionReadWithCronScheduleType = new ConnectionRead(
      CONNECTION_ID, CONNECTION_NAME, SOURCE_ID, DESTINATION_ID, new AirbyteCatalog(List.of()), ConnectionStatus.ACTIVE, false, null, null, null,
      null, null, ConnectionScheduleType.CRON, new ConnectionScheduleData(null, new ConnectionScheduleDataCron("0 0 12 * * ?", UTC)), null, null,
      null, null, null, null, null, null, null, null, null);

  private static final ConnectionRead connectionReadWithScheduleDisable = new ConnectionRead(
      CONNECTION_ID, CONNECTION_NAME, SOURCE_ID, DESTINATION_ID, new AirbyteCatalog(List.of()), ConnectionStatus.INACTIVE, false, null, null, null,
      null, new ConnectionSchedule(5L, ConnectionSchedule.TimeUnit.MINUTES), null, null, null, null, null, null, null, null, null, null, null, null,
      null);

  private static final ConnectionRead connectionReadWithScheduleDeleted = new ConnectionRead(
      CONNECTION_ID, CONNECTION_NAME, SOURCE_ID, DESTINATION_ID, new AirbyteCatalog(List.of()), ConnectionStatus.DEPRECATED, false, null, null, null,
      null, new ConnectionSchedule(5L, ConnectionSchedule.TimeUnit.MINUTES), null, null, null, null, null, null, null, null, null, null, null, null,
      null);

  private static final ConnectionRead connectionReadWithoutSchedule = new ConnectionRead(
      CONNECTION_ID, CONNECTION_NAME, SOURCE_ID, DESTINATION_ID, new AirbyteCatalog(List.of()), ConnectionStatus.DEPRECATED, false, null, null, null,
      null, null, null, null, null, null, null, null, null, null, null, null, null, null, null);

  @BeforeEach
  void setup() {
    mFeatureFlagClient = mock(TestClient.class);
  }

  @Nested
  class TimeToWaitTest {

    @BeforeEach
    void setup() throws IOException {
      when(mWorkspaceApi.getWorkspaceByConnectionId(any())).thenReturn(new WorkspaceRead(UUID.randomUUID(), UUID.randomUUID(), "name", "slug", false,
          UUID.randomUUID(), null, null, null, null, null, null, null, null, null, null, null, null, null));
    }

    @Nested
    class TestNotCron {

      @BeforeEach
      void setup() {
        when(mAirbyteApiClient.getConnectionApi()).thenReturn(mConnectionApi);
        when(mAirbyteApiClient.getWorkspaceApi()).thenReturn(mWorkspaceApi);
        configFetchActivity = new ConfigFetchActivityImpl(mAirbyteApiClient, SYNC_JOB_MAX_ATTEMPTS, currentSecondsSupplier,
            mFeatureFlagClient, mScheduleJitterHelper, mFfContextMapper);
      }

      @Test
      @DisplayName("Test that the job gets scheduled if it is not manual and if it is the first run with legacy schedule schema")
      void testFirstJobNonManual() throws IOException {
        when(mAirbyteApiClient.getJobsApi()).thenReturn(mJobsApi);
        when(mJobsApi.getLastReplicationJobWithCancel(any()))
            .thenReturn(new JobOptionalRead());

        when(mConnectionApi.getConnection(any()))
            .thenReturn(connectionReadWithLegacySchedule);

        final ScheduleRetrieverInput input = new ScheduleRetrieverInput(CONNECTION_ID);

        final ScheduleRetrieverOutput output = configFetchActivity.getTimeToWait(input);

        Assertions.assertThat(output.getTimeToWait())
            .isZero();
      }

      @Test
      @DisplayName("Test that the job will wait for a long time if it is manual in the legacy schedule schema")
      void testManual() throws IOException {
        when(mConnectionApi.getConnection(any()))
            .thenReturn(connectionReadWithoutSchedule);

        final ScheduleRetrieverInput input = new ScheduleRetrieverInput(CONNECTION_ID);

        final ScheduleRetrieverOutput output = configFetchActivity.getTimeToWait(input);

        Assertions.assertThat(output.getTimeToWait())
            .hasDays(100 * 365);
      }

      @Test
      @DisplayName("Test that the job will wait for a long time if it is disabled")
      void testDisable() throws IOException {
        when(mConnectionApi.getConnection(any()))
            .thenReturn(connectionReadWithScheduleDisable);

        final ScheduleRetrieverInput input = new ScheduleRetrieverInput(CONNECTION_ID);

        final ScheduleRetrieverOutput output = configFetchActivity.getTimeToWait(input);

        Assertions.assertThat(output.getTimeToWait())
            .hasDays(100 * 365);
      }

      @Test
      @DisplayName("Test that the connection will wait for a long time if it is deleted")
      void testDeleted() throws IOException {
        when(mConnectionApi.getConnection(any()))
            .thenReturn(connectionReadWithScheduleDeleted);

        final ScheduleRetrieverInput input = new ScheduleRetrieverInput(CONNECTION_ID);

        final ScheduleRetrieverOutput output = configFetchActivity.getTimeToWait(input);

        Assertions.assertThat(output.getTimeToWait())
            .hasDays(100 * 365);
      }

      @Test
      @DisplayName("Test we will wait the required amount of time with legacy config")
      void testWait() throws IOException {
        when(mAirbyteApiClient.getJobsApi()).thenReturn(mJobsApi);
        configFetchActivity =
            new ConfigFetchActivityImpl(mAirbyteApiClient, SYNC_JOB_MAX_ATTEMPTS, () -> 60L * 3, mFeatureFlagClient,
                mScheduleJitterHelper, mFfContextMapper);

        when(mJobRead.getStartedAt()).thenReturn(null);
        when(mJobRead.getCreatedAt())
            .thenReturn(60L);

        when(mJobsApi.getLastReplicationJobWithCancel(any()))
            .thenReturn(new JobOptionalRead(mJobRead));

        when(mConnectionApi.getConnection(any()))
            .thenReturn(connectionReadWithLegacySchedule);

        final ScheduleRetrieverInput input = new ScheduleRetrieverInput(CONNECTION_ID);

        final ScheduleRetrieverOutput output = configFetchActivity.getTimeToWait(input);

        Assertions.assertThat(output.getTimeToWait())
            .hasMinutes(3);
      }

      @Test
      @DisplayName("Test we will not wait if we are late in the legacy schedule schema")
      void testNotWaitIfLate() throws IOException {
        when(mAirbyteApiClient.getJobsApi()).thenReturn(mJobsApi);
        configFetchActivity =
            new ConfigFetchActivityImpl(mAirbyteApiClient, SYNC_JOB_MAX_ATTEMPTS, () -> 60L * 10, mFeatureFlagClient,
                mScheduleJitterHelper, mFfContextMapper);

        when(mJobRead.getStartedAt()).thenReturn(null);
        when(mJobRead.getCreatedAt())
            .thenReturn(60L);

        when(mJobsApi.getLastReplicationJobWithCancel(any()))
            .thenReturn(new JobOptionalRead(mJobRead));

        when(mConnectionApi.getConnection(any()))
            .thenReturn(connectionReadWithLegacySchedule);

        final ScheduleRetrieverInput input = new ScheduleRetrieverInput(CONNECTION_ID);

        final ScheduleRetrieverOutput output = configFetchActivity.getTimeToWait(input);

        Assertions.assertThat(output.getTimeToWait())
            .isZero();
      }

      @Test
      @DisplayName("Test that the job will wait a long time if it is MANUAL scheduleType")
      void testManualScheduleType() throws IOException {
        when(mConnectionApi.getConnection(any()))
            .thenReturn(connectionReadWithManualScheduleType);

        final ScheduleRetrieverInput input = new ScheduleRetrieverInput(CONNECTION_ID);

        final ScheduleRetrieverOutput output = configFetchActivity.getTimeToWait(input);

        Assertions.assertThat(output.getTimeToWait())
            .hasDays(100 * 365);
      }

      @Test
      @DisplayName("Test that the job will be immediately scheduled if it is a BASIC_SCHEDULE type on the first run")
      void testBasicScheduleTypeFirstRun() throws IOException {
        when(mAirbyteApiClient.getJobsApi()).thenReturn(mJobsApi);
        when(mJobsApi.getLastReplicationJobWithCancel(any()))
            .thenReturn(new JobOptionalRead());

        when(mConnectionApi.getConnection(any()))
            .thenReturn(connectionReadWithBasicScheduleType);

        final ScheduleRetrieverInput input = new ScheduleRetrieverInput(CONNECTION_ID);

        final ScheduleRetrieverOutput output = configFetchActivity.getTimeToWait(input);

        Assertions.assertThat(output.getTimeToWait())
            .isZero();
      }

      @Test
      @DisplayName("Test that we will wait the required amount of time with a BASIC_SCHEDULE type on a subsequent run")
      void testBasicScheduleSubsequentRun() throws IOException {
        when(mAirbyteApiClient.getJobsApi()).thenReturn(mJobsApi);
        configFetchActivity = new ConfigFetchActivityImpl(mAirbyteApiClient, SYNC_JOB_MAX_ATTEMPTS, () -> 60L * 3,
            mFeatureFlagClient, mScheduleJitterHelper, mFfContextMapper);

        when(mJobRead.getCreatedAt())
            .thenReturn(60L);

        when(mJobsApi.getLastReplicationJobWithCancel(any()))
            .thenReturn(new JobOptionalRead(mJobRead));

        when(mConnectionApi.getConnection(any()))
            .thenReturn(connectionReadWithBasicScheduleType);

        final ScheduleRetrieverInput input = new ScheduleRetrieverInput(CONNECTION_ID);

        final ScheduleRetrieverOutput output = configFetchActivity.getTimeToWait(input);

        Assertions.assertThat(output.getTimeToWait())
            .hasMinutes(3);
      }

    }

    @Nested
    class TestCronSchedule {

      @BeforeEach
      void setup() {
        when(mAirbyteApiClient.getConnectionApi()).thenReturn(mConnectionApi);
        when(mAirbyteApiClient.getJobsApi()).thenReturn(mJobsApi);
        when(mAirbyteApiClient.getWorkspaceApi()).thenReturn(mWorkspaceApi);
      }

      @Test
      @DisplayName("Test that the job will wait to be scheduled if it is a CRON type, and the prior job ran recently")
      void testCronScheduleSubsequentRunPriorJobRanRecently() throws IOException {
        final Calendar mockRightNow = Calendar.getInstance(TimeZone.getTimeZone(UTC));
        mockRightNow.set(Calendar.HOUR_OF_DAY, 0);
        mockRightNow.set(Calendar.MINUTE, 0);
        mockRightNow.set(Calendar.SECOND, 0);
        mockRightNow.set(Calendar.MILLISECOND, 0);
        final Supplier<Long> currentSecondsSupplier = () -> mockRightNow.getTimeInMillis() / 1000L;

        configFetchActivity =
            new ConfigFetchActivityImpl(mAirbyteApiClient, SYNC_JOB_MAX_ATTEMPTS,
                currentSecondsSupplier, mFeatureFlagClient, mScheduleJitterHelper, mFfContextMapper);

        when(mJobsApi.getLastReplicationJobWithCancel(any()))
            .thenReturn(new JobOptionalRead(mJobRead));

        // prior job completed 3 hours ago, so expect the next job to be scheduled
        // according to the next cron run time.
        final long threeHoursAgoSeconds = currentSecondsSupplier.get() - Duration.ofHours(3).toSeconds();
        when(mJobRead.getStartedAt()).thenReturn(null);
        when(mJobRead.getCreatedAt()).thenReturn(threeHoursAgoSeconds);

        when(mConnectionApi.getConnection(any()))
            .thenReturn(connectionReadWithCronScheduleType);

        final ScheduleRetrieverInput input = new ScheduleRetrieverInput(CONNECTION_ID);

        final ScheduleRetrieverOutput output = configFetchActivity.getTimeToWait(input);

        Assertions.assertThat(output.getTimeToWait())
            .hasHours(12);
      }

      @Test
      @DisplayName("Test that the job will run immediately if it is CRON type, and the expected interval has elapsed since the prior job")
      void testCronScheduleSubsequentRunPriorJobRanLongAgo() throws IOException {
        final Calendar mockRightNow = Calendar.getInstance(TimeZone.getTimeZone(UTC));
        mockRightNow.set(Calendar.HOUR_OF_DAY, 0);
        mockRightNow.set(Calendar.MINUTE, 0);
        mockRightNow.set(Calendar.SECOND, 0);
        mockRightNow.set(Calendar.MILLISECOND, 0);
        final Supplier<Long> currentSecondsSupplier = () -> mockRightNow.getTimeInMillis() / 1000L;

        configFetchActivity =
            new ConfigFetchActivityImpl(mAirbyteApiClient, SYNC_JOB_MAX_ATTEMPTS,
                currentSecondsSupplier, mFeatureFlagClient, mScheduleJitterHelper, mFfContextMapper);

        when(mJobsApi.getLastReplicationJobWithCancel(any()))
            .thenReturn(new JobOptionalRead(mJobRead));

        // Behavior is currently behind a feature flag
        when(mFeatureFlagClient.boolVariation(Mockito.eq(UseNewCronScheduleCalculation.INSTANCE), any())).thenReturn(true);

        // prior job completed over 24 hours ago, so expect the next job to be scheduled immediately
        final long twentyFiveHoursAgoSeconds = currentSecondsSupplier.get() - Duration.ofHours(25).toSeconds();
        when(mJobRead.getCreatedAt()).thenReturn(twentyFiveHoursAgoSeconds);

        when(mConnectionApi.getConnection(any()))
            .thenReturn(connectionReadWithCronScheduleType);

        final ScheduleRetrieverInput input = new ScheduleRetrieverInput(CONNECTION_ID);

        final ScheduleRetrieverOutput output = configFetchActivity.getTimeToWait(input);

        Assertions.assertThat(output.getTimeToWait()).isZero();
      }

      @Test
      @DisplayName("Test that the job will only be scheduled once per minimum cron interval")
      void testCronScheduleMinimumInterval() throws IOException {
        final Calendar mockRightNow = Calendar.getInstance(TimeZone.getTimeZone(UTC));
        mockRightNow.set(Calendar.HOUR_OF_DAY, 12);
        mockRightNow.set(Calendar.MINUTE, 0);
        mockRightNow.set(Calendar.SECOND, 0);
        mockRightNow.set(Calendar.MILLISECOND, 0);
        final Supplier<Long> currentSecondsSupplier = () -> mockRightNow.getTimeInMillis() / 1000L;

        configFetchActivity =
            new ConfigFetchActivityImpl(mAirbyteApiClient, SYNC_JOB_MAX_ATTEMPTS,
                currentSecondsSupplier, mFeatureFlagClient, mScheduleJitterHelper, mFfContextMapper);

        when(mJobRead.getStartedAt()).thenReturn(null);
        when(mJobRead.getCreatedAt()).thenReturn(mockRightNow.getTimeInMillis() / 1000L);
        when(mJobsApi.getLastReplicationJobWithCancel(any()))
            .thenReturn(new JobOptionalRead(mJobRead));

        when(mConnectionApi.getConnection(any()))
            .thenReturn(connectionReadWithCronScheduleType);

        final ScheduleRetrieverInput input = new ScheduleRetrieverInput(CONNECTION_ID);

        final ScheduleRetrieverOutput output = configFetchActivity.getTimeToWait(input);

        Assertions.assertThat(output.getTimeToWait())
            .hasHours(24);
      }

      @Test
      @DisplayName("Test that for specific workspace ids, we add some noise in the cron scheduling")
      void testCronSchedulingNoise() throws IOException {
        final Calendar mockRightNow = Calendar.getInstance(TimeZone.getTimeZone(UTC));
        mockRightNow.set(Calendar.HOUR_OF_DAY, 0);
        mockRightNow.set(Calendar.MINUTE, 0);
        mockRightNow.set(Calendar.SECOND, 0);
        mockRightNow.set(Calendar.MILLISECOND, 0);
        final Supplier<Long> currentSecondsSupplier = () -> mockRightNow.getTimeInMillis() / 1000L;

        when(mWorkspaceApi.getWorkspaceByConnectionId(any()))
            .thenReturn(new WorkspaceRead(UUID.fromString("226edbc1-4a9c-4401-95a9-90435d667d9d"), UUID.randomUUID(), "name", "slug", false,
                UUID.randomUUID(), null, null, null, null, null, null, null, null, null, null, null, null, null));

        configFetchActivity =
            new ConfigFetchActivityImpl(mAirbyteApiClient, SYNC_JOB_MAX_ATTEMPTS,
                currentSecondsSupplier, mFeatureFlagClient, mScheduleJitterHelper, mFfContextMapper);

        when(mJobRead.getStartedAt()).thenReturn(null);
        when(mJobRead.getCreatedAt()).thenReturn(mockRightNow.getTimeInMillis() / 1000L);
        when(mJobsApi.getLastReplicationJobWithCancel(any()))
            .thenReturn(new JobOptionalRead(mJobRead));

        when(mConnectionApi.getConnection(any()))
            .thenReturn(connectionReadWithCronScheduleType);

        final ScheduleRetrieverInput input = new ScheduleRetrieverInput(CONNECTION_ID);

        final ScheduleRetrieverOutput output = configFetchActivity.getTimeToWait(input);

        // Note: compareTo returns positive if the left side is greater than the right.
        Assertions.assertThat(output.getTimeToWait().compareTo(Duration.ofHours(12)) > 0).isTrue();
      }

    }

  }

  @Nested
  class TestGetMaxAttempt {

    @Test
    @DisplayName("Test that we are using to right service to get the maximum amount of attempt")
    void testGetMaxAttempt() {
      final int maxAttempt = 15031990;
      configFetchActivity =
          new ConfigFetchActivityImpl(mAirbyteApiClient, maxAttempt, () -> Instant.now().getEpochSecond(), mFeatureFlagClient,
              mScheduleJitterHelper, mFfContextMapper);
      Assertions.assertThat(configFetchActivity.getMaxAttempt().maxAttempt())
          .isEqualTo(maxAttempt);
    }

  }

  @Nested
  class TestGetConnectionContext {

    @BeforeEach
    void setup() {
      when(mAirbyteApiClient.getConnectionApi()).thenReturn(mConnectionApi);

      configFetchActivity = new ConfigFetchActivityImpl(mAirbyteApiClient, SYNC_JOB_MAX_ATTEMPTS, currentSecondsSupplier,
          mFeatureFlagClient, mScheduleJitterHelper, mFfContextMapper);
    }

    @Test
    @DisplayName("Retrieves connection context from server")
    void happyPath() throws IOException {
      final var contextRead = new ConnectionContextRead(
          connectionId,
          UUID.randomUUID(),
          UUID.randomUUID(),
          UUID.randomUUID(),
          UUID.randomUUID(),
          UUID.randomUUID(),
          UUID.randomUUID());

      when(mConnectionApi.getConnectionContext(request))
          .thenReturn(contextRead);

      final var result = configFetchActivity.getConnectionContext(new GetConnectionContextInput(connectionId));
      final var expected = new GetConnectionContextOutput(CommonConvertersKt.toInternal(contextRead));
      assertEquals(expected, result);
    }

    @MockitoSettings(strictness = Strictness.LENIENT)
    @Test
    @DisplayName("Propagates API exception as Retryable")
    void exceptionalPath() throws IOException {
      when(mConnectionApi.getConnectionContext(request))
          .thenThrow(new IOException("bang"));

      assertThrows(RetryableException.class, () -> configFetchActivity.getConnectionContext(new GetConnectionContextInput(connectionId)));
    }

    private static final UUID connectionId = UUID.randomUUID();
    private static final ConnectionIdRequestBody request = new ConnectionIdRequestBody(connectionId);

  }

  @Nested
  class TestLoadShedBackoff {

    @BeforeEach
    void setup() throws IOException {
      when(mFfContextMapper.map(connectionContext)).thenReturn(ffContext);

      configFetchActivity = new ConfigFetchActivityImpl(mAirbyteApiClient, SYNC_JOB_MAX_ATTEMPTS, currentSecondsSupplier,
          mFeatureFlagClient, mScheduleJitterHelper, mFfContextMapper);
    }

    @Test
    @DisplayName("Retrieves connection context from server and returns result of flag checked against the context")
    void happyPath() {
      final var backoff = 15;
      when(mFeatureFlagClient.intVariation(LoadShedSchedulerBackoffMinutes.INSTANCE, ffContext)).thenReturn(backoff);

      final var result = configFetchActivity.getLoadShedBackoff(new GetLoadShedBackoffInput(connectionContext));

      assertEquals(Duration.ofMinutes(backoff), result.getDuration());
    }

    @Test
    @DisplayName("Ensures time to wait is greater than 0")
    void clampLowerBound() {
      final var backoff = -1;
      when(mFeatureFlagClient.intVariation(LoadShedSchedulerBackoffMinutes.INSTANCE, ffContext)).thenReturn(backoff);

      final var result = configFetchActivity.getLoadShedBackoff(new GetLoadShedBackoffInput(connectionContext));

      assertEquals(Duration.ZERO, result.getDuration());
    }

    @Test
    @DisplayName("Ensures time to wait is at most 1 hour")
    void clampUpperBound() {
      final var backoff = 1242;
      when(mFeatureFlagClient.intVariation(LoadShedSchedulerBackoffMinutes.INSTANCE, ffContext)).thenReturn(backoff);

      final var result = configFetchActivity.getLoadShedBackoff(new GetLoadShedBackoffInput(connectionContext));

      assertEquals(Duration.ofHours(1), result.getDuration());
    }

    private static final UUID connectionId = UUID.randomUUID();
    private static final ConnectionIdRequestBody request = new ConnectionIdRequestBody(connectionId);
    private static final ConnectionContext connectionContext = new ConnectionContext()
        .withConnectionId(connectionId)
        .withSourceId(UUID.randomUUID())
        .withDestinationId(UUID.randomUUID())
        .withSourceDefinitionId(UUID.randomUUID())
        .withDestinationDefinitionId(UUID.randomUUID())
        .withWorkspaceId(UUID.randomUUID())
        .withOrganizationId(UUID.randomUUID());
    private static final Context ffContext = new Multi(List.of(new Workspace(UUID.randomUUID()), new Connection(UUID.randomUUID())));

  }

}
