/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.scheduling.activities;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.airbyte.api.client.generated.ConnectionApi;
import io.airbyte.api.client.generated.JobsApi;
import io.airbyte.api.client.generated.WorkspaceApi;
import io.airbyte.api.client.invoker.generated.ApiException;
import io.airbyte.api.client.model.generated.ConnectionRead;
import io.airbyte.api.client.model.generated.ConnectionSchedule;
import io.airbyte.api.client.model.generated.ConnectionScheduleData;
import io.airbyte.api.client.model.generated.ConnectionScheduleDataBasicSchedule;
import io.airbyte.api.client.model.generated.ConnectionScheduleDataBasicSchedule.TimeUnitEnum;
import io.airbyte.api.client.model.generated.ConnectionScheduleDataCron;
import io.airbyte.api.client.model.generated.ConnectionScheduleType;
import io.airbyte.api.client.model.generated.ConnectionStatus;
import io.airbyte.api.client.model.generated.JobOptionalRead;
import io.airbyte.api.client.model.generated.JobRead;
import io.airbyte.api.client.model.generated.WorkspaceRead;
import io.airbyte.config.persistence.ConfigNotFoundException;
import io.airbyte.featureflag.FeatureFlagClient;
import io.airbyte.featureflag.TestClient;
import io.airbyte.featureflag.UseNewCronScheduleCalculation;
import io.airbyte.validation.json.JsonValidationException;
import io.airbyte.workers.helpers.CronSchedulingHelper;
import io.airbyte.workers.helpers.ScheduleJitterHelper;
import io.airbyte.workers.temporal.scheduling.activities.ConfigFetchActivity.ScheduleRetrieverInput;
import io.airbyte.workers.temporal.scheduling.activities.ConfigFetchActivity.ScheduleRetrieverOutput;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Calendar;
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

@ExtendWith(MockitoExtension.class)
class ConfigFetchActivityTest {

  private static final Integer SYNC_JOB_MAX_ATTEMPTS = 3;

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

  private FeatureFlagClient mFeatureFlagClient;

  private ConfigFetchActivityImpl configFetchActivity;

  private final Supplier<Long> currentSecondsSupplier = () -> Instant.now().getEpochSecond();

  private static final UUID connectionId = UUID.randomUUID();
  private static final ConnectionRead connectionReadWithLegacySchedule = new ConnectionRead()
      .schedule(new ConnectionSchedule()
          .timeUnit(ConnectionSchedule.TimeUnitEnum.MINUTES)
          .units(5L))
      .status(ConnectionStatus.ACTIVE);

  private static final ConnectionRead connectionReadWithManualScheduleType = new ConnectionRead()
      .scheduleType(ConnectionScheduleType.MANUAL)
      .status(ConnectionStatus.ACTIVE);

  private static final ConnectionRead connectionReadWithBasicScheduleType = new ConnectionRead()
      .scheduleType(ConnectionScheduleType.BASIC)
      .status(ConnectionStatus.ACTIVE)
      .scheduleData(new ConnectionScheduleData()
          .basicSchedule(new ConnectionScheduleDataBasicSchedule()
              .timeUnit(TimeUnitEnum.MINUTES)
              .units(5L)));

  public static final String UTC = "UTC";
  private static final ConnectionRead connectionReadWithCronScheduleType = new ConnectionRead()
      .scheduleType(ConnectionScheduleType.CRON)
      .status(ConnectionStatus.ACTIVE)
      .scheduleData(new ConnectionScheduleData()
          .cron(new ConnectionScheduleDataCron()
              .cronExpression("0 0 12 * * ?")
              .cronTimeZone(UTC)));

  private static final ConnectionRead connectionReadWithScheduleDisable = new ConnectionRead()
      .schedule(new ConnectionSchedule()
          .timeUnit(ConnectionSchedule.TimeUnitEnum.MINUTES)
          .units(5L))
      .status(ConnectionStatus.INACTIVE);

  private static final ConnectionRead connectionReadWithScheduleDeleted = new ConnectionRead()
      .schedule(new ConnectionSchedule()
          .timeUnit(ConnectionSchedule.TimeUnitEnum.MINUTES)
          .units(5L))
      .status(ConnectionStatus.DEPRECATED);
  private static final ConnectionRead connectionReadWithoutSchedule = new ConnectionRead();

  @BeforeEach
  void setup() {
    mFeatureFlagClient = mock(TestClient.class);
  }

  @Nested
  class TimeToWaitTest {

    @BeforeEach
    void setup() throws ApiException {
      when(mWorkspaceApi.getWorkspaceByConnectionId(any())).thenReturn(new WorkspaceRead().workspaceId(UUID.randomUUID()));
    }

    @Nested
    class TestNotCron {

      @BeforeEach
      void setup() {
        configFetchActivity = new ConfigFetchActivityImpl(mJobsApi, mWorkspaceApi, SYNC_JOB_MAX_ATTEMPTS, currentSecondsSupplier, mConnectionApi,
            mFeatureFlagClient, mScheduleJitterHelper);
      }

      @Test
      @DisplayName("Test that the job gets scheduled if it is not manual and if it is the first run with legacy schedule schema")
      void testFirstJobNonManual() throws IOException, JsonValidationException, ConfigNotFoundException, ApiException {
        when(mJobsApi.getLastReplicationJob(any()))
            .thenReturn(new JobOptionalRead());

        when(mConnectionApi.getConnection(any()))
            .thenReturn(connectionReadWithLegacySchedule);

        final ScheduleRetrieverInput input = new ScheduleRetrieverInput(connectionId);

        final ScheduleRetrieverOutput output = configFetchActivity.getTimeToWait(input);

        Assertions.assertThat(output.getTimeToWait())
            .isZero();
      }

      @Test
      @DisplayName("Test that the job will wait for a long time if it is manual in the legacy schedule schema")
      void testManual() throws ApiException {
        when(mConnectionApi.getConnection(any()))
            .thenReturn(connectionReadWithoutSchedule);

        final ScheduleRetrieverInput input = new ScheduleRetrieverInput(connectionId);

        final ScheduleRetrieverOutput output = configFetchActivity.getTimeToWait(input);

        Assertions.assertThat(output.getTimeToWait())
            .hasDays(100 * 365);
      }

      @Test
      @DisplayName("Test that the job will wait for a long time if it is disabled")
      void testDisable() throws ApiException {
        when(mConnectionApi.getConnection(any()))
            .thenReturn(connectionReadWithScheduleDisable);

        final ScheduleRetrieverInput input = new ScheduleRetrieverInput(connectionId);

        final ScheduleRetrieverOutput output = configFetchActivity.getTimeToWait(input);

        Assertions.assertThat(output.getTimeToWait())
            .hasDays(100 * 365);
      }

      @Test
      @DisplayName("Test that the connection will wait for a long time if it is deleted")
      void testDeleted() throws ApiException {
        when(mConnectionApi.getConnection(any()))
            .thenReturn(connectionReadWithScheduleDeleted);

        final ScheduleRetrieverInput input = new ScheduleRetrieverInput(connectionId);

        final ScheduleRetrieverOutput output = configFetchActivity.getTimeToWait(input);

        Assertions.assertThat(output.getTimeToWait())
            .hasDays(100 * 365);
      }

      @Test
      @DisplayName("Test we will wait the required amount of time with legacy config")
      void testWait() throws IOException, JsonValidationException, ConfigNotFoundException, ApiException {
        configFetchActivity =
            new ConfigFetchActivityImpl(mJobsApi, mWorkspaceApi, SYNC_JOB_MAX_ATTEMPTS, () -> 60L * 3, mConnectionApi, mFeatureFlagClient,
                mScheduleJitterHelper);

        when(mJobRead.getStartedAt()).thenReturn(null);
        when(mJobRead.getCreatedAt())
            .thenReturn(60L);

        when(mJobsApi.getLastReplicationJob(any()))
            .thenReturn(new JobOptionalRead().job(mJobRead));

        when(mConnectionApi.getConnection(any()))
            .thenReturn(connectionReadWithLegacySchedule);

        final ScheduleRetrieverInput input = new ScheduleRetrieverInput(connectionId);

        final ScheduleRetrieverOutput output = configFetchActivity.getTimeToWait(input);

        Assertions.assertThat(output.getTimeToWait())
            .hasMinutes(3);
      }

      @Test
      @DisplayName("Test we will not wait if we are late in the legacy schedule schema")
      void testNotWaitIfLate() throws IOException, ApiException {
        configFetchActivity =
            new ConfigFetchActivityImpl(mJobsApi, mWorkspaceApi, SYNC_JOB_MAX_ATTEMPTS, () -> 60L * 10, mConnectionApi, mFeatureFlagClient,
                mScheduleJitterHelper);

        when(mJobRead.getStartedAt()).thenReturn(null);
        when(mJobRead.getCreatedAt())
            .thenReturn(60L);

        when(mJobsApi.getLastReplicationJob(any()))
            .thenReturn(new JobOptionalRead().job(mJobRead));

        when(mConnectionApi.getConnection(any()))
            .thenReturn(connectionReadWithLegacySchedule);

        final ScheduleRetrieverInput input = new ScheduleRetrieverInput(connectionId);

        final ScheduleRetrieverOutput output = configFetchActivity.getTimeToWait(input);

        Assertions.assertThat(output.getTimeToWait())
            .isZero();
      }

      @Test
      @DisplayName("Test that the job will wait a long time if it is MANUAL scheduleType")
      void testManualScheduleType() throws ApiException {
        when(mConnectionApi.getConnection(any()))
            .thenReturn(connectionReadWithManualScheduleType);

        final ScheduleRetrieverInput input = new ScheduleRetrieverInput(connectionId);

        final ScheduleRetrieverOutput output = configFetchActivity.getTimeToWait(input);

        Assertions.assertThat(output.getTimeToWait())
            .hasDays(100 * 365);
      }

      @Test
      @DisplayName("Test that the job will be immediately scheduled if it is a BASIC_SCHEDULE type on the first run")
      void testBasicScheduleTypeFirstRun() throws IOException, ApiException {
        when(mJobsApi.getLastReplicationJob(any()))
            .thenReturn(new JobOptionalRead());

        when(mConnectionApi.getConnection(any()))
            .thenReturn(connectionReadWithBasicScheduleType);

        final ScheduleRetrieverInput input = new ScheduleRetrieverInput(connectionId);

        final ScheduleRetrieverOutput output = configFetchActivity.getTimeToWait(input);

        Assertions.assertThat(output.getTimeToWait())
            .isZero();
      }

      @Test
      @DisplayName("Test that we will wait the required amount of time with a BASIC_SCHEDULE type on a subsequent run")
      void testBasicScheduleSubsequentRun() throws IOException, ApiException {
        configFetchActivity = new ConfigFetchActivityImpl(mJobsApi, mWorkspaceApi, SYNC_JOB_MAX_ATTEMPTS, () -> 60L * 3, mConnectionApi,
            mFeatureFlagClient, mScheduleJitterHelper);

        when(mJobRead.getStartedAt()).thenReturn(null);
        when(mJobRead.getCreatedAt())
            .thenReturn(60L);

        when(mJobsApi.getLastReplicationJob(any()))
            .thenReturn(new JobOptionalRead().job(mJobRead));

        when(mConnectionApi.getConnection(any()))
            .thenReturn(connectionReadWithBasicScheduleType);

        final ScheduleRetrieverInput input = new ScheduleRetrieverInput(connectionId);

        final ScheduleRetrieverOutput output = configFetchActivity.getTimeToWait(input);

        Assertions.assertThat(output.getTimeToWait())
            .hasMinutes(3);
      }

    }

    @Nested
    class TestCronSchedule {

      @Test
      @DisplayName("Test that the job will wait to be scheduled if it is a CRON type, and the prior job ran recently")
      void testCronScheduleSubsequentRunPriorJobRanRecently() throws ApiException {
        final Calendar mockRightNow = Calendar.getInstance(TimeZone.getTimeZone(UTC));
        mockRightNow.set(Calendar.HOUR_OF_DAY, 0);
        mockRightNow.set(Calendar.MINUTE, 0);
        mockRightNow.set(Calendar.SECOND, 0);
        mockRightNow.set(Calendar.MILLISECOND, 0);
        final Supplier<Long> currentSecondsSupplier = () -> mockRightNow.getTimeInMillis() / 1000L;

        configFetchActivity =
            new ConfigFetchActivityImpl(mJobsApi, mWorkspaceApi, SYNC_JOB_MAX_ATTEMPTS,
                currentSecondsSupplier, mConnectionApi, mFeatureFlagClient, mScheduleJitterHelper);

        when(mJobsApi.getLastReplicationJob(any()))
            .thenReturn(new JobOptionalRead().job(mJobRead));

        // prior job completed 3 hours ago, so expect the next job to be scheduled
        // according to the next cron run time.
        final long threeHoursAgoSeconds = currentSecondsSupplier.get() - Duration.ofHours(3).toSeconds();
        when(mJobRead.getStartedAt()).thenReturn(null);
        when(mJobRead.getCreatedAt()).thenReturn(threeHoursAgoSeconds);

        when(mConnectionApi.getConnection(any()))
            .thenReturn(connectionReadWithCronScheduleType);

        final ScheduleRetrieverInput input = new ScheduleRetrieverInput(connectionId);

        final ScheduleRetrieverOutput output = configFetchActivity.getTimeToWait(input);

        Assertions.assertThat(output.getTimeToWait())
            .hasHours(12);
      }

      @Test
      @DisplayName("Test that the job will run immediately if it is CRON type, and the expected interval has elapsed since the prior job")
      void testCronScheduleSubsequentRunPriorJobRanLongAgo() throws ApiException {
        final Calendar mockRightNow = Calendar.getInstance(TimeZone.getTimeZone(UTC));
        mockRightNow.set(Calendar.HOUR_OF_DAY, 0);
        mockRightNow.set(Calendar.MINUTE, 0);
        mockRightNow.set(Calendar.SECOND, 0);
        mockRightNow.set(Calendar.MILLISECOND, 0);
        final Supplier<Long> currentSecondsSupplier = () -> mockRightNow.getTimeInMillis() / 1000L;

        configFetchActivity =
            new ConfigFetchActivityImpl(mJobsApi, mWorkspaceApi, SYNC_JOB_MAX_ATTEMPTS,
                currentSecondsSupplier, mConnectionApi, mFeatureFlagClient, mScheduleJitterHelper);

        when(mJobsApi.getLastReplicationJob(any()))
            .thenReturn(new JobOptionalRead().job(mJobRead));

        // Behavior is currently behind a feature flag
        when(mFeatureFlagClient.boolVariation(Mockito.eq(UseNewCronScheduleCalculation.INSTANCE), any())).thenReturn(true);

        // prior job completed over 24 hours ago, so expect the next job to be scheduled immediately
        final long twentyFiveHoursAgoSeconds = currentSecondsSupplier.get() - Duration.ofHours(25).toSeconds();
        when(mJobRead.getCreatedAt()).thenReturn(twentyFiveHoursAgoSeconds);

        when(mConnectionApi.getConnection(any()))
            .thenReturn(connectionReadWithCronScheduleType);

        final ScheduleRetrieverInput input = new ScheduleRetrieverInput(connectionId);

        final ScheduleRetrieverOutput output = configFetchActivity.getTimeToWait(input);

        Assertions.assertThat(output.getTimeToWait()).isZero();
      }

      @Test
      @DisplayName("Test that the job will only be scheduled once per minimum cron interval")
      void testCronScheduleMinimumInterval() throws IOException, JsonValidationException, ConfigNotFoundException, ApiException {
        final Calendar mockRightNow = Calendar.getInstance(TimeZone.getTimeZone(UTC));
        mockRightNow.set(Calendar.HOUR_OF_DAY, 12);
        mockRightNow.set(Calendar.MINUTE, 0);
        mockRightNow.set(Calendar.SECOND, 0);
        mockRightNow.set(Calendar.MILLISECOND, 0);
        final Supplier<Long> currentSecondsSupplier = () -> mockRightNow.getTimeInMillis() / 1000L;

        configFetchActivity =
            new ConfigFetchActivityImpl(mJobsApi, mWorkspaceApi, SYNC_JOB_MAX_ATTEMPTS,
                currentSecondsSupplier, mConnectionApi, mFeatureFlagClient, mScheduleJitterHelper);

        when(mJobRead.getStartedAt()).thenReturn(null);
        when(mJobRead.getCreatedAt()).thenReturn(mockRightNow.getTimeInMillis() / 1000L);
        when(mJobsApi.getLastReplicationJob(any()))
            .thenReturn(new JobOptionalRead().job(mJobRead));

        when(mConnectionApi.getConnection(any()))
            .thenReturn(connectionReadWithCronScheduleType);

        final ScheduleRetrieverInput input = new ScheduleRetrieverInput(connectionId);

        final ScheduleRetrieverOutput output = configFetchActivity.getTimeToWait(input);

        Assertions.assertThat(output.getTimeToWait())
            .hasHours(24);
      }

      @Test
      @DisplayName("Test that for specific workspace ids, we add some noise in the cron scheduling")
      void testCronSchedulingNoise() throws IOException, JsonValidationException, ConfigNotFoundException, ApiException {
        final Calendar mockRightNow = Calendar.getInstance(TimeZone.getTimeZone(UTC));
        mockRightNow.set(Calendar.HOUR_OF_DAY, 0);
        mockRightNow.set(Calendar.MINUTE, 0);
        mockRightNow.set(Calendar.SECOND, 0);
        mockRightNow.set(Calendar.MILLISECOND, 0);
        final Supplier<Long> currentSecondsSupplier = () -> mockRightNow.getTimeInMillis() / 1000L;

        when(mWorkspaceApi.getWorkspaceByConnectionId(any()))
            .thenReturn(new WorkspaceRead().workspaceId(UUID.fromString("226edbc1-4a9c-4401-95a9-90435d667d9d")));

        configFetchActivity =
            new ConfigFetchActivityImpl(mJobsApi, mWorkspaceApi, SYNC_JOB_MAX_ATTEMPTS,
                currentSecondsSupplier, mConnectionApi, mFeatureFlagClient, mScheduleJitterHelper);

        when(mJobRead.getStartedAt()).thenReturn(null);
        when(mJobRead.getCreatedAt()).thenReturn(mockRightNow.getTimeInMillis() / 1000L);
        when(mJobsApi.getLastReplicationJob(any()))
            .thenReturn(new JobOptionalRead().job(mJobRead));

        when(mConnectionApi.getConnection(any()))
            .thenReturn(connectionReadWithCronScheduleType);

        final ScheduleRetrieverInput input = new ScheduleRetrieverInput(connectionId);

        final ScheduleRetrieverOutput output = configFetchActivity.getTimeToWait(input);

        // Note: compareTo returns positive if the left side is greater than the right.
        Assertions.assertThat(output.getTimeToWait().compareTo(Duration.ofHours(12)) > 0).isTrue();
      }

    }

  }

  @Nested
  class TestGetMaxAttempt {

    @Mock
    private CronSchedulingHelper mCronSchedulingHelper;

    @Test
    @DisplayName("Test that we are using to right service to get the maximum amount of attempt")
    void testGetMaxAttempt() {
      final int maxAttempt = 15031990;
      configFetchActivity =
          new ConfigFetchActivityImpl(mJobsApi, mWorkspaceApi, maxAttempt, () -> Instant.now().getEpochSecond(), mConnectionApi, mFeatureFlagClient,
              mScheduleJitterHelper);
      Assertions.assertThat(configFetchActivity.getMaxAttempt().getMaxAttempt())
          .isEqualTo(maxAttempt);
    }

  }

}
