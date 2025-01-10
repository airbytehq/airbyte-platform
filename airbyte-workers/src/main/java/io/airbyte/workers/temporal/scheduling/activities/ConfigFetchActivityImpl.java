/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.scheduling.activities;

import static io.airbyte.metrics.lib.ApmTraceConstants.ACTIVITY_TRACE_OPERATION_NAME;
import static io.airbyte.metrics.lib.ApmTraceConstants.Tags.CONNECTION_ID_KEY;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.annotations.VisibleForTesting;
import datadog.trace.api.Trace;
import io.airbyte.api.client.AirbyteApiClient;
import io.airbyte.api.client.model.generated.ConnectionIdRequestBody;
import io.airbyte.api.client.model.generated.ConnectionRead;
import io.airbyte.api.client.model.generated.ConnectionSchedule;
import io.airbyte.api.client.model.generated.ConnectionScheduleDataBasicSchedule;
import io.airbyte.api.client.model.generated.ConnectionScheduleDataBasicSchedule.TimeUnit;
import io.airbyte.api.client.model.generated.ConnectionScheduleDataCron;
import io.airbyte.api.client.model.generated.ConnectionScheduleType;
import io.airbyte.api.client.model.generated.ConnectionStatus;
import io.airbyte.api.client.model.generated.JobOptionalRead;
import io.airbyte.api.client.model.generated.JobRead;
import io.airbyte.api.client.model.generated.SourceIdRequestBody;
import io.airbyte.api.client.model.generated.WorkspaceRead;
import io.airbyte.commons.temporal.exception.RetryableException;
import io.airbyte.featureflag.Connection;
import io.airbyte.featureflag.FeatureFlagClient;
import io.airbyte.featureflag.FieldSelectionWorkspaces.AddSchedulingJitter;
import io.airbyte.featureflag.Multi;
import io.airbyte.featureflag.UseNewCronScheduleCalculation;
import io.airbyte.featureflag.Workspace;
import io.airbyte.metrics.lib.ApmTraceUtils;
import io.airbyte.workers.helpers.CronSchedulingHelper;
import io.airbyte.workers.helpers.ScheduleJitterHelper;
import io.micronaut.context.annotation.Value;
import io.micronaut.http.HttpStatus;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.text.ParseException;
import java.time.DateTimeException;
import java.time.Duration;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TimeZone;
import java.util.UUID;
import java.util.function.Supplier;
import org.joda.time.DateTimeZone;
import org.openapitools.client.infrastructure.ClientException;
import org.quartz.CronExpression;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ConfigFetchActivityImpl.
 */
@Singleton
public class ConfigFetchActivityImpl implements ConfigFetchActivity {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final long MS_PER_SECOND = 1000L;
  private static final long MIN_CRON_INTERVAL_SECONDS = 60;
  private static final Set<UUID> SCHEDULING_NOISE_WORKSPACE_IDS = Set.of(
      // Testing
      UUID.fromString("0ace5e1f-4787-43df-8919-456f5f4d03d1"),
      UUID.fromString("20810d92-41a4-4cfd-85db-fb50e77cf36b"),
      // Prod
      UUID.fromString("226edbc1-4a9c-4401-95a9-90435d667d9d"));
  private static final long SCHEDULING_NOISE_CONSTANT = 15;

  private final AirbyteApiClient airbyteApiClient;
  private final Integer syncJobMaxAttempts;
  private final Supplier<Long> currentSecondsSupplier;
  private final FeatureFlagClient featureFlagClient;
  private final ScheduleJitterHelper scheduleJitterHelper;

  @VisibleForTesting
  protected ConfigFetchActivityImpl(final AirbyteApiClient airbyteApiClient,
                                    @Value("${airbyte.worker.sync.max-attempts}") final Integer syncJobMaxAttempts,
                                    @Named("currentSecondsSupplier") final Supplier<Long> currentSecondsSupplier,
                                    final FeatureFlagClient featureFlagClient,
                                    final ScheduleJitterHelper scheduleJitterHelper) {
    this.airbyteApiClient = airbyteApiClient;
    this.syncJobMaxAttempts = syncJobMaxAttempts;
    this.currentSecondsSupplier = currentSecondsSupplier;
    this.featureFlagClient = featureFlagClient;
    this.scheduleJitterHelper = scheduleJitterHelper;
  }

  @Trace(operationName = ACTIVITY_TRACE_OPERATION_NAME)
  @Override
  public ScheduleRetrieverOutput getTimeToWait(final ScheduleRetrieverInput input) {
    try {
      ApmTraceUtils.addTagsToTrace(Map.of(CONNECTION_ID_KEY, input.getConnectionId()));
      final ConnectionIdRequestBody connectionIdRequestBody = new ConnectionIdRequestBody(input.getConnectionId());
      final ConnectionRead connectionRead = airbyteApiClient.getConnectionApi().getConnection(connectionIdRequestBody);
      final UUID workspaceId = airbyteApiClient.getWorkspaceApi().getWorkspaceByConnectionId(connectionIdRequestBody).getWorkspaceId();
      final Duration timeToWait = connectionRead.getScheduleType() != null
          ? getTimeToWaitFromScheduleType(connectionRead, input.getConnectionId(), workspaceId)
          : getTimeToWaitFromLegacy(connectionRead, input.getConnectionId());
      final Duration timeToWaitWithSchedulingJitter =
          applyJitterRules(timeToWait, input.getConnectionId(), connectionRead.getScheduleType(), workspaceId);
      return new ScheduleRetrieverOutput(timeToWaitWithSchedulingJitter);
    } catch (final ClientException e) {
      if (e.getStatusCode() == HttpStatus.NOT_FOUND.getCode()) {
        throw e;
      }
      throw new RetryableException(e);
    } catch (final IOException e) {
      throw new RetryableException(e);
    }
  }

  private Duration applyJitterRules(final Duration timeToWait,
                                    final UUID connectionId,
                                    final ConnectionScheduleType scheduleType,
                                    final UUID workspaceId) {
    if (featureFlagClient.boolVariation(AddSchedulingJitter.INSTANCE, new Multi(List.of(
        new Workspace(workspaceId),
        new Connection(connectionId))))) {
      return scheduleJitterHelper.addJitterBasedOnWaitTime(timeToWait, scheduleType);
    } else {
      return addSchedulingNoiseForAllowListedWorkspace(timeToWait, scheduleType, workspaceId);
    }
  }

  /**
   * Get time to wait from new schedule. This method consumes the `scheduleType` and `scheduleData`
   * fields.
   *
   * @param connectionRead connection read
   * @param connectionId connection id
   * @return time to wait
   * @throws IOException exception while interacting with db
   */
  private Duration getTimeToWaitFromScheduleType(final ConnectionRead connectionRead, final UUID connectionId, final UUID workspaceId)
      throws IOException {
    if (connectionRead.getScheduleType() == ConnectionScheduleType.MANUAL || connectionRead.getStatus() != ConnectionStatus.ACTIVE) {
      // Manual syncs wait for their first run
      return Duration.ofDays(100 * 365);
    }

    final JobOptionalRead previousJobOptional =
        airbyteApiClient.getJobsApi().getLastReplicationJobWithCancel(new ConnectionIdRequestBody(connectionId));

    if (connectionRead.getScheduleType() == ConnectionScheduleType.BASIC) {
      if (previousJobOptional.getJob() == null) {
        // Basic schedules don't wait for their first run.
        return Duration.ZERO;
      }
      final long prevRunStart = previousJobOptional.getJob().getStartedAt() != null ? previousJobOptional.getJob().getStartedAt()
          : previousJobOptional.getJob().getCreatedAt();
      final long nextRunStart = prevRunStart + getIntervalInSecond(connectionRead.getScheduleData().getBasicSchedule());
      final Duration timeToWait = Duration.ofSeconds(
          Math.max(0, nextRunStart - currentSecondsSupplier.get()));
      return timeToWait;
    } else { // connectionRead.getScheduleType() == ConnectionScheduleType.CRON
      final ConnectionScheduleDataCron scheduleCron = connectionRead.getScheduleData().getCron();
      final TimeZone timeZone = DateTimeZone.forID(scheduleCron.getCronTimeZone()).toTimeZone();
      try {
        final CronExpression cronExpression = new CronExpression(scheduleCron.getCronExpression());
        cronExpression.setTimeZone(timeZone);
        if (featureFlagClient.boolVariation(UseNewCronScheduleCalculation.INSTANCE, new Multi(List.of(
            new Workspace(workspaceId),
            new Connection(connectionId))))) {
          return CronSchedulingHelper.getNextRuntimeBasedOnPreviousJobAndSchedule(currentSecondsSupplier, previousJobOptional.getJob(),
              cronExpression);
        } else {
          // Ensure that at least a minimum interval -- one minute -- passes between executions. This prevents
          // us from multiple executions for the same scheduled time, since cron only has a 1-minute
          // resolution.
          final long earliestNextRun = Math.max(currentSecondsSupplier.get() * MS_PER_SECOND,
              (previousJobOptional.getJob() != null
                  ? previousJobOptional.getJob().getStartedAt() != null ? previousJobOptional.getJob().getStartedAt() + MIN_CRON_INTERVAL_SECONDS
                      : previousJobOptional.getJob().getCreatedAt()
                          + MIN_CRON_INTERVAL_SECONDS
                  : currentSecondsSupplier.get()) * MS_PER_SECOND);
          final Date nextRunStart = cronExpression.getNextValidTimeAfter(new Date(earliestNextRun));
          return Duration.ofSeconds(
              Math.max(0, nextRunStart.getTime() / MS_PER_SECOND - currentSecondsSupplier.get()));
        }
      } catch (final ParseException e) {
        throw (DateTimeException) new DateTimeException(e.getMessage()).initCause(e);
      }
    }
  }

  private Duration addSchedulingNoiseForAllowListedWorkspace(final Duration timeToWait,
                                                             final ConnectionScheduleType scheduleType,
                                                             final UUID workspaceId) {
    if (!SCHEDULING_NOISE_WORKSPACE_IDS.contains(workspaceId)) {
      // Only apply to a specific set of workspaces.
      return timeToWait;
    }
    if (!scheduleType.equals(ConnectionScheduleType.CRON)) {
      // Only apply noise to cron connections.
      return timeToWait;
    }

    // We really do want to add some scheduling noise for this connection.
    final long minutesToWait = (long) (Math.random() * SCHEDULING_NOISE_CONSTANT);
    log.debug("Adding {} minutes noise to wait", minutesToWait);
    // Note: we add an extra second to make the unit tests pass in case `minutesToWait` was 0.
    return timeToWait.plusMinutes(minutesToWait).plusSeconds(1);
  }

  /**
   * Get wait time from legacy schedule. This method consumes the `schedule` field.
   *
   * @param connectionRead connection read
   * @param connectionId connection id
   * @return time to wait
   * @throws IOException exception when interacting with the db
   */
  private Duration getTimeToWaitFromLegacy(final ConnectionRead connectionRead, final UUID connectionId)
      throws IOException {
    if (connectionRead.getSchedule() == null || connectionRead.getStatus() != ConnectionStatus.ACTIVE) {
      // Manual syncs wait for their first run
      return Duration.ofDays(100 * 365);
    }

    final JobOptionalRead previousJobOptional =
        airbyteApiClient.getJobsApi().getLastReplicationJobWithCancel(new ConnectionIdRequestBody(connectionId));

    if (previousJobOptional.getJob() == null && connectionRead.getSchedule() != null) {
      // Non-manual syncs don't wait for their first run
      return Duration.ZERO;
    }

    final JobRead previousJob = previousJobOptional.getJob();
    final long prevRunStart = previousJob.getStartedAt() != null ? previousJob.getStartedAt() : previousJob.getCreatedAt();

    final long nextRunStart = prevRunStart + getIntervalInSecond(connectionRead.getSchedule());

    return Duration.ofSeconds(
        Math.max(0, nextRunStart - currentSecondsSupplier.get()));

  }

  @Trace(operationName = ACTIVITY_TRACE_OPERATION_NAME)
  @Override
  public GetMaxAttemptOutput getMaxAttempt() {
    return new GetMaxAttemptOutput(syncJobMaxAttempts);
  }

  @Override
  public Boolean isWorkspaceTombstone(UUID connectionId) {
    try {
      WorkspaceRead workspaceRead =
          airbyteApiClient.getWorkspaceApi().getWorkspaceByConnectionIdWithTombstone(new ConnectionIdRequestBody(connectionId));
      return workspaceRead.getTombstone();
    } catch (final ClientException e) {
      if (e.getStatusCode() == HttpStatus.NOT_FOUND.getCode()) {
        throw e;
      }
      throw new RetryableException(e);
    } catch (final IOException e) {
      log.warn("Fail to get the workspace.", e);
      return false;
    }
  }

  @Override
  public Optional<UUID> getSourceId(final UUID connectionId) {
    try {
      final io.airbyte.api.client.model.generated.ConnectionIdRequestBody requestBody =
          new io.airbyte.api.client.model.generated.ConnectionIdRequestBody(connectionId);
      final ConnectionRead connectionRead = airbyteApiClient.getConnectionApi().getConnection(requestBody);
      return Optional.ofNullable(connectionRead.getSourceId());
    } catch (final ClientException e) {
      if (e.getStatusCode() == HttpStatus.NOT_FOUND.getCode()) {
        throw e;
      }
      throw new RetryableException(e);
    } catch (final IOException e) {
      log.info("Encountered an error fetching the connection's Source ID: ", e);
      return Optional.empty();
    }
  }

  @Override
  public JsonNode getSourceConfig(final UUID sourceId) {
    try {
      return airbyteApiClient.getSourceApi().getSource(new SourceIdRequestBody(sourceId)).getConnectionConfiguration();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Optional<ConnectionStatus> getStatus(final UUID connectionId) {
    try {
      final io.airbyte.api.client.model.generated.ConnectionIdRequestBody requestBody =
          new io.airbyte.api.client.model.generated.ConnectionIdRequestBody(connectionId);
      final ConnectionRead connectionRead = airbyteApiClient.getConnectionApi().getConnection(requestBody);
      return Optional.ofNullable(connectionRead.getStatus());
    } catch (final ClientException e) {
      if (e.getStatusCode() == HttpStatus.NOT_FOUND.getCode()) {
        throw e;
      }
      throw new RetryableException(e);
    } catch (final IOException e) {
      log.info("Encountered an error fetching the connection's status: ", e);
      return Optional.empty();
    }
  }

  private Long getIntervalInSecond(final ConnectionScheduleDataBasicSchedule schedule) {
    return getSecondsInUnit(schedule.getTimeUnit()) * schedule.getUnits();
  }

  private Long getIntervalInSecond(final ConnectionSchedule schedule) {
    return getSecondsInUnit(schedule.getTimeUnit()) * schedule.getUnits();
  }

  private Long getSecondsInUnit(final TimeUnit timeUnitEnum) {
    switch (timeUnitEnum) {
      case MINUTES:
        return java.util.concurrent.TimeUnit.MINUTES.toSeconds(1);
      case HOURS:
        return java.util.concurrent.TimeUnit.HOURS.toSeconds(1);
      case DAYS:
        return java.util.concurrent.TimeUnit.DAYS.toSeconds(1);
      case WEEKS:
        return java.util.concurrent.TimeUnit.DAYS.toSeconds(1) * 7;
      case MONTHS:
        return java.util.concurrent.TimeUnit.DAYS.toSeconds(1) * 30;
      default:
        throw new RuntimeException("Unhandled TimeUnitEnum: " + timeUnitEnum);
    }
  }

  private Long getSecondsInUnit(final ConnectionSchedule.TimeUnit timeUnitEnum) {
    switch (timeUnitEnum) {
      case MINUTES:
        return java.util.concurrent.TimeUnit.MINUTES.toSeconds(1);
      case HOURS:
        return java.util.concurrent.TimeUnit.HOURS.toSeconds(1);
      case DAYS:
        return java.util.concurrent.TimeUnit.DAYS.toSeconds(1);
      case WEEKS:
        return java.util.concurrent.TimeUnit.DAYS.toSeconds(1) * 7;
      case MONTHS:
        return java.util.concurrent.TimeUnit.DAYS.toSeconds(1) * 30;
      default:
        throw new RuntimeException("Unhandled TimeUnitEnum: " + timeUnitEnum);
    }
  }

}
