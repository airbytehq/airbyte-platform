/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.config;

import io.airbyte.commons.temporal.TemporalConstants;
import io.airbyte.commons.temporal.config.WorkerMode;
import io.airbyte.workers.exception.WorkerException;
import io.airbyte.workers.temporal.check.connection.CheckConnectionActivity;
import io.airbyte.workers.temporal.discover.catalog.DiscoverCatalogActivity;
import io.airbyte.workers.temporal.scheduling.activities.AppendToAttemptLogActivity;
import io.airbyte.workers.temporal.scheduling.activities.AutoDisableConnectionActivity;
import io.airbyte.workers.temporal.scheduling.activities.CheckRunProgressActivity;
import io.airbyte.workers.temporal.scheduling.activities.ConfigFetchActivity;
import io.airbyte.workers.temporal.scheduling.activities.FeatureFlagFetchActivity;
import io.airbyte.workers.temporal.scheduling.activities.GenerateInputActivity;
import io.airbyte.workers.temporal.scheduling.activities.JobCreationAndStatusUpdateActivity;
import io.airbyte.workers.temporal.scheduling.activities.NotifyActivity;
import io.airbyte.workers.temporal.scheduling.activities.RecordMetricActivity;
import io.airbyte.workers.temporal.scheduling.activities.RetryStatePersistenceActivity;
import io.airbyte.workers.temporal.scheduling.activities.RouteToSyncTaskQueueActivity;
import io.airbyte.workers.temporal.scheduling.activities.StreamResetActivity;
import io.airbyte.workers.temporal.scheduling.activities.WorkflowConfigActivity;
import io.airbyte.workers.temporal.spec.SpecActivity;
import io.airbyte.workers.temporal.sync.DbtTransformationActivity;
import io.airbyte.workers.temporal.sync.NormalizationActivity;
import io.airbyte.workers.temporal.sync.NormalizationSummaryCheckActivity;
import io.airbyte.workers.temporal.sync.RefreshSchemaActivity;
import io.airbyte.workers.temporal.sync.ReplicationActivity;
import io.airbyte.workers.temporal.sync.WebhookOperationActivity;
import io.airbyte.workers.temporal.sync.WorkloadFeatureFlagActivity;
import io.micronaut.context.annotation.Factory;
import io.micronaut.context.annotation.Property;
import io.micronaut.context.annotation.Requires;
import io.micronaut.context.annotation.Value;
import io.temporal.activity.ActivityCancellationType;
import io.temporal.activity.ActivityOptions;
import io.temporal.common.RetryOptions;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import java.time.Duration;
import java.util.List;

/**
 * Micronaut bean factory for activity-related singletons.
 */
@Factory
@SuppressWarnings("PMD.AvoidDuplicateLiterals")
public class ActivityBeanFactory {

  @Singleton
  @Named("checkConnectionActivities")
  public List<Object> checkConnectionActivities(
                                                final CheckConnectionActivity checkConnectionActivity) {
    return List.of(checkConnectionActivity);
  }

  @Singleton
  @Requires(env = WorkerMode.CONTROL_PLANE)
  @Named("connectionManagerActivities")
  public List<Object> connectionManagerActivities(
                                                  final GenerateInputActivity generateInputActivity,
                                                  final JobCreationAndStatusUpdateActivity jobCreationAndStatusUpdateActivity,
                                                  final ConfigFetchActivity configFetchActivity,
                                                  final CheckConnectionActivity checkConnectionActivity,
                                                  final AutoDisableConnectionActivity autoDisableConnectionActivity,
                                                  final StreamResetActivity streamResetActivity,
                                                  final RecordMetricActivity recordMetricActivity,
                                                  final WorkflowConfigActivity workflowConfigActivity,
                                                  final RouteToSyncTaskQueueActivity routeToTaskQueueActivity,
                                                  final FeatureFlagFetchActivity featureFlagFetchActivity,
                                                  final CheckRunProgressActivity checkRunProgressActivity,
                                                  final RetryStatePersistenceActivity retryStatePersistenceActivity,
                                                  final AppendToAttemptLogActivity appendToAttemptLogActivity) {
    return List.of(generateInputActivity,
        jobCreationAndStatusUpdateActivity,
        configFetchActivity,
        checkConnectionActivity,
        autoDisableConnectionActivity,
        streamResetActivity,
        recordMetricActivity,
        workflowConfigActivity,
        routeToTaskQueueActivity,
        featureFlagFetchActivity,
        checkRunProgressActivity,
        retryStatePersistenceActivity,
        appendToAttemptLogActivity);
  }

  @Singleton
  @Named("discoverActivities")
  public List<Object> discoverActivities(
                                         final DiscoverCatalogActivity discoverCatalogActivity) {
    return List.of(discoverCatalogActivity);
  }

  @Singleton
  @Requires(env = WorkerMode.CONTROL_PLANE)
  @Named("specActivities")
  public List<Object> specActivities(
                                     final SpecActivity specActivity) {
    return List.of(specActivity);
  }

  @Singleton
  @Named("syncActivities")
  public List<Object> syncActivities(
                                     final ReplicationActivity replicationActivity,
                                     final NormalizationActivity normalizationActivity,
                                     final DbtTransformationActivity dbtTransformationActivity,
                                     final NormalizationSummaryCheckActivity normalizationSummaryCheckActivity,
                                     final WebhookOperationActivity webhookOperationActivity,
                                     final ConfigFetchActivity configFetchActivity,
                                     final RefreshSchemaActivity refreshSchemaActivity,
                                     final WorkloadFeatureFlagActivity workloadFeatureFlagActivity) {
    return List.of(replicationActivity, normalizationActivity, dbtTransformationActivity, normalizationSummaryCheckActivity,
        webhookOperationActivity, configFetchActivity, refreshSchemaActivity, workloadFeatureFlagActivity);
  }

  @Singleton
  @Requires(env = WorkerMode.CONTROL_PLANE)
  @Named("notificationActivities")
  public List<Object> notificationActivities(
                                             final NotifyActivity notifyActivity) {
    return List.of(notifyActivity);
  }

  @Singleton
  @Named("checkActivityOptions")
  public ActivityOptions checkActivityOptions(@Property(name = "airbyte.activity.check-timeout",
                                                        defaultValue = "5") final Integer checkTimeoutMinutes) {
    return ActivityOptions.newBuilder()
        .setScheduleToCloseTimeout(Duration.ofMinutes(checkTimeoutMinutes))
        .setRetryOptions(TemporalConstants.NO_RETRY)
        .build();
  }

  @Singleton
  @Named("discoveryActivityOptions")
  public ActivityOptions discoveryActivityOptions(@Property(name = "airbyte.activity.discovery-timeout",
                                                            defaultValue = "30") final Integer discoveryTimeoutMinutes) {
    return ActivityOptions.newBuilder()
        .setScheduleToCloseTimeout(Duration.ofMinutes(discoveryTimeoutMinutes))
        .setRetryOptions(TemporalConstants.NO_RETRY)
        .build();
  }

  @Singleton
  @Named("refreshSchemaActivityOptions")
  public ActivityOptions refreshSchemaActivityOptions() {
    return ActivityOptions.newBuilder()
        .setScheduleToCloseTimeout(Duration.ofMinutes(30))
        .setRetryOptions(TemporalConstants.NO_RETRY)
        .build();
  }

  @Singleton
  @Named("longRunActivityOptions")
  public ActivityOptions longRunActivityOptions(
                                                @Value("${airbyte.worker.sync.max-timeout}") final Long maxTimeout,
                                                @Named("longRunActivityRetryOptions") final RetryOptions retryOptions) {
    return ActivityOptions.newBuilder()
        .setScheduleToCloseTimeout(Duration.ofDays(maxTimeout))
        .setStartToCloseTimeout(Duration.ofDays(maxTimeout))
        .setScheduleToStartTimeout(Duration.ofDays(maxTimeout))
        .setCancellationType(ActivityCancellationType.WAIT_CANCELLATION_COMPLETED)
        .setRetryOptions(retryOptions)
        .setHeartbeatTimeout(TemporalConstants.HEARTBEAT_TIMEOUT)
        .build();
  }

  @Singleton
  @Named("shortActivityOptions")
  public ActivityOptions shortActivityOptions(@Property(name = "airbyte.activity.max-timeout",
                                                        defaultValue = "120") final Long maxTimeout,
                                              @Named("shortRetryOptions") final RetryOptions shortRetryOptions) {
    return ActivityOptions.newBuilder()
        .setStartToCloseTimeout(Duration.ofSeconds(maxTimeout))
        .setCancellationType(ActivityCancellationType.WAIT_CANCELLATION_COMPLETED)
        .setRetryOptions(shortRetryOptions)
        .setHeartbeatTimeout(TemporalConstants.HEARTBEAT_TIMEOUT)
        .build();
  }

  @Singleton
  @Requires(env = WorkerMode.CONTROL_PLANE)
  @Named("specActivityOptions")
  public ActivityOptions specActivityOptions() {
    return ActivityOptions.newBuilder()
        .setScheduleToCloseTimeout(Duration.ofHours(1))
        .setRetryOptions(TemporalConstants.NO_RETRY)
        .build();
  }

  @Singleton
  @Requires(property = "airbyte.container.orchestrator.enabled",
            value = "true")
  @Named("longRunActivityRetryOptions")
  public RetryOptions containerOrchestratorRetryOptions() {
    return RetryOptions.newBuilder()
        .setDoNotRetry(RuntimeException.class.getName(), WorkerException.class.getName())
        .build();
  }

  @Singleton
  @Requires(property = "airbyte.container.orchestrator.enabled",
            notEquals = "true")
  @Named("longRunActivityRetryOptions")
  public RetryOptions noRetryOptions() {
    return TemporalConstants.NO_RETRY;
  }

  @Singleton
  @Named("shortRetryOptions")
  public RetryOptions shortRetryOptions(@Property(name = "airbyte.activity.max-attempts",
                                                  defaultValue = "5") final Integer activityNumberOfAttempts,
                                        @Property(name = "airbyte.activity.initial-delay",
                                                  defaultValue = "30") final Integer initialDelayBetweenActivityAttemptsSeconds,
                                        @Property(name = "airbyte.activity.max-delay",
                                                  defaultValue = "600") final Integer maxDelayBetweenActivityAttemptsSeconds) {
    return RetryOptions.newBuilder()
        .setMaximumAttempts(activityNumberOfAttempts)
        .setInitialInterval(Duration.ofSeconds(initialDelayBetweenActivityAttemptsSeconds))
        .setMaximumInterval(Duration.ofSeconds(maxDelayBetweenActivityAttemptsSeconds))
        .build();
  }

}
