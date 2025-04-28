/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.config;

import io.airbyte.commons.micronaut.EnvConstants;
import io.airbyte.commons.temporal.TemporalConstants;
import io.airbyte.commons.temporal.utils.PayloadChecker;
import io.airbyte.metrics.MetricClient;
import io.airbyte.workers.temporal.discover.catalog.DiscoverCatalogHelperActivity;
import io.airbyte.workers.temporal.scheduling.activities.AppendToAttemptLogActivity;
import io.airbyte.workers.temporal.scheduling.activities.AutoDisableConnectionActivity;
import io.airbyte.workers.temporal.scheduling.activities.CheckRunProgressActivity;
import io.airbyte.workers.temporal.scheduling.activities.ConfigFetchActivity;
import io.airbyte.workers.temporal.scheduling.activities.FeatureFlagFetchActivity;
import io.airbyte.workers.temporal.scheduling.activities.GenerateInputActivity;
import io.airbyte.workers.temporal.scheduling.activities.JobCreationAndStatusUpdateActivity;
import io.airbyte.workers.temporal.scheduling.activities.RecordMetricActivity;
import io.airbyte.workers.temporal.scheduling.activities.RetryStatePersistenceActivity;
import io.airbyte.workers.temporal.scheduling.activities.StreamResetActivity;
import io.airbyte.workers.temporal.scheduling.activities.WorkflowConfigActivity;
import io.airbyte.workers.temporal.sync.AsyncReplicationActivity;
import io.airbyte.workers.temporal.sync.GenerateReplicationActivityInputActivity;
import io.airbyte.workers.temporal.sync.InvokeOperationsActivity;
import io.airbyte.workers.temporal.sync.ReportRunTimeActivity;
import io.airbyte.workers.temporal.sync.WorkloadStatusCheckActivity;
import io.airbyte.workers.temporal.workflows.ConnectorCommandActivity;
import io.micronaut.context.annotation.Factory;
import io.micronaut.context.annotation.Property;
import io.micronaut.context.annotation.Requires;
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
  @Named("uiCommandsActivities")
  public List<Object> uiCommandsActivities(
                                           final ConnectorCommandActivity connectorCommandActivity) {
    return List.of(connectorCommandActivity);
  }

  @Singleton
  @Requires(env = EnvConstants.CONTROL_PLANE)
  @Named("connectionManagerActivities")
  public List<Object> connectionManagerActivities(
                                                  final GenerateInputActivity generateInputActivity,
                                                  final JobCreationAndStatusUpdateActivity jobCreationAndStatusUpdateActivity,
                                                  final ConfigFetchActivity configFetchActivity,
                                                  final AutoDisableConnectionActivity autoDisableConnectionActivity,
                                                  final StreamResetActivity streamResetActivity,
                                                  final RecordMetricActivity recordMetricActivity,
                                                  final WorkflowConfigActivity workflowConfigActivity,
                                                  final FeatureFlagFetchActivity featureFlagFetchActivity,
                                                  final CheckRunProgressActivity checkRunProgressActivity,
                                                  final RetryStatePersistenceActivity retryStatePersistenceActivity,
                                                  final AppendToAttemptLogActivity appendToAttemptLogActivity) {
    return List.of(generateInputActivity,
        jobCreationAndStatusUpdateActivity,
        configFetchActivity,
        autoDisableConnectionActivity,
        streamResetActivity,
        recordMetricActivity,
        workflowConfigActivity,
        featureFlagFetchActivity,
        checkRunProgressActivity,
        retryStatePersistenceActivity,
        appendToAttemptLogActivity);
  }

  @Singleton
  public PayloadChecker payloadChecker(final MetricClient metricClient) {
    return new PayloadChecker(metricClient);
  }

  @Singleton
  @Named("syncActivities")
  public List<Object> syncActivities(final ConfigFetchActivity configFetchActivity,
                                     final GenerateReplicationActivityInputActivity generateReplicationActivityInputActivity,
                                     final ReportRunTimeActivity reportRunTimeActivity,
                                     final InvokeOperationsActivity invokeOperationsActivity,
                                     final AsyncReplicationActivity asyncReplicationActivity,
                                     final WorkloadStatusCheckActivity workloadStatusCheckActivity,
                                     final DiscoverCatalogHelperActivity discoverCatalogHelperActivity) {
    return List.of(configFetchActivity, generateReplicationActivityInputActivity, reportRunTimeActivity,
        invokeOperationsActivity, asyncReplicationActivity, workloadStatusCheckActivity, discoverCatalogHelperActivity);
  }

  @Singleton
  @Named("asyncActivityOptions")
  public ActivityOptions asyncActivityOptions(@Property(name = "airbyte.activity.async-timeout") final Integer asyncTimeoutSeconds) {
    return ActivityOptions.newBuilder()
        .setStartToCloseTimeout(Duration.ofSeconds(asyncTimeoutSeconds))
        .setCancellationType(ActivityCancellationType.WAIT_CANCELLATION_COMPLETED)
        .setRetryOptions(TemporalConstants.NO_RETRY)
        .build();
  }

  @Singleton
  @Named("workloadStatusCheckActivityOptions")
  public ActivityOptions workloadStatusCheckActivityOptions(@Property(name = "airbyte.activity.async-timeout") final Integer asyncTimeoutSeconds) {
    return ActivityOptions.newBuilder()
        .setStartToCloseTimeout(Duration.ofSeconds(asyncTimeoutSeconds))
        .setCancellationType(ActivityCancellationType.WAIT_CANCELLATION_COMPLETED)
        .setRetryOptions(RetryOptions.newBuilder().setMaximumAttempts(5).build())
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
