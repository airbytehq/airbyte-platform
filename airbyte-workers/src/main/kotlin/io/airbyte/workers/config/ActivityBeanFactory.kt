/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.config

import io.airbyte.commons.micronaut.EnvConstants
import io.airbyte.commons.temporal.TemporalConstants
import io.airbyte.commons.temporal.utils.PayloadChecker
import io.airbyte.metrics.MetricClient
import io.airbyte.workers.temporal.discover.catalog.DiscoverCatalogHelperActivity
import io.airbyte.workers.temporal.scheduling.activities.AppendToAttemptLogActivity
import io.airbyte.workers.temporal.scheduling.activities.AutoDisableConnectionActivity
import io.airbyte.workers.temporal.scheduling.activities.CheckRunProgressActivity
import io.airbyte.workers.temporal.scheduling.activities.ConfigFetchActivity
import io.airbyte.workers.temporal.scheduling.activities.FeatureFlagFetchActivity
import io.airbyte.workers.temporal.scheduling.activities.JobCreationAndStatusUpdateActivity
import io.airbyte.workers.temporal.scheduling.activities.JobPostProcessingActivity
import io.airbyte.workers.temporal.scheduling.activities.RecordMetricActivity
import io.airbyte.workers.temporal.scheduling.activities.RetryStatePersistenceActivity
import io.airbyte.workers.temporal.scheduling.activities.StreamResetActivity
import io.airbyte.workers.temporal.scheduling.activities.WorkflowConfigActivity
import io.airbyte.workers.temporal.sync.InvokeOperationsActivity
import io.airbyte.workers.temporal.workflows.ConnectorCommandActivity
import io.micronaut.context.annotation.Factory
import io.micronaut.context.annotation.Property
import io.micronaut.context.annotation.Requires
import io.temporal.activity.ActivityCancellationType
import io.temporal.activity.ActivityOptions
import io.temporal.common.RetryOptions
import jakarta.inject.Named
import jakarta.inject.Singleton
import java.time.Duration

/**
 * Micronaut bean factory for activity-related singletons.
 */
@Factory
class ActivityBeanFactory {
  @Singleton
  @Named("uiCommandsActivities")
  fun uiCommandsActivities(connectorCommandActivity: ConnectorCommandActivity): List<Any> = listOf<Any>(connectorCommandActivity)

  @Singleton
  @Requires(env = [EnvConstants.CONTROL_PLANE])
  @Named("connectionManagerActivities")
  fun connectionManagerActivities(
    jobCreationAndStatusUpdateActivity: JobCreationAndStatusUpdateActivity,
    configFetchActivity: ConfigFetchActivity,
    autoDisableConnectionActivity: AutoDisableConnectionActivity,
    streamResetActivity: StreamResetActivity,
    recordMetricActivity: RecordMetricActivity,
    workflowConfigActivity: WorkflowConfigActivity,
    featureFlagFetchActivity: FeatureFlagFetchActivity,
    checkRunProgressActivity: CheckRunProgressActivity,
    retryStatePersistenceActivity: RetryStatePersistenceActivity,
    appendToAttemptLogActivity: AppendToAttemptLogActivity,
  ): List<Any> =
    listOf(
      jobCreationAndStatusUpdateActivity,
      configFetchActivity,
      autoDisableConnectionActivity,
      streamResetActivity,
      recordMetricActivity,
      workflowConfigActivity,
      featureFlagFetchActivity,
      checkRunProgressActivity,
      retryStatePersistenceActivity,
      appendToAttemptLogActivity,
    )

  @Singleton
  @Named("jobPostProcessingActivities")
  fun jobPostProcessingActivities(jobPostProcessingActivity: JobPostProcessingActivity): List<Any> = listOf(jobPostProcessingActivity)

  @Singleton
  fun payloadChecker(metricClient: MetricClient): PayloadChecker = PayloadChecker(metricClient)

  @Singleton
  @Named("syncActivities")
  fun syncActivities(
    configFetchActivity: ConfigFetchActivity,
    invokeOperationsActivity: InvokeOperationsActivity,
    discoverCatalogHelperActivity: DiscoverCatalogHelperActivity,
  ): List<Any> =
    listOf(
      configFetchActivity,
      invokeOperationsActivity,
      discoverCatalogHelperActivity,
    )

  @Singleton
  @Named("shortActivityOptions")
  fun shortActivityOptions(
    @Property(name = "airbyte.activity.max-timeout", defaultValue = "120") maxTimeout: Long,
    @Named("shortRetryOptions") shortRetryOptions: RetryOptions?,
  ): ActivityOptions =
    ActivityOptions
      .newBuilder()
      .setStartToCloseTimeout(Duration.ofSeconds(maxTimeout))
      .setCancellationType(ActivityCancellationType.WAIT_CANCELLATION_COMPLETED)
      .setRetryOptions(shortRetryOptions)
      .setHeartbeatTimeout(TemporalConstants.HEARTBEAT_TIMEOUT)
      .build()

  @Singleton
  @Named("shortRetryOptions")
  fun shortRetryOptions(
    @Property(name = "airbyte.activity.max-attempts", defaultValue = "5") activityNumberOfAttempts: Int,
    @Property(name = "airbyte.activity.initial-delay", defaultValue = "30") initialDelayBetweenActivityAttemptsSeconds: Int,
    @Property(name = "airbyte.activity.max-delay", defaultValue = "600") maxDelayBetweenActivityAttemptsSeconds: Int,
  ): RetryOptions =
    RetryOptions
      .newBuilder()
      .setMaximumAttempts(activityNumberOfAttempts)
      .setInitialInterval(Duration.ofSeconds(initialDelayBetweenActivityAttemptsSeconds.toLong()))
      .setMaximumInterval(Duration.ofSeconds(maxDelayBetweenActivityAttemptsSeconds.toLong()))
      .build()
}
