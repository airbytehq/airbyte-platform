/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher.config

import io.airbyte.commons.temporal.TemporalConstants
import io.airbyte.commons.temporal.queue.QueueActivity
import io.airbyte.commons.temporal.queue.QueueActivityImpl
import io.airbyte.config.messages.LauncherInputMessage
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.featureflag.Geography
import io.airbyte.featureflag.Multi
import io.airbyte.featureflag.PlaneName
import io.airbyte.featureflag.Priority
import io.airbyte.featureflag.Priority.Companion.HIGH_PRIORITY
import io.airbyte.featureflag.WorkloadApiRouting
import io.airbyte.workload.launcher.pipeline.consumer.LauncherMessageConsumer
import io.micronaut.context.annotation.Factory
import io.micronaut.context.annotation.Property
import io.temporal.activity.ActivityOptions
import jakarta.inject.Named
import jakarta.inject.Singleton
import java.time.Duration

@Factory
class TemporalBeanFactory {
  @Singleton
  @Named("queueActivityOptions")
  fun queueActivityOptions(
    @Property(name = "airbyte.workload-launcher.workload-start-timeout") workloadStartTimeout: Duration,
  ): ActivityOptions =
    ActivityOptions
      .newBuilder()
      .setScheduleToCloseTimeout(workloadStartTimeout)
      .setRetryOptions(TemporalConstants.NO_RETRY)
      .build()

  @Singleton
  @Named("starterActivities")
  fun workloadStarterActivities(launcherMessageConsumer: LauncherMessageConsumer): QueueActivity<LauncherInputMessage> =
    QueueActivityImpl(launcherMessageConsumer)

  @Named("workloadLauncherQueue")
  @Singleton
  fun launcherQueue(
    featureFlagClient: FeatureFlagClient,
    @Property(name = "airbyte.workload-launcher.geography") geography: String,
    @Property(name = "airbyte.data-plane-name") dataPlaneName: String?,
  ): String {
    val context =
      if (dataPlaneName.isNullOrBlank()) {
        Geography(geography)
      } else {
        Multi(listOf(Geography(geography), PlaneName(dataPlaneName)))
      }
    return featureFlagClient.stringVariation(WorkloadApiRouting, context)
  }

  @Named("workloadLauncherHighPriorityQueue")
  @Singleton
  fun highPriorityLauncherQueue(
    featureFlagClient: FeatureFlagClient,
    @Property(name = "airbyte.workload-launcher.geography") geography: String,
    @Property(name = "airbyte.data-plane-name") dataPlaneName: String?,
  ): String {
    val context =
      if (dataPlaneName.isNullOrBlank()) {
        Multi(listOf(Geography(geography), Priority(HIGH_PRIORITY)))
      } else {
        Multi(listOf(Geography(geography), Priority(HIGH_PRIORITY), PlaneName(dataPlaneName)))
      }
    return featureFlagClient.stringVariation(WorkloadApiRouting, context)
  }
}
