/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher

import com.google.common.annotations.VisibleForTesting
import io.airbyte.metrics.lib.ApmTraceUtils
import io.airbyte.workload.launcher.metrics.CustomMetricPublisher
import io.airbyte.workload.launcher.metrics.WorkloadLauncherMetricMetadata
import io.airbyte.workload.launcher.temporal.TemporalWorkerController
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.event.ApplicationEventListener
import io.micronaut.discovery.event.ServiceReadyEvent
import jakarta.inject.Singleton
import kotlin.concurrent.thread

private val logger = KotlinLogging.logger {}

@Singleton
class StartupApplicationEventListener(
  private val claimedProcessor: ClaimedProcessor,
  private val claimProcessorTracker: ClaimProcessorTracker,
  private val customMetricPublisher: CustomMetricPublisher,
  private val temporalWorkerController: TemporalWorkerController,
  private val launcherShutdownHelper: LauncherShutdownHelper,
) : ApplicationEventListener<ServiceReadyEvent> {
  @VisibleForTesting
  var processorThread: Thread? = null
  var trackerThread: Thread? = null

  override fun onApplicationEvent(event: ServiceReadyEvent?) {
    processorThread =
      thread {
        try {
          claimedProcessor.retrieveAndProcess()
        } catch (e: Exception) {
          customMetricPublisher.count(WorkloadLauncherMetricMetadata.WORKLOAD_LAUNCHER_REHYDRATE_FAILURE)
          ApmTraceUtils.addExceptionToTrace(e)
          logger.error(e) { "Failed to retrieve and resume claimed workloads, exiting." }
          launcherShutdownHelper.shutdown(2)
        }
      }

    trackerThread =
      thread {
        claimProcessorTracker.await()
        temporalWorkerController.start()
      }
  }
}
