/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher

import com.google.common.annotations.VisibleForTesting
import io.airbyte.metrics.MetricClient
import io.airbyte.metrics.OssMetricsRegistry
import io.airbyte.metrics.lib.ApmTraceUtils
import io.airbyte.workload.launcher.authn.DataplaneIdentityService
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
  private val metricClient: MetricClient,
  private val queueConsumerController: QueueConsumerController,
  private val launcherShutdownHelper: LauncherShutdownHelper,
  private val identityService: DataplaneIdentityService,
) : ApplicationEventListener<ServiceReadyEvent> {
  @VisibleForTesting
  var processorThread: Thread? = null
  var trackerThread: Thread? = null

  override fun onApplicationEvent(event: ServiceReadyEvent?) {
    identityService.initialize()

    processorThread =
      thread {
        try {
          claimedProcessor.retrieveAndProcess(identityService.getDataplaneId())
        } catch (e: Exception) {
          metricClient.count(metric = OssMetricsRegistry.WORKLOAD_LAUNCHER_REHYDRATE_FAILURE)
          ApmTraceUtils.addExceptionToTrace(e)
          logger.error(e) { "Failed to retrieve and resume claimed workloads, exiting." }
          launcherShutdownHelper.shutdown(2)
        }
      }

    trackerThread =
      thread {
        claimProcessorTracker.await()
        queueConsumerController.start()
      }
  }
}
