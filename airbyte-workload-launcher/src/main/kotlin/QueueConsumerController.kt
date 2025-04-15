/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher

import io.airbyte.workload.launcher.model.DataplaneConfig
import io.airbyte.workload.launcher.pipeline.consumer.WorkloadApiQueueConsumer
import io.micronaut.context.event.ApplicationEventListener
import jakarta.inject.Singleton
import java.util.concurrent.atomic.AtomicBoolean

@Singleton
class QueueConsumerController(
  private val workloadApiQueueConsumer: WorkloadApiQueueConsumer,
) : ApplicationEventListener<DataplaneConfig> {
  private val started: AtomicBoolean = AtomicBoolean(false)
  private var currentDataplaneConfig: DataplaneConfig? = null
  private var pollersConsuming: Boolean? = null

  fun start() {
    started.set(true)

    updateEnabledStatus()
  }

  override fun onApplicationEvent(event: DataplaneConfig) {
    if (currentDataplaneConfig == null) {
      workloadApiQueueConsumer.initialize(event.dataplaneGroupId.toString())
    }
    currentDataplaneConfig = event
    updateEnabledStatus()
  }

  private fun updateEnabledStatus() {
    // If the Controller hasn't been started, we shouldn't consume anything yet.
    // The launcher is either initializing or resuming claims.
    // Same if we do not have a current config, there isn't anything to do yet.
    if (!started.get() || currentDataplaneConfig == null) {
      return
    }

    val shouldPollerConsume = currentDataplaneConfig?.dataplaneEnabled ?: false
    if (shouldPollerConsume != pollersConsuming) {
      if (shouldPollerConsume) {
        workloadApiQueueConsumer.resumePolling()
      } else {
        workloadApiQueueConsumer.suspendPolling()
      }
      pollersConsuming = shouldPollerConsume
    }
  }
}
