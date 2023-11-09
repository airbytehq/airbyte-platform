package io.airbyte.workload.launcher.metrics

import io.airbyte.metrics.lib.MetricEmittingApp
import io.airbyte.metrics.lib.MetricEmittingApps
import io.airbyte.metrics.lib.MetricsRegistry

enum class WorkloadLauncherMetricMetadata(
  private val metricName: String,
  private val description: String,
) : MetricsRegistry {
  WORKLOAD_QUEUE_SIZE(
    "workload_queue_size",
    "used to track the queue size launcher does not processes a workload successfully",
  ),
  WORKLOAD_RECEIVED(
    "workload_received",
    "increments when the launcher receives a workload from the queue",
  ),
  WORKLOAD_CLAIMED(
    "workload_claimed",
    "increments when the launcher claims a workload",
  ),
  WORKLOAD_NOT_CLAIMED(
    "workload_not_claimed",
    "increments when the launcher is not able to claim a workload",
  ),
  WORKLOAD_ALREADY_RUNNING(
    "workload_already_running",
    "increments when the launcher claims a workload and finds the that there is a job already running for that workload",
  ),
  WORKLOAD_PROCESSED_ON_RESTART(
    "workload_already_running",
    "increments when the launcher restarts and finds out a workload that was claimed before restart and needs to be processed",
  ),
  WORKLOAD_PROCESSED_SUCCESSFULLY(
    "workload_processed_successfully",
    "increments when the launcher processes a workload successfully",
  ),
  WORKLOAD_PROCESSED_UNSUCCESSFULLY(
    "workload_processed_unsuccessfully",
    "increments when the launcher does not processes a workload successfully",
  ),
  ;

  override fun getApplication(): MetricEmittingApp {
    return MetricEmittingApps.WORKLOAD_LAUNCHER
  }

  override fun getMetricName(): String {
    return metricName
  }

  override fun getMetricDescription(): String {
    return description
  }
}
