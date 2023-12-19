package io.airbyte.workload.launcher.metrics

import io.airbyte.metrics.lib.MetricEmittingApp
import io.airbyte.metrics.lib.MetricEmittingApps
import io.airbyte.metrics.lib.MetricsRegistry

enum class WorkloadLauncherMetricMetadata(
  private val metricName: String,
  private val description: String,
) : MetricsRegistry {
  WORKLOAD_LAUNCHER_KUBERNETES_RESOURCE_MONITOR_START(
    "workload_launcher_kubernetes_resource_monitor_start",
    "increments when the Kube resource monitor starts",
  ),
  WORKLOAD_LAUNCHER_KUBERNETES_RESOURCE_MONITOR_RUN(
    "workload_launcher_kubernetes_resource_monitor_run",
    "increments when the Kube resource monitor ends",
  ),
  WORKLOAD_LAUNCHER_POLLER_STATUS(
    "workload_launcher_poller_status",
    "tracks the status of the workload task poller",
  ),
  WORKLOAD_QUEUE_SIZE(
    "workload_queue_size",
    "used to track the queue size launcher does not processes a workload successfully",
  ),
  WORKLOAD_RECEIVED(
    "workload_received",
    "increments when the launcher receives a workload from the queue",
  ),
  WORKLOAD_LAUNCH_DURATION(
    "workload_launch_duration",
    "tracks the duration of the launch of a workload",
  ),
  WORKLOAD_CLAIM_RESUMED(
    "workload_claim_resumed",
    "increments when a claimed workload is retrieved and processed on startup",
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
    "workload_claims_rehydrated",
    "increments when the launcher restarts and finds out a workload that was claimed before restart and needs to be processed",
  ),
  WORKLOAD_PROCESSED(
    "workload_processed",
    "increments when the launcher processes a workload",
  ),
  WORKLOAD_STAGE_START(
    "workload_stage_start",
    "increments when a workload stage starts",
  ),
  WORKLOAD_STAGE_DONE(
    "workload_stage_done",
    "increments when a workload stage is done",
  ),
  WORKLOAD_STAGE_DURATION(
    "workload_stage_duration",
    "tracks the duration of a workload stage",
  ),
  PODS_DELETED_FOR_MUTEX_KEY(
    "workload_pods_deleted_for_mutex_key",
    "existing pods for the provided mutex key were found and deleted",
  ),
  TOTAL_PENDING_PODS(
    "workload_pods_pending",
    "number of pending pods started by the launcher",
  ),
  OLDEST_PENDING_JOB_POD_TIME(
    "workload_pods_oldest_pending_time",
    "the time of the oldest pending job (in seconds)",
  ),
  PRODUCER_TO_CONSUMER_LATENCY_MS(
    "producer_to_consumer_start_latency_ms",
    "the time it takes to produce a message until it is consumed",
  ),
  PRODUCER_TO_POD_STARTED_LATENCY_MS(
    "producer_to_pod_started_latency_ms",
    "the time it takes to produce a message until it is fully processed",
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
