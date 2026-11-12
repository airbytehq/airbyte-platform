/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher.diagnostics

import io.airbyte.micronaut.runtime.AirbyteWorkerConfig
import io.airbyte.workers.pod.Metadata.CHECK_JOB
import io.airbyte.workers.pod.Metadata.DISCOVER_JOB
import io.airbyte.workers.pod.Metadata.JOB_TYPE_KEY
import io.airbyte.workers.pod.Metadata.SYNC_JOB
import io.airbyte.workload.launcher.client.KubernetesClientWrapper
import io.fabric8.kubernetes.api.model.Pod
import jakarta.inject.Singleton

/**
 * Lists the pods of sync, check and discover workloads in the jobs namespace, in any phase.
 */
@Singleton
class WorkloadPodLister(
  private val kubernetesClient: KubernetesClientWrapper,
  private val airbyteWorkerConfig: AirbyteWorkerConfig,
) {
  /**
   * Throws [io.fabric8.kubernetes.client.KubernetesClientException] when the pods can't be listed.
   */
  fun list(): List<Pod> =
    kubernetesClient
      .listJobPods(airbyteWorkerConfig.job.kubernetes.namespace) { it.withLabelIn(JOB_TYPE_KEY, SYNC_JOB, CHECK_JOB, DISCOVER_JOB) }
      .items
}
