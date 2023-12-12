package io.airbyte.workers.orchestrator

import io.airbyte.workers.process.AsyncKubePodStatus
import io.airbyte.workers.sync.ReplicationLauncherWorker.POD_NAME_PREFIX
import io.micronaut.context.annotation.Value
import jakarta.inject.Singleton

@Singleton
class OrchestratorNameGenerator(
  @Value("\${airbyte.worker.job.kube.namespace}") private val namespace: String,
) {
  fun getOrchestratorPodName(
    jobId: String,
    attemptId: Long,
  ): String {
    return "$POD_NAME_PREFIX-job-$jobId-attempt-$attemptId"
  }

  fun getOrchestratorOutputLocation(
    jobId: String,
    attemptId: Long,
  ): String {
    return "$namespace/${getOrchestratorPodName(jobId, attemptId)}/${AsyncKubePodStatus.SUCCEEDED.name}"
  }
}
