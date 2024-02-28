package io.airbyte.workers.orchestrator

import io.airbyte.workers.process.AsyncKubePodStatus
import io.airbyte.workers.process.KubeProcessFactory.KUBE_NAME_LEN_LIMIT
import io.airbyte.workers.process.ProcessFactory
import io.airbyte.workers.sync.ReplicationLauncherWorker.POD_NAME_PREFIX
import io.micronaut.context.annotation.Value
import jakarta.inject.Singleton

// TODO: add discrete unit tests — this is indirectly tested from the PayloadKubeInputMapper unit tests.
@Singleton
class PodNameGenerator(
  @Value("\${airbyte.worker.job.kube.namespace}") val namespace: String,
) {
  fun getReplicationOrchestratorPodName(
    jobId: String,
    attemptId: Long,
  ): String {
    return "$REPL_POD_PREFIX-job-$jobId-attempt-$attemptId"
  }

  fun getCheckPodName(
    image: String,
    jobId: String,
    attemptId: Long,
  ): String {
    return ProcessFactory.createProcessName(
      image,
      "check",
      jobId,
      attemptId.toInt(),
      KUBE_NAME_LEN_LIMIT,
    )
  }

  fun getDiscoverPodName(
    image: String,
    jobId: String,
    attemptId: Long,
  ): String {
    return ProcessFactory.createProcessName(
      image,
      "discover",
      jobId,
      attemptId.toInt(),
      KUBE_NAME_LEN_LIMIT,
    )
  }

  fun getSpecPodName(
    image: String,
    jobId: String,
    attemptId: Long,
  ): String {
    return ProcessFactory.createProcessName(
      image,
      "spec",
      jobId,
      attemptId.toInt(),
      KUBE_NAME_LEN_LIMIT,
    )
  }

  fun getOrchestratorOutputLocation(
    jobId: String,
    attemptId: Long,
  ): String {
    return "$namespace/${getReplicationOrchestratorPodName(jobId, attemptId)}/${AsyncKubePodStatus.SUCCEEDED.name}"
  }

  companion object {
    const val REPL_POD_PREFIX = POD_NAME_PREFIX
  }
}
