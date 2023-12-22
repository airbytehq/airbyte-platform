package io.airbyte.workers.orchestrator

import io.airbyte.workers.process.AsyncKubePodStatus
import io.airbyte.workers.sync.ReplicationLauncherWorker.POD_NAME_PREFIX
import io.micronaut.context.annotation.Value
import jakarta.inject.Singleton

// TODO: add discrete unit tests — this is indirectly tested from the PayloadKubeInputMapper unit tests.
@Singleton
class OrchestratorNameGenerator(
  @Value("\${airbyte.worker.job.kube.namespace}") private val namespace: String,
) {
  fun getReplicationOrchestratorPodName(
    jobId: String,
    attemptId: Long,
  ): String {
    return "$REPL_POD_PREFIX-job-$jobId-attempt-$attemptId"
  }

  fun getCheckOrchestratorPodName(
    jobId: String,
    attemptId: Long,
  ): String {
    return "$CHECK_POD_PREFIX-job-$jobId-attempt-$attemptId"
  }

  fun getOrchestratorOutputLocation(
    jobId: String,
    attemptId: Long,
  ): String {
    return "$namespace/${getReplicationOrchestratorPodName(jobId, attemptId)}/${AsyncKubePodStatus.SUCCEEDED.name}"
  }

  companion object {
    const val REPL_POD_PREFIX = POD_NAME_PREFIX
    const val CHECK_POD_PREFIX = "orchestrator-check"
  }
}
