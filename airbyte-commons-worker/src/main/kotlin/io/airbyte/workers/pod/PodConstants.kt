package io.airbyte.workers.pod

object PodConstants {
  const val KUBE_NAME_LEN_LIMIT = 63
  const val REPL_POD_PREFIX = "replication"
  const val REPLICATION_APPLICATION_NAME = "replication-orchestrator"
  const val NOOP_APPLICATION_NAME = "replication-orchestrator"
  const val NO_OP_APPLICATION_NAME = "NO_OP"
}
