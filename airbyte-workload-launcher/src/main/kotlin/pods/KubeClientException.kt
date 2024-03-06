package io.airbyte.workload.launcher.pods

class KubeClientException(
  override val message: String,
  override val cause: Throwable,
  val commandType: KubeCommandType,
  val podType: PodType? = null,
) : Throwable(message, cause)

enum class PodType {
  ORCHESTRATOR,
  SOURCE,
  DESTINATION,
}

enum class KubeCommandType {
  WAIT_INIT,
  WAIT_MAIN,
  COPY,
  DELETE,
  CREATE,
}
