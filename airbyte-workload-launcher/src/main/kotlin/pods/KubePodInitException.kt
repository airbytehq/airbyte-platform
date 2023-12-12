package io.airbyte.workload.launcher.pods

class KubePodInitException(
  override val message: String,
  override val cause: Throwable,
) : Throwable(message, cause)
