package io.airbyte.workers.exception

class WorkloadLauncherException(
  message: String?,
) : RuntimeException(message)
