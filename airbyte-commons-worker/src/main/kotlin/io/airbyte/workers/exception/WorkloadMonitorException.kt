package io.airbyte.workers.exception

class WorkloadMonitorException(
  message: String?,
) : RuntimeException(message)
