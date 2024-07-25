package io.airbyte.workers.exception

class WorkloadMonitorException : RuntimeException {
  constructor(message: String?) : super(message)
}
