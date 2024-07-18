package io.airbyte.workers.exception

class WorkloadMonitorException : RuntimeException {
  constructor(message: String?) : super(message)

  constructor(message: String?, cause: Throwable?) : super(message, cause)
}
