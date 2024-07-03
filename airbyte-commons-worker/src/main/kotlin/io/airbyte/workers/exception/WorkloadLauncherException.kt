package io.airbyte.workers.exception

class WorkloadLauncherException : RuntimeException {
  constructor(message: String?) : super(message)

  constructor(message: String?, cause: Throwable?) : super(message, cause)
}
