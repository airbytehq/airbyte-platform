package io.airbyte.workers.exception

class WorkloadLauncherException : RuntimeException {
  constructor(message: String?) : super(message)
}
