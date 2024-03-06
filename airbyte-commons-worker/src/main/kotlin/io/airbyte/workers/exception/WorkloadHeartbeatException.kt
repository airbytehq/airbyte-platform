package io.airbyte.workers.exception

class WorkloadHeartbeatException(message: String, cause: Throwable? = null) : WorkerException(message, cause) {
  companion object {
    @java.io.Serial
    private const val serialVersionUID: Long = 1L
  }
}
