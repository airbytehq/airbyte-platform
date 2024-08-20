package io.airbyte.workers.exception

class WorkloadHeartbeatException(message: String, cause: Throwable? = null) : WorkerException(message, cause) {
  companion object {
    @Suppress("ConstPropertyName")
    @java.io.Serial
    private const val serialVersionUID: Long = 1L
  }
}
