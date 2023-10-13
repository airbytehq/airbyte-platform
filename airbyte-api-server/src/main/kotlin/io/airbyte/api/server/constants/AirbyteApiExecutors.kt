package io.airbyte.api.server.constants

interface AirbyteApiExecutors {
  companion object {
    /**
     * The name of the [java.util.concurrent.ExecutorService] used to schedule health check tasks.
     */
    const val HEALTH = "health"
  }
}
