package io.airbyte.workers.workload.exception

class DocStoreAccessException(override val message: String, override val cause: Throwable) : Exception(message, cause)
