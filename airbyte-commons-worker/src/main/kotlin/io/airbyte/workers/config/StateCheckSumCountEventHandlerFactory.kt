package io.airbyte.workers.config

import io.airbyte.workers.general.StateCheckSumCountEventHandler
import io.micronaut.context.ApplicationContext
import jakarta.inject.Singleton
import java.util.UUID

@Singleton
class StateCheckSumCountEventHandlerFactory(private val applicationContext: ApplicationContext) {
  fun get(
    connectionId: UUID,
    workspaceId: UUID,
    jobId: Long,
    attemptNumber: Int,
  ): StateCheckSumCountEventHandler {
    // We use a method to create the bean so that we can pass in the required params
    val properties =
      mapOf(
        "connectionId" to connectionId,
        "workspaceId" to workspaceId,
        "jobId" to jobId,
        "attemptNumber" to attemptNumber,
      )
    return applicationContext.createBean(StateCheckSumCountEventHandler::class.java, properties)
  }
}
