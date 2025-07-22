/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.container.orchestrator.worker

import io.airbyte.config.ConfiguredAirbyteCatalog
import io.airbyte.container.orchestrator.worker.context.ReplicationContext
import io.airbyte.persistence.job.models.ReplicationInput
import io.micronaut.context.annotation.Value
import jakarta.inject.Named
import jakarta.inject.Singleton

@Singleton
class ReplicationContextProvider(
  @Named("attemptId") private val attempt: Int,
  @Value("\${airbyte.job-id}") private val jobId: Long,
) {
  fun provideContext(replicationInput: ReplicationInput): Context {
    val replicationContext =
      ReplicationContext(
        isReset = replicationInput.isReset,
        connectionId = replicationInput.connectionId,
        sourceId = replicationInput.sourceId,
        destinationId = replicationInput.destinationId,
        jobId = jobId,
        attempt = attempt,
        workspaceId = replicationInput.workspaceId,
        sourceDefinitionId = replicationInput.connectionContext.sourceDefinitionId,
        destinationDefinitionId = replicationInput.connectionContext.destinationDefinitionId,
      )

    return Context(
      replicationContext = replicationContext,
      configuredCatalog = replicationInput.catalog,
      supportRefreshes = replicationInput.supportsRefreshes,
      replicationInput = replicationInput,
    )
  }

  data class Context(
    val replicationContext: ReplicationContext,
    val configuredCatalog: ConfiguredAirbyteCatalog,
    val supportRefreshes: Boolean,
    val replicationInput: ReplicationInput,
  )
}
