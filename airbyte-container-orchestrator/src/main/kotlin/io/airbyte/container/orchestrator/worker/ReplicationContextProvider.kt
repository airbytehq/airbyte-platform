/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.container.orchestrator.worker

import io.airbyte.config.ConfiguredAirbyteCatalog
import io.airbyte.container.orchestrator.worker.context.ReplicationContext
import io.airbyte.micronaut.runtime.AirbyteContextConfig
import io.airbyte.persistence.job.models.ReplicationInput
import jakarta.inject.Singleton

@Singleton
class ReplicationContextProvider(
  private val airbyteContextConfig: AirbyteContextConfig,
) {
  fun provideContext(replicationInput: ReplicationInput): Context {
    val replicationContext =
      ReplicationContext(
        isReset = replicationInput.isReset,
        connectionId = replicationInput.connectionId,
        sourceId = replicationInput.sourceId,
        destinationId = replicationInput.destinationId,
        jobId = airbyteContextConfig.jobId,
        attempt = airbyteContextConfig.attemptId,
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
