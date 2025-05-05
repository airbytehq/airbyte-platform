/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.general.buffered.worker

import io.airbyte.config.ConfiguredAirbyteCatalog
import io.airbyte.persistence.job.models.ReplicationInput
import io.airbyte.workers.context.ReplicationContext

class ReplicationContextProvider(
  private val jobId: Long,
  private val attempt: Int,
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
        sourceImage = replicationInput.sourceLauncherConfig.dockerImage,
        destinationImage = replicationInput.destinationLauncherConfig.dockerImage,
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
