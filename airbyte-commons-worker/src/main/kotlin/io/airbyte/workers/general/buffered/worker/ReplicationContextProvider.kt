/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.general.buffered.worker

import io.airbyte.api.client.AirbyteApiClient
import io.airbyte.api.client.model.generated.ActorType
import io.airbyte.api.client.model.generated.DestinationIdRequestBody
import io.airbyte.api.client.model.generated.ResolveActorDefinitionVersionRequestBody
import io.airbyte.api.client.model.generated.SourceIdRequestBody
import io.airbyte.commons.helper.DockerImageName
import io.airbyte.config.ConfiguredAirbyteCatalog
import io.airbyte.persistence.job.models.ReplicationInput
import io.airbyte.workers.context.ReplicationContext
import java.util.UUID

class ReplicationContextProvider(
  private val jobId: String,
  private val attempt: Int,
  private val airbyteApiClient: AirbyteApiClient,
) {
  fun provideContext(replicationInput: ReplicationInput): Context {
    val sourceDefinitionId = getSourceDefinitionId(replicationInput.sourceId)
    val destinationDefinitionId = getDestinationDefinitionId(replicationInput.destinationId)

    val replicationContext =
      ReplicationContext(
        isReset = replicationInput.isReset,
        connectionId = replicationInput.connectionId,
        sourceId = replicationInput.sourceId,
        destinationId = replicationInput.destinationId,
        jobId = jobId.toLong(),
        attempt = attempt,
        workspaceId = replicationInput.workspaceId,
        sourceImage = replicationInput.sourceLauncherConfig.dockerImage,
        destinationImage = replicationInput.destinationLauncherConfig.dockerImage,
        sourceDefinitionId = sourceDefinitionId,
        destinationDefinitionId = destinationDefinitionId,
      )

    val supportsRefresh =
      supportsRefreshes(
        destinationDefinitionId,
        replicationInput.destinationLauncherConfig.dockerImage,
      )

    return Context(
      replicationContext = replicationContext,
      configuredCatalog = replicationInput.catalog,
      supportRefreshes = supportsRefresh,
      replicationInput = replicationInput,
    )
  }

  private fun supportsRefreshes(
    destinationDefinitionId: UUID,
    destinationImage: String,
  ): Boolean =
    airbyteApiClient.actorDefinitionVersionApi
      .resolveActorDefinitionVersionByTag(
        ResolveActorDefinitionVersionRequestBody(
          actorDefinitionId = destinationDefinitionId,
          actorType = ActorType.DESTINATION,
          dockerImageTag = DockerImageName.extractTag(destinationImage),
        ),
      ).supportRefreshes

  private fun getSourceDefinitionId(sourceId: UUID): UUID =
    airbyteApiClient.sourceApi.getSource(SourceIdRequestBody(sourceId = sourceId)).sourceDefinitionId

  private fun getDestinationDefinitionId(destinationId: UUID): UUID =
    airbyteApiClient.destinationApi.getDestination(DestinationIdRequestBody(destinationId = destinationId)).destinationDefinitionId

  data class Context(
    val replicationContext: ReplicationContext,
    val configuredCatalog: ConfiguredAirbyteCatalog,
    val supportRefreshes: Boolean,
    val replicationInput: ReplicationInput,
  )
}
