/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.services

import com.google.common.hash.Hashing
import io.airbyte.api.problems.model.generated.ProblemDestinationCatalogNotFoundData
import io.airbyte.api.problems.model.generated.ProblemDestinationDiscoverData
import io.airbyte.api.problems.throwable.generated.DestinationCatalogNotFoundProblem
import io.airbyte.api.problems.throwable.generated.DestinationDiscoverNotSupportedProblem
import io.airbyte.commons.json.Jsons
import io.airbyte.config.ActorCatalog
import io.airbyte.config.DestinationCatalog
import io.airbyte.config.DestinationCatalogWithId
import io.airbyte.config.DestinationOperation
import io.airbyte.config.DestinationSyncMode
import io.airbyte.config.persistence.ActorDefinitionVersionHelper
import io.airbyte.data.services.CatalogService
import io.airbyte.data.services.ConnectionService
import io.airbyte.data.services.DestinationService
import io.airbyte.domain.models.ActorId
import io.airbyte.domain.models.ConnectionId
import io.airbyte.domain.models.DestinationCatalogId
import jakarta.inject.Singleton
import kotlin.jvm.optionals.getOrNull

@Singleton
class DestinationDiscoverService(
  private val destinationService: DestinationService,
  private val actorDefinitionVersionHelper: ActorDefinitionVersionHelper,
  private val catalogService: CatalogService,
  private val connectionService: ConnectionService,
) {
  companion object {
    val MOCK_CATALOG =
      DestinationCatalog(
        operations =
          listOf(
            DestinationOperation(
              objectName = "person_events",
              syncMode = DestinationSyncMode.APPEND_DEDUP,
              jsonSchema =
                Jsons.jsonNode(
                  mapOf(
                    "type" to "object",
                    "additionalProperties" to true,
                    "properties" to
                      mapOf(
                        "person_cio_id" to
                          mapOf(
                            "type" to "string",
                            "description" to "The unique identifier for the person.",
                          ),
                        "email" to
                          mapOf(
                            "type" to "string",
                            "description" to "The email address of the person.",
                          ),
                      ),
                  ),
                ),
              matchingKeys =
                listOf(
                  listOf("person_cio_id"),
                  listOf("email"),
                ),
            ),
          ),
      )
  }

  fun mockDiscover(
    destinationId: ActorId,
    destinationVersion: String,
    configHash: String,
  ): DestinationCatalogId {
    // TODO(pedro): wire this up to a real discover on the destination
    val catalogId = catalogService.writeActorCatalogWithFetchEvent(MOCK_CATALOG, destinationId.value, destinationVersion, configHash)
    return DestinationCatalogId(catalogId)
  }

  fun getDestinationCatalog(
    destinationId: ActorId,
    skipCache: Boolean = false,
  ): DestinationCatalogWithId {
    val destination = destinationService.getDestinationConnection(destinationId.value)
    val destinationDefinition = destinationService.getStandardDestinationDefinition(destination.destinationDefinitionId)
    val destinationVersion =
      actorDefinitionVersionHelper.getDestinationVersion(
        destinationDefinition,
        destination.workspaceId,
        destination.destinationId,
      )

    if (!destinationVersion.supportsDataActivation) {
      throw DestinationDiscoverNotSupportedProblem(
        ProblemDestinationDiscoverData()
          .destinationId(destination.destinationId)
          .destinationVersion(destinationVersion.dockerImageTag)
          .destinationDefinitionId(destination.destinationDefinitionId),
      )
    }

    val configHash = Hashing.md5().hashBytes(Jsons.serialize(destination.configuration).toByteArray()).toString()

    if (!skipCache) {
      val cachedCatalog = catalogService.getActorCatalog(destinationId.value, destinationVersion.dockerImageTag, configHash).getOrNull()

      if (cachedCatalog != null) {
        return actorCatalogToDestinationCatalog(cachedCatalog)
      }
    }

    val discoveredCatalogId = mockDiscover(destinationId, destinationVersion.dockerImageTag, configHash)

    val actorCatalog = catalogService.getActorCatalogById(discoveredCatalogId.value)
    return actorCatalogToDestinationCatalog(actorCatalog)
  }

  fun getDestinationCatalog(connectionId: ConnectionId): DestinationCatalogWithId {
    val connection = connectionService.getStandardSync(connectionId.value)
    if (connection.destinationCatalogId == null) {
      throw DestinationCatalogNotFoundProblem(
        ProblemDestinationCatalogNotFoundData()
          .connectionId(connectionId.value),
      )
    }
    val catalog = catalogService.getActorCatalogById(connection.destinationCatalogId)
    return actorCatalogToDestinationCatalog(catalog)
  }

  fun actorCatalogToDestinationCatalog(actorCatalog: ActorCatalog): DestinationCatalogWithId =
    DestinationCatalogWithId(
      catalogId = DestinationCatalogId(actorCatalog.id),
      catalog = Jsons.`object`(actorCatalog.catalog, DestinationCatalog::class.java),
    )
}
