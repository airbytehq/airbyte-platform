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
import io.airbyte.commons.server.scheduler.SynchronousSchedulerClient
import io.airbyte.config.ActorCatalog
import io.airbyte.config.DestinationCatalog
import io.airbyte.config.DestinationCatalogWithId
import io.airbyte.config.persistence.ActorDefinitionVersionHelper
import io.airbyte.config.toModel
import io.airbyte.config.toProtocol
import io.airbyte.data.services.CatalogService
import io.airbyte.data.services.ConnectionService
import io.airbyte.data.services.DestinationService
import io.airbyte.domain.models.ActorId
import io.airbyte.domain.models.ConnectionId
import io.airbyte.domain.models.DestinationCatalogId
import jakarta.inject.Singleton
import kotlin.jvm.optionals.getOrNull
import io.airbyte.protocol.models.v0.DestinationCatalog as ProtocolDestinationCatalog

@Singleton
class DestinationDiscoverService(
  private val destinationService: DestinationService,
  private val actorDefinitionVersionHelper: ActorDefinitionVersionHelper,
  private val catalogService: CatalogService,
  private val connectionService: ConnectionService,
  private val synchronousSchedulerClient: SynchronousSchedulerClient,
) {
  /**
   * Fetches the catalog of a destination, either from cache or from the actual
   * destination connector.
   *
   * @param destinationId the id of the destination to get the catalog of
   * @param skipCache if true, does not check the cache and always fetches the
   * catalog from the destination connector
   * @return the catalog of the destination
   * @throws DestinationCatalogNotFoundProblem if the destination catalog
   * cannot be found
   * @throws DestinationDiscoverNotSupportedProblem if the destination version
   * does not support data activation
   */
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

    val discoveredCatalogResponse = synchronousSchedulerClient.createDestinationDiscoverJob(destination, destinationDefinition, destinationVersion)

    val discoveredCatalogId = DestinationCatalogId(discoveredCatalogResponse.output)

    val actorCatalog = catalogService.getActorCatalogById(discoveredCatalogId.value)
    return actorCatalogToDestinationCatalog(actorCatalog)
  }

  /**
   * Retrieves the destination catalog associated with a given connection.
   *
   * @param connectionId the ID of the connection for which the destination catalog is to be retrieved.
   * @return the destination catalog with its ID.
   * @throws DestinationCatalogNotFoundProblem if the connection does not have an associated destination catalog ID.
   */
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

  /**
   * (INTERNAL) Writes the discovered catalog result for a destination.
   *
   * @param destinationId the ID of the destination for which the catalog is being written.
   * @param catalog the destination catalog to be written.
   * @param configHash the hash of the configuration used to generate the catalog.
   * @param destinationVersion the version of the destination connector.
   * @return an ActorCatalogId representing the ID of the written catalog.
   */
  fun writeDiscoverCatalogResult(
    destinationId: ActorId,
    catalog: DestinationCatalog,
    configHash: String,
    destinationVersion: String,
  ): DestinationCatalogId =
    DestinationCatalogId(
      catalogService.writeActorCatalogWithFetchEvent(
        catalog.toProtocol(),
        destinationId.value,
        destinationVersion,
        configHash,
      ),
    )

  companion object {
    /**
     * Converts an ActorCatalog to a DestinationCatalogWithId.
     *
     * @param actorCatalog the ActorCatalog to convert.
     * @return the converted DestinationCatalogWithId.
     */
    @JvmStatic
    fun actorCatalogToDestinationCatalog(actorCatalog: ActorCatalog): DestinationCatalogWithId =
      DestinationCatalogWithId(
        catalogId = DestinationCatalogId(actorCatalog.id),
        catalog = Jsons.`object`(actorCatalog.catalog, ProtocolDestinationCatalog::class.java).toModel(),
      )
  }
}
