/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services

import io.airbyte.config.ActorCatalog
import io.airbyte.config.ActorCatalogFetchEvent
import io.airbyte.config.ActorCatalogWithUpdatedAt
import io.airbyte.data.ConfigNotFoundException
import io.airbyte.protocol.models.v0.AirbyteCatalog
import io.airbyte.protocol.models.v0.DestinationCatalog
import java.io.IOException
import java.util.Optional
import java.util.UUID

/**
 * This service is responsible for Catalogs.
 */
interface CatalogService {
  @Throws(IOException::class, ConfigNotFoundException::class)
  fun getActorCatalogById(actorCatalogId: UUID): ActorCatalog

  @Throws(IOException::class)
  fun getActorCatalog(
    actorId: UUID,
    actorVersion: String,
    configHash: String,
  ): Optional<ActorCatalog>

  @Throws(IOException::class)
  fun getMostRecentSourceActorCatalog(sourceId: UUID): Optional<ActorCatalogWithUpdatedAt>

  @Throws(IOException::class)
  fun getMostRecentActorCatalogForSource(sourceId: UUID): Optional<ActorCatalog>

  @Throws(IOException::class)
  fun getMostRecentActorCatalogFetchEventForSource(sourceId: UUID): Optional<ActorCatalogFetchEvent>

  @Throws(IOException::class)
  fun writeActorCatalogWithFetchEvent(
    catalog: AirbyteCatalog,
    actorId: UUID,
    connectorVersion: String,
    configurationHash: String,
  ): UUID

  @Throws(IOException::class)
  fun writeActorCatalogWithFetchEvent(
    catalog: DestinationCatalog,
    actorId: UUID,
    connectorVersion: String,
    configurationHash: String,
  ): UUID

  @Throws(IOException::class)
  fun getMostRecentActorCatalogFetchEventForSources(sourceIds: List<UUID>): Map<UUID, ActorCatalogFetchEvent>
}
