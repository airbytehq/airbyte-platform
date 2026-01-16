/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services

import io.airbyte.config.ActorCatalog
import io.airbyte.config.ActorCatalogFetchEvent
import io.airbyte.config.ActorCatalogWithUpdatedAt
import io.airbyte.protocol.models.v0.AirbyteCatalog
import io.airbyte.protocol.models.v0.DestinationCatalog
import java.io.IOException
import java.util.Optional
import java.util.UUID

/**
 * This service is responsible for Catalogs.
 */
interface CatalogService {
  fun getActorCatalogById(actorCatalogId: UUID): ActorCatalog

  fun getActorCatalog(
    actorId: UUID,
    actorVersion: String,
    configHash: String,
  ): Optional<ActorCatalog>

  fun getMostRecentSourceActorCatalog(sourceId: UUID): Optional<ActorCatalogWithUpdatedAt>

  fun getMostRecentActorCatalogForSource(sourceId: UUID): Optional<ActorCatalog>

  fun getMostRecentActorCatalogFetchEventForSource(sourceId: UUID): Optional<ActorCatalogFetchEvent>

  fun writeActorCatalogWithFetchEvent(
    catalog: AirbyteCatalog,
    actorId: UUID,
    connectorVersion: String,
    configurationHash: String,
  ): UUID

  fun writeActorCatalogWithFetchEvent(
    catalog: DestinationCatalog,
    actorId: UUID,
    connectorVersion: String,
    configurationHash: String,
  ): UUID

  fun getMostRecentActorCatalogFetchEventForSources(sourceIds: List<UUID>): Map<UUID, ActorCatalogFetchEvent>

  @Throws(IOException::class)
  fun getActorIdByCatalogId(actorCatalogId: UUID): Optional<UUID>
}
