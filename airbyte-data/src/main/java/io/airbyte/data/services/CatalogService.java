/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services;

import io.airbyte.config.ActorCatalog;
import io.airbyte.config.ActorCatalogFetchEvent;
import io.airbyte.config.ActorCatalogWithUpdatedAt;
import io.airbyte.config.DestinationCatalog;
import io.airbyte.data.exceptions.ConfigNotFoundException;
import io.airbyte.protocol.models.v0.AirbyteCatalog;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

/**
 * This service is responsible for Catalogs.
 */
public interface CatalogService {

  ActorCatalog getActorCatalogById(UUID actorCatalogId) throws IOException, ConfigNotFoundException;

  Optional<ActorCatalog> getActorCatalog(UUID actorId, String actorVersion, String configHash) throws IOException;

  Optional<ActorCatalogWithUpdatedAt> getMostRecentSourceActorCatalog(UUID sourceId) throws IOException;

  Optional<ActorCatalog> getMostRecentActorCatalogForSource(UUID sourceId) throws IOException;

  Optional<ActorCatalogFetchEvent> getMostRecentActorCatalogFetchEventForSource(UUID sourceId) throws IOException;

  UUID writeActorCatalogWithFetchEvent(AirbyteCatalog catalog, UUID actorId, String connectorVersion, String configurationHash) throws IOException;

  UUID writeActorCatalogWithFetchEvent(DestinationCatalog catalog, UUID actorId, String connectorVersion, String configurationHash)
      throws IOException;

  Map<UUID, ActorCatalogFetchEvent> getMostRecentActorCatalogFetchEventForSources(final List<UUID> sourceIds) throws IOException;

}
