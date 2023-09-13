/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services;

import io.airbyte.config.ActorCatalog;
import io.airbyte.config.ActorCatalogFetchEvent;
import io.airbyte.config.ActorCatalogWithUpdatedAt;
import io.airbyte.config.persistence.ConfigNotFoundException;
import io.airbyte.protocol.models.AirbyteCatalog;
import java.io.IOException;
import java.time.OffsetDateTime;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

/**
 * This service is responsible for Catalogs.
 */
public interface CatalogService {

  Map<UUID, AirbyteCatalog> findCatalogByHash(String catalogHash);

  ActorCatalog getActorCatalogById(UUID actorCatalogId) throws IOException, ConfigNotFoundException;

  /**
   * Store an Airbyte catalog in DB if it is not present already.
   * <p>
   * Checks in the config DB if the catalog is present already, if so returns it identifier. It is not
   * present, it is inserted in DB with a new identifier and that identifier is returned.
   *
   * @param airbyteCatalog An Airbyte catalog to cache
   * @return the db identifier for the cached catalog.
   */
  UUID getOrInsertActorCatalog(AirbyteCatalog airbyteCatalog, OffsetDateTime timestamp);

  /**
   * This function will be used to gradually migrate the existing data in the database to use the
   * canonical json serialization. It will first try to find the catalog using the canonical json
   * serialization. If it fails, it will fallback to the old json serialization.
   *
   * @param airbyteCatalog the catalog to be cached
   * @param timestamp - timestamp
   * @param writeCatalogInCanonicalJson - should we write the catalog in canonical json
   * @return the db identifier for the cached catalog.
   */
  UUID getOrInsertCanonicalActorCatalog(AirbyteCatalog airbyteCatalog, OffsetDateTime timestamp, boolean writeCatalogInCanonicalJson);

  UUID lookupCatalogId(String catalogHash, AirbyteCatalog airbyteCatalog);

  UUID insertCatalog(AirbyteCatalog airbyteCatalog, String catalogHash, OffsetDateTime timestamp);

  UUID findAndReturnCatalogId(String catalogHash, AirbyteCatalog airbyteCatalog);

  Optional<ActorCatalog> getActorCatalog(UUID actorId, String actorVersion, String configHash) throws IOException;

  Optional<ActorCatalogWithUpdatedAt> getMostRecentSourceActorCatalog(UUID sourceId) throws IOException;

  Optional<ActorCatalog> getMostRecentActorCatalogForSource(UUID sourceId) throws IOException;

  Optional<ActorCatalogFetchEvent> getMostRecentActorCatalogFetchEventForSource(UUID sourceId) throws IOException;

  UUID writeActorCatalogFetchEvent(AirbyteCatalog catalog, UUID actorId, String connectorVersion, String configurationHash) throws IOException;

  UUID writeCanonicalActorCatalogFetchEvent(AirbyteCatalog catalog,
                                            UUID actorId,
                                            String connectorVersion,
                                            String configurationHash,
                                            boolean writeCatalogInCanonicalJson)
      throws IOException;

}
