/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.services;

import io.airbyte.commons.constants.AirbyteCatalogConstants;
import io.airbyte.config.init.RemoteDefinitionsProvider;
import io.airbyte.config.specs.CombinedConnectorCatalogDownloader;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Convenience class for retrieving the remote OSS Catalog.
 */
// This should be deleted, in favour of the DefinitionsProvider Singleton when cloud is migrated to
// micronaut
@Deprecated(forRemoval = true)
@Singleton
public class AirbyteRemoteOssCatalog extends RemoteDefinitionsProvider {

  private static final long TIMEOUT = 30000;
  private static final Logger LOGGER = LoggerFactory.getLogger(CombinedConnectorCatalogDownloader.class);

  public AirbyteRemoteOssCatalog() {
    this(AirbyteCatalogConstants.REMOTE_OSS_CATALOG_URL);
  }

  public AirbyteRemoteOssCatalog(String remoteCatalogUrl) {
    super(remoteCatalogUrl, TIMEOUT);
    LOGGER.info("Initializing OSS catalog from {} with timeout {}", AirbyteCatalogConstants.REMOTE_OSS_CATALOG_URL, TIMEOUT);
  }

}
