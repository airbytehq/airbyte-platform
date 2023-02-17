/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.services;

import io.airbyte.config.CatalogDefinitionsConfig;
import io.airbyte.config.StandardDestinationDefinition;
import io.airbyte.config.init.RemoteDefinitionsProvider;
import io.airbyte.config.specs.CombinedConnectorCatalogDownloader;
import jakarta.inject.Singleton;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Convenience class for retrieving the remote OSS Catalog.
 */
@SuppressWarnings("PMD.AvoidCatchingThrowable")
@Singleton
public class AirbyteRemoteOssCatalog extends RemoteDefinitionsProvider {

  private static final String REMOTE_OSS_CATALOG_URL = CatalogDefinitionsConfig.getRemoteOssCatalogUrl();
  private static final long TIMEOUT = 30000;
  private static final Logger LOGGER = LoggerFactory.getLogger(CombinedConnectorCatalogDownloader.class);

  public AirbyteRemoteOssCatalog() {
    super(REMOTE_OSS_CATALOG_URL, TIMEOUT);
    LOGGER.info("Initializing OSS catalog from {} with timeout {}", REMOTE_OSS_CATALOG_URL, TIMEOUT);
  }

}
