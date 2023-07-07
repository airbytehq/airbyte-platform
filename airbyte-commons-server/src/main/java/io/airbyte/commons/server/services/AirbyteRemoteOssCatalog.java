/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.services;

import io.airbyte.commons.constants.AirbyteCatalogConstants;
import io.airbyte.config.ConnectorRegistryDestinationDefinition;
import io.airbyte.config.ConnectorRegistrySourceDefinition;
import io.airbyte.config.init.RemoteDefinitionsProvider;
import jakarta.inject.Singleton;
import java.util.Collections;
import java.util.List;
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
  private static final Logger LOGGER = LoggerFactory.getLogger(AirbyteRemoteOssCatalog.class);

  public AirbyteRemoteOssCatalog() {
    this(AirbyteCatalogConstants.REMOTE_OSS_CATALOG_URL);
  }

  public AirbyteRemoteOssCatalog(final String remoteCatalogUrl) {
    super(remoteCatalogUrl, TIMEOUT);
    LOGGER.info("Initializing OSS catalog from {} with timeout {}", AirbyteCatalogConstants.REMOTE_OSS_CATALOG_URL, TIMEOUT);
  }

  @Override
  @SuppressWarnings("PMD.AvoidCatchingThrowable")
  public List<ConnectorRegistryDestinationDefinition> getDestinationDefinitions() {
    try {
      return super.getDestinationDefinitions();
    } catch (final Throwable e) {
      LOGGER.warn(
          "Unable to retrieve latest Destination list from Remote Registry. This warning is expected if this cluster does not have internet access.",
          e);
      return Collections.emptyList();
    }
  }

  @Override
  @SuppressWarnings("PMD.AvoidCatchingThrowable")
  public List<ConnectorRegistrySourceDefinition> getSourceDefinitions() {
    try {
      return super.getSourceDefinitions();
    } catch (final Throwable e) {
      LOGGER.warn(
          "Unable to retrieve latest Source list from Remote Registry. This warning is expected if this cluster does not have internet access.",
          e);
      return Collections.emptyList();
    }
  }

}
