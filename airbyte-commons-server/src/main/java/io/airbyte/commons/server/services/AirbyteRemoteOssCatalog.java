/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.services;

import io.airbyte.config.CatalogDefinitionsConfig;
import io.airbyte.config.init.RemoteDefinitionsProvider;
import java.net.URISyntaxException;
import java.time.Duration;

/**
 * Convenience class for retrieving files checked into the Airbyte Github repo.
 */
@SuppressWarnings("PMD.AvoidCatchingThrowable")
public class AirbyteRemoteOssCatalog extends RemoteDefinitionsProvider {

  private static final String REMOTE_OSS_CATALOG_URL = CatalogDefinitionsConfig.getRemoteOssCatalogUrl();

  public static AirbyteRemoteOssCatalog production() {
    try {
      return new AirbyteRemoteOssCatalog(REMOTE_OSS_CATALOG_URL, Duration.ofSeconds(30));
    } catch (URISyntaxException e) {
      throw new RuntimeException("Failed to create AirbyteRemoteOssCatalog", e);
    }

  }

  public static AirbyteRemoteOssCatalog test(final String testBaseUrl, final Duration timeout) throws URISyntaxException {
    return new AirbyteRemoteOssCatalog(testBaseUrl, timeout);
  }

  // TODO (ben): Remove the need for subclassing if possible
  public AirbyteRemoteOssCatalog(final String baseUrl, final Duration timeout) throws URISyntaxException {
    super(baseUrl, timeout.toMillis());
  }

}
