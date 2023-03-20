/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.specs;

import io.airbyte.commons.constants.AirbyteCatalogConstants;
import io.airbyte.config.CatalogDefinitionsConfig;
import java.net.URL;
import java.nio.file.Path;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Download connector catalogs from airbytehq/airbyte repository.
 */
public class CombinedConnectorCatalogDownloader {

  private static final Logger LOGGER = LoggerFactory.getLogger(CombinedConnectorCatalogDownloader.class);

  /**
   * This method is to create a path to the resource folder in the project. This is so that it's
   * available at runtime via the getResource method.
   */
  public static Path getResourcePath(final String projectPath, final String relativePath) {
    return Path.of(projectPath, "src/main/resources/", relativePath);
  }

  /**
   * This method is to download the OSS catalog from the remote URL and save it to the local resource
   * folder.
   */
  public static void main(final String[] args) throws Exception {
    final String projectPath = args[0];
    final String relativeWritePath = CatalogDefinitionsConfig.getLocalCatalogWritePath();
    final Path writePath = getResourcePath(projectPath, relativeWritePath);

    LOGGER.info("Downloading OSS catalog from {} to {}", AirbyteCatalogConstants.REMOTE_OSS_CATALOG_URL, writePath);

    final int timeout = 10000;
    FileUtils.copyURLToFile(new URL(AirbyteCatalogConstants.REMOTE_OSS_CATALOG_URL), writePath.toFile(), timeout, timeout);
  }

}
