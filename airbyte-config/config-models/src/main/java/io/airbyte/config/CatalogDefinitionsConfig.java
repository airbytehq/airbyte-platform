/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config;

import io.airbyte.commons.constants.AirbyteCatalogConstants;
import java.util.Optional;

/**
 * Catalog definitions config.
 */
public class CatalogDefinitionsConfig {

  /**
   * This method is used to get the path to the local connector catalog. Its override is intended for
   * Airbyte developers to test out changes to the catalog locally
   *
   * @return path to connector catalog
   */
  public static String getLocalConnectorCatalogPath() {
    Optional<String> customCatalogPath = new EnvConfigs().getLocalCatalogPath();
    if (customCatalogPath.isPresent()) {
      return customCatalogPath.get();
    }

    return AirbyteCatalogConstants.DEFAULT_LOCAL_CONNECTOR_CATALOG_PATH;
  }

  /**
   * This method is used to get the relative path used for writing the local connector catalog to
   * resources.
   *
   * Note: We always want to write to the default path. This is to prevent overwriting the catalog
   * file in the event we are using a custom catalog path.
   */
  public static String getLocalCatalogWritePath() {
    return AirbyteCatalogConstants.DEFAULT_LOCAL_CONNECTOR_CATALOG_PATH;
  }

}
