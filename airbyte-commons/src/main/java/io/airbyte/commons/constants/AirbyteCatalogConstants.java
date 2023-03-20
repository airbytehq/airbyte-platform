/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.constants;

/**
 * Collection of constants related to the generation and consumption of the Airbyte Catalogs.
 */
public final class AirbyteCatalogConstants {

  /**
   * The name of the resource subdirectory that we write the OSS catalog to.
   */
  private static final String SEED_SUBDIRECTORY = "seed/";

  /**
   * The name of the resource subdirectory that we write our icon files to.
   */
  public static final String ICON_SUBDIRECTORY = "icons/";

  public static final String LOCAL_CONNECTOR_CATALOG_PATH_FILE_NAME = "oss_catalog.json";
  public static final String LOCAL_SECRETS_MASKS_FILE_NAME = "specs_secrets_mask.yaml";

  public static final String DEFAULT_LOCAL_CONNECTOR_CATALOG_PATH =
      SEED_SUBDIRECTORY + LOCAL_CONNECTOR_CATALOG_PATH_FILE_NAME;
  public static final String LOCAL_SECRETS_MASKS_PATH = "/" + SEED_SUBDIRECTORY + LOCAL_SECRETS_MASKS_FILE_NAME;

  public static final String REMOTE_OSS_CATALOG_URL =
      "https://connectors.airbyte.com/api/v0/catalog/oss_catalog.json";

  private AirbyteCatalogConstants() {
    // Private constructor to prevent instantiation
  }

}
