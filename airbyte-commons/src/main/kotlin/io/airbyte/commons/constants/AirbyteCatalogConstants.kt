/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.constants

/**
 * Collection of constants related to the generation and consumption of the Airbyte Catalogs.
 */
object AirbyteCatalogConstants {
  /**
   * The name of the resource subdirectory that we write the OSS catalog to.
   */
  private const val SEED_SUBDIRECTORY = "seed/"

  const val LOCAL_CONNECTOR_CATALOG_PATH_FILE_NAME: String = "local_oss_registry.json"
  const val LOCAL_SECRETS_MASKS_FILE_NAME: String = "specs_secrets_mask.yaml"

  const val DEFAULT_LOCAL_CONNECTOR_CATALOG_PATH: String = SEED_SUBDIRECTORY + LOCAL_CONNECTOR_CATALOG_PATH_FILE_NAME
  const val LOCAL_SECRETS_MASKS_PATH: String = "/" + SEED_SUBDIRECTORY + LOCAL_SECRETS_MASKS_FILE_NAME

  const val REMOTE_REGISTRY_BASE_URL: String = "https://connectors.airbyte.com/files/"

  const val AIRBYTE_SOURCE_DECLARATIVE_MANIFEST_IMAGE: String = "airbyte/source-declarative-manifest"
}
