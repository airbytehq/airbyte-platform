/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.storage;

import io.airbyte.config.storage.CloudStorageConfigs;
import java.nio.file.Path;

/**
 * Supported document stores.
 */
public class StateClients {

  /**
   * Create document store clients.
   *
   * @param cloudStorageConfigs cloud storage configs
   * @param prefix prefix
   * @return document store client
   */
  public static DocumentStoreClient create(final CloudStorageConfigs cloudStorageConfigs, final Path prefix) {
    DocumentStoreClient documentStoreClient = null;

    switch (cloudStorageConfigs.getType()) {
      case S3 -> {
        documentStoreClient = S3DocumentStoreClient.s3(cloudStorageConfigs.getS3Config(), prefix);
      }
      case MINIO -> {
        documentStoreClient = S3DocumentStoreClient.minio(cloudStorageConfigs.getMinioConfig(), prefix);
      }
      case GCS -> {
        documentStoreClient = GcsDocumentStoreClient.create(cloudStorageConfigs.getGcsConfig(), prefix);
      }
      default -> {
        // to do
      }
    }

    return documentStoreClient;
  }

}
