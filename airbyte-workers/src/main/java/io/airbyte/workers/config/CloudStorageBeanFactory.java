/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.config;

import io.airbyte.config.storage.CloudStorageConfigs;
import io.airbyte.config.storage.CloudStorageConfigs.WorkerStorageType;
import io.airbyte.workers.storage.DocumentStoreClient;
import io.micronaut.context.annotation.Factory;
import jakarta.inject.Named;
import jakarta.inject.Singleton;

/**
 * Micronaut bean factory for cloud storage-related singletons.
 */
@Factory
@SuppressWarnings("PMD.AvoidDuplicateLiterals")
public class CloudStorageBeanFactory {

  @Singleton
  @Named("logStorageConfigs")
  public CloudStorageConfigs logStorageConfigs(final DocumentStoreConfigFactory documentStoreConfigFactory) {
    final WorkerStorageType storageType = documentStoreConfigFactory.getStorageType(DocumentType.LOGS);
    return documentStoreConfigFactory.get(DocumentType.LOGS, storageType);
  }

  @Singleton
  @Named("stateDocumentStore")
  public DocumentStoreClient documentStoreClient(final DocumentStoreFactory documentStoreFactory) {
    return documentStoreFactory.get(DocumentType.STATE);
  }

  @SuppressWarnings("LineLength")

  @Singleton
  @Named("outputDocumentStore")
  public DocumentStoreClient outputDocumentStoreClient(final DocumentStoreFactory documentStoreFactory) {
    return documentStoreFactory.get(DocumentType.WORKLOAD_OUTPUTS);
  }

}
