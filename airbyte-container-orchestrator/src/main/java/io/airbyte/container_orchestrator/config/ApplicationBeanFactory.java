/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.container_orchestrator.config;

import io.airbyte.commons.storage.DocumentType;
import io.airbyte.commons.storage.StorageClient;
import io.airbyte.commons.storage.StorageClientFactory;
import io.airbyte.workers.internal.stateaggregator.StateAggregatorFactory;
import io.micronaut.context.annotation.Factory;
import io.micronaut.context.annotation.Prototype;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

@Factory
class ApplicationBeanFactory {

  @Singleton
  @Named("stateDocumentStore")
  StorageClient documentStoreClient(final StorageClientFactory factory) {
    return factory.create(DocumentType.STATE);
  }

  @Singleton
  @Named("outputDocumentStore")
  StorageClient outputDocumentStoreClient(final StorageClientFactory factory) {
    return factory.create(DocumentType.WORKLOAD_OUTPUT);
  }

  @Prototype
  @Named("syncPersistenceExecutorService")
  public ScheduledExecutorService syncPersistenceExecutorService() {
    return Executors.newSingleThreadScheduledExecutor();
  }

  @Singleton
  public StateAggregatorFactory stateAggregatorFactory() {
    return new StateAggregatorFactory();
  }

}
