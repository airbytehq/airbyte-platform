/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.container.orchestrator.config

import io.airbyte.commons.storage.DocumentType
import io.airbyte.commons.storage.StorageClient
import io.airbyte.commons.storage.StorageClientFactory
import io.airbyte.workers.tracker.ThreadedTimeTracker
import io.micronaut.context.annotation.Factory
import jakarta.inject.Named
import jakarta.inject.Singleton
import java.time.Instant
import java.util.UUID
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledExecutorService
import java.util.function.Supplier

private const val BASE_URL = "https://cloud.airbyte.com/"

@Factory
class ApplicationBeanFactory {
  @Singleton
  @Named("stateDocumentStore")
  fun documentStoreClient(factory: StorageClientFactory): StorageClient = factory.create(DocumentType.STATE)

  @Singleton
  @Named("epochMilliSupplier")
  fun epochMilliSupplier() = Supplier { Instant.now().toEpochMilli() }

  @Singleton
  @Named("idSupplier")
  fun idSupplier() = Supplier { UUID.randomUUID() }

  @Singleton
  @Named("outputDocumentStore")
  fun outputDocumentStoreClient(factory: StorageClientFactory): StorageClient = factory.create(DocumentType.WORKLOAD_OUTPUT)

  @Singleton
  @Named("syncPersistenceExecutorService")
  fun syncPersistenceExecutorService(): ScheduledExecutorService = Executors.newSingleThreadScheduledExecutor()

  @Singleton
  fun threadedTimeTracker() = ThreadedTimeTracker()
}
