/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.container.orchestrator.config

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.commons.storage.DocumentType
import io.airbyte.commons.storage.StorageClient
import io.airbyte.commons.storage.StorageClientFactory
import io.airbyte.persistence.job.models.ReplicationInput
import io.airbyte.protocol.models.v0.AirbyteStreamNameNamespacePair
import io.airbyte.workers.WorkerUtils
import io.micronaut.context.annotation.Factory
import io.micronaut.context.annotation.Value
import jakarta.inject.Named
import jakarta.inject.Singleton
import java.time.Instant
import java.util.UUID
import java.util.concurrent.ExecutorService
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
  @Named("replicationWorkerDispatcher")
  fun replicationWorkerDispatcher(
    @Value("\${airbyte.replication.dispatcher.n-threads:4}") nThreads: Int,
  ) = Executors.newFixedThreadPool(nThreads)

  @Singleton
  @Named("syncPersistenceExecutorService")
  fun syncPersistenceExecutorService(): ScheduledExecutorService = Executors.newSingleThreadScheduledExecutor()

  @Singleton
  @Named("streamNamesToSchemas")
  fun streamNamesToSchemas(replicationInput: ReplicationInput): MutableMap<AirbyteStreamNameNamespacePair, JsonNode?> =
    WorkerUtils.mapStreamNamesToSchemas(replicationInput.catalog)

  @Singleton
  @Named("schemaValidationExecutorService")
  fun schemaValidationExecutorService(): ExecutorService = Executors.newSingleThreadExecutor()
}
