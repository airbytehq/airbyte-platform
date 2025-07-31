/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.config

import io.airbyte.commons.json.JsonSerde
import io.airbyte.commons.storage.DocumentType
import io.airbyte.commons.storage.StorageClient
import io.airbyte.commons.storage.StorageClientFactory
import io.airbyte.config.ConfiguredAirbyteCatalog
import io.airbyte.config.State
import io.airbyte.metrics.MetricClient
import io.airbyte.workers.storage.activities.ActivityPayloadStorageClient
import io.airbyte.workers.storage.activities.OutputStorageClient
import io.micronaut.context.annotation.Factory
import jakarta.inject.Named
import jakarta.inject.Singleton

/**
 * Micronaut bean factory for cloud storage-related singletons.
 */
@Factory
class CloudStorageBeanFactory {
  @Singleton
  @Named("logDocumentStore")
  fun logStorageClient(factory: StorageClientFactory): StorageClient = factory.create(DocumentType.LOGS)

  @Singleton
  @Named("auditLoggingDocumentStore")
  fun auditLoggingStorageClient(factory: StorageClientFactory): StorageClient = factory.create(DocumentType.AUDIT_LOGS)

  @Singleton
  @Named("stateDocumentStore")
  fun stateStorageClient(factory: StorageClientFactory): StorageClient = factory.create(DocumentType.STATE)

  @Singleton
  @Named("outputDocumentStore")
  fun workloadStorageClient(factory: StorageClientFactory): StorageClient = factory.create(DocumentType.WORKLOAD_OUTPUT)

  @Singleton
  @Named("payloadDocumentStore")
  fun payloadStorageClient(factory: StorageClientFactory): StorageClient = factory.create(DocumentType.ACTIVITY_PAYLOADS)

  @Singleton
  fun jsonSerde(): JsonSerde = JsonSerde()

  @Singleton
  fun activityPayloadStorageClient(
    @Named("payloadDocumentStore") storageClientRaw: StorageClient,
    jsonSerde: JsonSerde,
    metricClient: MetricClient,
  ): ActivityPayloadStorageClient =
    ActivityPayloadStorageClient(
      storageClientRaw,
      jsonSerde,
      metricClient,
    )

  @Singleton
  @Named("outputStateClient")
  fun outputStateClient(
    payloadStorageClient: ActivityPayloadStorageClient,
    metricClient: MetricClient,
  ): OutputStorageClient<State> =
    OutputStorageClient(
      payloadStorageClient,
      metricClient,
      "output-state",
      State::class.java,
    )

  @Singleton
  @Named("outputCatalogClient")
  fun outputCatalogClient(
    payloadStorageClient: ActivityPayloadStorageClient,
    metricClient: MetricClient,
  ): OutputStorageClient<ConfiguredAirbyteCatalog> =
    OutputStorageClient(
      payloadStorageClient,
      metricClient,
      "output-catalog",
      ConfiguredAirbyteCatalog::class.java,
    )
}
