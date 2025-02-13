/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.config;

import io.airbyte.commons.json.JsonSerde;
import io.airbyte.commons.storage.DocumentType;
import io.airbyte.commons.storage.StorageClient;
import io.airbyte.commons.storage.StorageClientFactory;
import io.airbyte.config.ConfiguredAirbyteCatalog;
import io.airbyte.config.State;
import io.airbyte.metrics.lib.MetricClient;
import io.airbyte.workers.storage.activities.ActivityPayloadStorageClient;
import io.airbyte.workers.storage.activities.OutputStorageClient;
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
  @Named("logDocumentStore")
  public StorageClient logStorageClient(final StorageClientFactory factory) {
    return factory.create(DocumentType.LOGS);
  }

  @Singleton
  @Named("auditLoggingDocumentStore")
  public StorageClient auditLoggingStorageClient(final StorageClientFactory factory) {
    return factory.create(DocumentType.AUDIT_LOGS);
  }

  @Singleton
  @Named("stateDocumentStore")
  public StorageClient stateStorageClient(final StorageClientFactory factory) {
    return factory.create(DocumentType.STATE);
  }

  @SuppressWarnings("LineLength")

  @Singleton
  @Named("outputDocumentStore")
  public StorageClient workloadStorageClient(final StorageClientFactory factory) {
    return factory.create(DocumentType.WORKLOAD_OUTPUT);
  }

  @Singleton
  @Named("payloadDocumentStore")
  public StorageClient payloadStorageClient(final StorageClientFactory factory) {
    return factory.create(DocumentType.ACTIVITY_PAYLOADS);
  }

  @Singleton
  public JsonSerde jsonSerde() {
    return new JsonSerde();
  }

  @Singleton
  public ActivityPayloadStorageClient activityPayloadStorageClient(
                                                                   @Named("payloadDocumentStore") final StorageClient storageClientRaw,
                                                                   final JsonSerde jsonSerde,
                                                                   final MetricClient metricClient) {
    return new ActivityPayloadStorageClient(
        storageClientRaw,
        jsonSerde,
        metricClient);
  }

  @Singleton
  @Named("outputStateClient")
  public OutputStorageClient<State> outputStateClient(
                                                      final ActivityPayloadStorageClient payloadStorageClient,
                                                      final MetricClient metricClient) {
    return new OutputStorageClient<>(
        payloadStorageClient,
        metricClient,
        "output-state",
        State.class);
  }

  @Singleton
  @Named("outputCatalogClient")
  public OutputStorageClient<ConfiguredAirbyteCatalog> outputCatalogClient(
                                                                           final ActivityPayloadStorageClient payloadStorageClient,
                                                                           final MetricClient metricClient) {
    return new OutputStorageClient<>(
        payloadStorageClient,
        metricClient,
        "output-catalog",
        ConfiguredAirbyteCatalog.class);
  }

}
