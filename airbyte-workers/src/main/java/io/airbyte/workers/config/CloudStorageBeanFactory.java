/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.config;

import io.airbyte.commons.json.JsonSerde;
import io.airbyte.featureflag.FeatureFlagClient;
import io.airbyte.metrics.lib.MetricClient;
import io.airbyte.workers.storage.DocumentType;
import io.airbyte.workers.storage.StorageClient;
import io.airbyte.workers.storage.StorageClientFactory;
import io.airbyte.workers.storage.activities.ActivityPayloadStorageClient;
import io.airbyte.workers.storage.activities.payloads.StandardSyncOutputClient;
import io.micronaut.context.annotation.Factory;
import jakarta.inject.Named;
import jakarta.inject.Singleton;

/**
 * Micronaut bean factory for cloud storage-related singletons.
 */
@Factory
@SuppressWarnings("PMD.AvoidDuplicateLiterals")
public class CloudStorageBeanFactory {

  // @Singleton
  // @Named("logStorageConfigs")
  // public StorageConfig logStorageConfigs(final StorageConfig storageConfig) {
  // return storageConfig;
  // }

  @Singleton
  @Named("logDocumentStore")
  public StorageClient logStorageClient(final StorageClientFactory factory) {
    return factory.get(DocumentType.LOGS);
  }

  @Singleton
  @Named("stateDocumentStore")
  public StorageClient stateStorageClient(final StorageClientFactory factory) {
    return factory.get(DocumentType.STATE);
  }

  @SuppressWarnings("LineLength")

  @Singleton
  @Named("outputDocumentStore")
  public StorageClient workloadStorageClient(final StorageClientFactory factory) {
    return factory.get(DocumentType.WORKLOAD_OUTPUT);
  }

  @Singleton
  @Named("payloadDocumentStore")
  public StorageClient payloadStorageClient(final StorageClientFactory factory) {
    return factory.get(DocumentType.ACTIVITY_PAYLOADS);
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
  public StandardSyncOutputClient standardSyncOutputClient(
                                                           final ActivityPayloadStorageClient payloadStorageClient,
                                                           final FeatureFlagClient featureFlagClient,
                                                           final MetricClient metricClient) {
    return new StandardSyncOutputClient(
        payloadStorageClient,
        metricClient,
        featureFlagClient);
  }

}
