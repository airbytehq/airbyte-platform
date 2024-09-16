package io.airbyte.connectorSidecar.config

import io.airbyte.commons.storage.DocumentType
import io.airbyte.commons.storage.StorageClient
import io.airbyte.commons.storage.StorageClientFactory
import io.airbyte.metrics.lib.MetricClient
import io.airbyte.metrics.lib.NotImplementedMetricClient
import io.micronaut.context.annotation.Factory
import jakarta.inject.Named
import jakarta.inject.Singleton

@Factory
class ApplicationFactory {
  @Singleton
  @Named("outputDocumentStore")
  fun workloadStorageClient(factory: StorageClientFactory): StorageClient = factory.get(DocumentType.WORKLOAD_OUTPUT)

  @Singleton
  fun metricClient(): MetricClient = NotImplementedMetricClient()
}
