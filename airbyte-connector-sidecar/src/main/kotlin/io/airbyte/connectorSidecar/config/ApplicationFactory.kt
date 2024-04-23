package io.airbyte.connectorSidecar.config

import io.airbyte.metrics.lib.MetricClient
import io.airbyte.metrics.lib.NotImplementedMetricClient
import io.airbyte.workers.storage.DocumentType
import io.airbyte.workers.storage.StorageClient
import io.airbyte.workers.storage.StorageClientFactory
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
