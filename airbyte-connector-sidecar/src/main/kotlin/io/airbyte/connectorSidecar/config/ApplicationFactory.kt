package io.airbyte.connectorSidecar.config

import io.airbyte.metrics.lib.MetricClient
import io.airbyte.metrics.lib.MetricClientFactory.initialize
import io.airbyte.metrics.lib.MetricEmittingApps
import io.airbyte.workers.config.DocumentStoreFactory
import io.airbyte.workers.config.DocumentType
import io.airbyte.workers.storage.DocumentStoreClient
import io.micronaut.context.annotation.Factory
import jakarta.inject.Named
import jakarta.inject.Singleton

@Factory
class ApplicationFactory {
  @Singleton
  @Named("outputDocumentStore")
  fun outputDocumentStoreClient(documentStoreFactory: DocumentStoreFactory): DocumentStoreClient {
    return documentStoreFactory.get(DocumentType.WORKLOAD_OUTPUTS)
  }

  @Singleton
  fun metricClient(): MetricClient {
    initialize(MetricEmittingApps.SIDECAR_ORCHESTRATOR)
    return io.airbyte.metrics.lib.MetricClientFactory.getMetricClient()
  }
}
