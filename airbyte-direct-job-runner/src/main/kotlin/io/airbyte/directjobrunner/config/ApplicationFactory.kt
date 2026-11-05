/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.directjobrunner.config

import io.airbyte.api.client.AirbyteApiClient
import io.airbyte.commons.net.BlockedAddressMatcher
import io.airbyte.commons.storage.DocumentType
import io.airbyte.commons.storage.StorageClient
import io.airbyte.commons.storage.StorageClientFactory
import io.airbyte.config.secrets.SecretsRepositoryReader
import io.airbyte.config.secrets.persistence.RuntimeSecretPersistenceFactory
import io.airbyte.config.secrets.persistence.SecretPersistence
import io.airbyte.directjobrunner.connect.ConnectProbe
import io.airbyte.metrics.MetricClient
import io.airbyte.micronaut.runtime.AirbyteSecretsManagerConfig
import io.airbyte.workers.hydration.ConnectorSecretsHydrator
import io.micronaut.context.annotation.Factory
import jakarta.inject.Named
import jakarta.inject.Singleton

@Factory
class ApplicationFactory {
  @Singleton
  @Named("outputDocumentStore")
  fun workloadStorageClient(factory: StorageClientFactory): StorageClient = factory.create(DocumentType.WORKLOAD_OUTPUT)

  @Singleton
  fun blockedAddressMatcher(config: ConnectConfig): BlockedAddressMatcher =
    if (config.blockInternalAddresses) {
      BlockedAddressMatcher(config.extraBlockedCidrs)
    } else {
      BlockedAddressMatcher(defaultBlockedCidrs = emptyList())
    }

  @Singleton
  fun connectProbe(
    matcher: BlockedAddressMatcher,
    config: ConnectConfig,
    metricClient: MetricClient,
  ): ConnectProbe = ConnectProbe(matcher = matcher, overallTimeout = config.overallTimeout, metricClient = metricClient)

  // Same wiring as the init container, so checks can hydrate secrets in-process.
  @Singleton
  fun connectorSecretsHydrator(
    airbyteApiClient: AirbyteApiClient,
    runtimeSecretPersistenceFactory: RuntimeSecretPersistenceFactory,
    secretsRepositoryReader: SecretsRepositoryReader,
    airbyteSecretsManagerConfig: AirbyteSecretsManagerConfig,
    defaultSecretPersistence: SecretPersistence,
  ): ConnectorSecretsHydrator =
    ConnectorSecretsHydrator(
      secretsRepositoryReader = secretsRepositoryReader,
      airbyteApiClient = airbyteApiClient,
      useRuntimeSecretPersistence = airbyteSecretsManagerConfig.useRuntimeSecretPersistence,
      environmentSecretPersistence = defaultSecretPersistence,
      runtimeSecretPersistenceFactory = runtimeSecretPersistenceFactory,
    )
}
