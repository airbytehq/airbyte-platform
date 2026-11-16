/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.directjobrunner.config

import dev.failsafe.RetryPolicy
import io.airbyte.api.client.AirbyteApiClient
import io.airbyte.commons.net.BlockedAddressMatcher
import io.airbyte.commons.storage.DocumentType
import io.airbyte.commons.storage.StorageClient
import io.airbyte.commons.storage.StorageClientFactory
import io.airbyte.config.secrets.SecretsRepositoryReader
import io.airbyte.config.secrets.persistence.RuntimeSecretPersistenceFactory
import io.airbyte.config.secrets.persistence.SecretPersistence
import io.airbyte.directjobrunner.connect.ConnectProbe
import io.airbyte.manifestserver.api.client.generated.HealthApi
import io.airbyte.metrics.MetricClient
import io.airbyte.micronaut.runtime.AirbyteManifestServerApiClientConfig
import io.airbyte.micronaut.runtime.AirbyteSecretsManagerConfig
import io.airbyte.micronaut.runtime.MANIFEST_SERVER_API_PREFIX
import io.airbyte.workers.hydration.ConnectorSecretsHydrator
import io.micronaut.context.annotation.Factory
import io.micronaut.context.annotation.Requires
import jakarta.inject.Named
import jakarta.inject.Singleton
import okhttp3.OkHttpClient
import okhttp3.Response

/** The manifest server's health endpoint, with timeouts short enough to answer inside the launcher's handoff. */
const val MANIFEST_SERVER_HEALTH_API = "manifestServerHealthApi"

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

  // The manifest server's /health with its own short timeouts: the shared client waits up to 70s for a check, and the
  // health call runs inside the launcher's 10s handoff. /health isn't signed, so it needs no token.
  @Singleton
  @Named(MANIFEST_SERVER_HEALTH_API)
  @Requires(property = "$MANIFEST_SERVER_API_PREFIX.base-path", pattern = ".+")
  fun manifestServerHealthApi(
    apiConfig: AirbyteManifestServerApiClientConfig,
    config: ManifestServerCheckConfig,
  ): HealthApi =
    HealthApi(
      basePath = apiConfig.basePath,
      client =
        OkHttpClient
          .Builder()
          .connectTimeout(config.healthTimeout)
          .readTimeout(config.healthTimeout)
          // Connecting and reading together, so the whole call fits in healthTimeout.
          .callTimeout(config.healthTimeout)
          .build(),
      policy = RetryPolicy.builder<Response>().withMaxRetries(0).build(),
    )

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
