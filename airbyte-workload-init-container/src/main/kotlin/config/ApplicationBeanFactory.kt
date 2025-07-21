/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.initContainer.config

import io.airbyte.api.client.AirbyteApiClient
import io.airbyte.commons.converters.CatalogClientConverters
import io.airbyte.commons.protocol.DefaultProtocolSerializer
import io.airbyte.commons.protocol.ProtocolSerializer
import io.airbyte.config.secrets.SecretsRepositoryReader
import io.airbyte.config.secrets.persistence.SecretPersistence
import io.airbyte.initContainer.hydration.CheckConnectionInputHydrator
import io.airbyte.initContainer.hydration.DiscoverCatalogInputHydrator
import io.airbyte.metrics.MetricClient
import io.airbyte.workers.ReplicationInputHydrator
import io.airbyte.workers.helper.BackfillHelper
import io.airbyte.workers.helper.MapperSecretHydrationHelper
import io.airbyte.workers.helper.ResumableFullRefreshStatsHelper
import io.airbyte.workers.hydration.ConnectorSecretsHydrator
import io.airbyte.workers.input.ReplicationInputMapper
import io.micronaut.context.annotation.Factory
import io.micronaut.context.annotation.Value
import jakarta.inject.Singleton

@Factory
class ApplicationBeanFactory {
  @Singleton
  fun replicationInputHydrator(
    airbyteApiClient: AirbyteApiClient,
    resumableFullRefreshStatsHelper: ResumableFullRefreshStatsHelper,
    mapperSecretHydrationHelper: MapperSecretHydrationHelper,
    connectorSecretsHydrator: ConnectorSecretsHydrator,
    backfillHelper: BackfillHelper,
    catalogClientConverters: CatalogClientConverters,
    metricClient: MetricClient,
    mapper: ReplicationInputMapper,
    @Value("\${airbyte.secret.use-runtime-persistence}") useRuntimeSecretPersistence: Boolean,
  ): ReplicationInputHydrator =
    ReplicationInputHydrator(
      airbyteApiClient,
      resumableFullRefreshStatsHelper,
      mapperSecretHydrationHelper,
      backfillHelper,
      catalogClientConverters,
      mapper,
      metricClient,
      connectorSecretsHydrator,
      useRuntimeSecretPersistence,
    )

  @Singleton
  fun baseConnectorInputHydrator(
    airbyteApiClient: AirbyteApiClient,
    metricClient: MetricClient,
    secretsRepositoryReader: SecretsRepositoryReader,
    @Value("\${airbyte.secret.use-runtime-persistence}") useRuntimeSecretPersistence: Boolean,
    defaultSecretPersistence: SecretPersistence,
  ): ConnectorSecretsHydrator =
    ConnectorSecretsHydrator(
      secretsRepositoryReader = secretsRepositoryReader,
      airbyteApiClient = airbyteApiClient,
      useRuntimeSecretPersistence = useRuntimeSecretPersistence,
      environmentSecretPersistence = defaultSecretPersistence,
      metricClient = metricClient,
    )

  @Singleton
  fun checkInputHydrator(connectorSecretsHydrator: ConnectorSecretsHydrator): CheckConnectionInputHydrator =
    CheckConnectionInputHydrator(connectorSecretsHydrator)

  @Singleton
  fun discoverCatalogInputHydrator(connectorSecretsHydrator: ConnectorSecretsHydrator): DiscoverCatalogInputHydrator =
    DiscoverCatalogInputHydrator(connectorSecretsHydrator)

  @Singleton
  fun protocolSerializer(): ProtocolSerializer = DefaultProtocolSerializer()
}
