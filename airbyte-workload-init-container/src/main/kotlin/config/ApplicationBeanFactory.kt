/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.initContainer.config

import io.airbyte.api.client.AirbyteApiClient
import io.airbyte.commons.converters.CatalogClientConverters
import io.airbyte.commons.protocol.DefaultProtocolSerializer
import io.airbyte.commons.protocol.ProtocolSerializer
import io.airbyte.config.secrets.SecretsRepositoryReader
import io.airbyte.metrics.MetricClient
import io.airbyte.workers.CheckConnectionInputHydrator
import io.airbyte.workers.ConnectorSecretsHydrator
import io.airbyte.workers.DiscoverCatalogInputHydrator
import io.airbyte.workers.ReplicationInputHydrator
import io.airbyte.workers.helper.BackfillHelper
import io.airbyte.workers.helper.MapperSecretHydrationHelper
import io.airbyte.workers.helper.ResumableFullRefreshStatsHelper
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
    secretsRepositoryReader: SecretsRepositoryReader,
    mapperSecretHydrationHelper: MapperSecretHydrationHelper,
    backfillHelper: BackfillHelper,
    catalogClientConverters: CatalogClientConverters,
    metricClient: MetricClient,
    mapper: ReplicationInputMapper,
    @Value("\${airbyte.secret.use-runtime-persistence}") useRuntimeSecretPersistence: Boolean,
  ): ReplicationInputHydrator =
    ReplicationInputHydrator(
      airbyteApiClient,
      resumableFullRefreshStatsHelper,
      secretsRepositoryReader,
      mapperSecretHydrationHelper,
      backfillHelper,
      catalogClientConverters,
      mapper,
      metricClient,
      useRuntimeSecretPersistence,
    )

  @Singleton
  fun baseConnectorInputHydrator(
    airbyteApiClient: AirbyteApiClient,
    metricClient: MetricClient,
    secretsRepositoryReader: SecretsRepositoryReader,
    @Value("\${airbyte.secret.use-runtime-persistence}") useRuntimeSecretPersistence: Boolean,
  ): ConnectorSecretsHydrator =
    ConnectorSecretsHydrator(
      secretsRepositoryReader = secretsRepositoryReader,
      airbyteApiClient = airbyteApiClient,
      useRuntimeSecretPersistence = useRuntimeSecretPersistence,
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
