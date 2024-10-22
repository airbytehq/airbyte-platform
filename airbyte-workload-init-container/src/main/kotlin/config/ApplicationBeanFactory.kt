package io.airbyte.initContainer.config

import io.airbyte.api.client.AirbyteApiClient
import io.airbyte.commons.converters.CatalogClientConverters
import io.airbyte.commons.protocol.DefaultProtocolSerializer
import io.airbyte.commons.protocol.ProtocolSerializer
import io.airbyte.config.secrets.SecretsRepositoryReader
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.metrics.lib.MetricClient
import io.airbyte.metrics.lib.NotImplementedMetricClient
import io.airbyte.workers.CheckConnectionInputHydrator
import io.airbyte.workers.ConnectorSecretsHydrator
import io.airbyte.workers.DiscoverCatalogInputHydrator
import io.airbyte.workers.ReplicationInputHydrator
import io.airbyte.workers.helper.BackfillHelper
import io.airbyte.workers.helper.ResumableFullRefreshStatsHelper
import io.micronaut.context.annotation.Factory
import jakarta.inject.Singleton

@Factory
class ApplicationBeanFactory {
  @Singleton
  fun replicationInputHydrator(
    airbyteApiClient: AirbyteApiClient,
    resumableFullRefreshStatsHelper: ResumableFullRefreshStatsHelper,
    secretsRepositoryReader: SecretsRepositoryReader,
    featureFlagClient: FeatureFlagClient,
    backfillHelper: BackfillHelper,
    catalogClientConverters: CatalogClientConverters,
  ): ReplicationInputHydrator {
    return ReplicationInputHydrator(
      airbyteApiClient,
      resumableFullRefreshStatsHelper,
      secretsRepositoryReader,
      featureFlagClient,
      backfillHelper,
      catalogClientConverters,
    )
  }

  @Singleton
  fun metricClient(): MetricClient = NotImplementedMetricClient()

  @Singleton
  fun baseConnectorInputHydrator(
    airbyteApiClient: AirbyteApiClient,
    secretsRepositoryReader: SecretsRepositoryReader,
    featureFlagClient: FeatureFlagClient,
  ): ConnectorSecretsHydrator {
    return ConnectorSecretsHydrator(
      secretsRepositoryReader = secretsRepositoryReader,
      airbyteApiClient = airbyteApiClient,
      featureFlagClient = featureFlagClient,
    )
  }

  @Singleton
  fun checkInputHydrator(connectorSecretsHydrator: ConnectorSecretsHydrator): CheckConnectionInputHydrator {
    return CheckConnectionInputHydrator(connectorSecretsHydrator)
  }

  @Singleton
  fun discoverCatalogInputHydrator(connectorSecretsHydrator: ConnectorSecretsHydrator): DiscoverCatalogInputHydrator {
    return DiscoverCatalogInputHydrator(connectorSecretsHydrator)
  }

  @Singleton
  fun protocolSerializer(): ProtocolSerializer {
    return DefaultProtocolSerializer()
  }
}
