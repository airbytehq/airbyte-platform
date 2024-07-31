package io.airbyte.initContainer.config

import io.airbyte.api.client.AirbyteApiClient
import io.airbyte.config.secrets.SecretsRepositoryReader
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.workers.ReplicationInputHydrator
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
  ): ReplicationInputHydrator {
    return ReplicationInputHydrator(airbyteApiClient, resumableFullRefreshStatsHelper, secretsRepositoryReader, featureFlagClient)
  }
}
