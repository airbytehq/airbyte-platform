/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services

import io.airbyte.data.repositories.DataplaneNetworkConfigRepository
import io.airbyte.data.services.impls.data.mappers.toDomainModel
import io.airbyte.data.services.impls.data.mappers.toEntity
import io.airbyte.domain.models.CloudProvider
import io.airbyte.domain.models.DataplaneNetworkConfig
import io.airbyte.domain.models.NetworkConfig
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Singleton
import java.util.UUID

private val logger = KotlinLogging.logger {}

@Singleton
class DataplaneNetworkConfigService(
  private val repository: DataplaneNetworkConfigRepository,
) {
  fun create(
    dataplaneGroupId: UUID,
    provider: CloudProvider,
    config: NetworkConfig,
  ): DataplaneNetworkConfig {
    validateProviderMatchesConfig(provider, config)
    val domainModel =
      DataplaneNetworkConfig(
        dataplaneGroupId = dataplaneGroupId,
        provider = provider,
        config = config,
      )
    val saved = repository.save(domainModel.toEntity())
    logger.info { "Created dataplane network config ${saved.id} for dataplane group $dataplaneGroupId" }
    return saved.toDomainModel()
  }

  fun getByDataplaneGroupId(dataplaneGroupId: UUID): DataplaneNetworkConfig {
    val entity =
      repository
        .findByDataplaneGroupId(dataplaneGroupId)
        .orElseThrow { NoSuchElementException("Dataplane network config not found for dataplane group $dataplaneGroupId") }
    return entity.toDomainModel()
  }

  private fun validateProviderMatchesConfig(
    provider: CloudProvider,
    config: NetworkConfig,
  ) {
    require(provider == CloudProvider.AWS && config is NetworkConfig.Aws) {
      "Unsupported provider: $provider. Only AWS is currently supported."
    }
  }
}
