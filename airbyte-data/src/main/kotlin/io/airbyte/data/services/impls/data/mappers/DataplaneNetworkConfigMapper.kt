/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.data.mappers

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import io.airbyte.domain.models.NetworkConfig

typealias EntityDataplaneNetworkConfig = io.airbyte.data.repositories.entities.DataplaneNetworkConfig
typealias DomainDataplaneNetworkConfig = io.airbyte.domain.models.DataplaneNetworkConfig
typealias EntityCloudProvider = io.airbyte.db.instance.configs.jooq.generated.enums.CloudProvider
typealias DomainCloudProvider = io.airbyte.domain.models.CloudProvider

private val networkConfigMapper = jacksonObjectMapper()

fun EntityCloudProvider.toDomainEnum(): DomainCloudProvider =
  when (this) {
    EntityCloudProvider.aws -> DomainCloudProvider.AWS
  }

fun DomainCloudProvider.toEntityEnum(): EntityCloudProvider =
  when (this) {
    DomainCloudProvider.AWS -> EntityCloudProvider.aws
  }

fun EntityDataplaneNetworkConfig.toDomainModel(): DomainDataplaneNetworkConfig {
  val configClass =
    when (this.provider) {
      EntityCloudProvider.aws -> NetworkConfig.Aws::class.java
    }
  return DomainDataplaneNetworkConfig(
    id = this.id,
    dataplaneGroupId = this.dataplaneGroupId,
    provider = this.provider.toDomainEnum(),
    config = networkConfigMapper.readValue(this.config, configClass),
    createdAt = this.createdAt,
    updatedAt = this.updatedAt,
  )
}

fun DomainDataplaneNetworkConfig.toEntity(): EntityDataplaneNetworkConfig =
  EntityDataplaneNetworkConfig(
    id = this.id,
    dataplaneGroupId = this.dataplaneGroupId,
    provider = this.provider.toEntityEnum(),
    config = networkConfigMapper.writeValueAsString(this.config),
    createdAt = this.createdAt,
    updatedAt = this.updatedAt,
  )
