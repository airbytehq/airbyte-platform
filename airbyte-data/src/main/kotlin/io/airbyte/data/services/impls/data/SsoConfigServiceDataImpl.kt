/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.data

import io.airbyte.data.ConfigNotFoundException
import io.airbyte.data.repositories.SsoConfigRepository
import io.airbyte.data.services.SsoConfigService
import io.airbyte.data.services.impls.data.mappers.toConfigModel
import io.airbyte.data.services.impls.data.mappers.toEntity
import io.airbyte.domain.models.SsoConfig
import io.airbyte.domain.models.SsoConfigStatus
import jakarta.inject.Singleton
import java.util.UUID
import io.airbyte.data.repositories.entities.SsoConfig as SsoConfigEntity

@Singleton
open class SsoConfigServiceDataImpl internal constructor(
  private val ssoConfigRepository: SsoConfigRepository,
) : SsoConfigService {
  override fun createSsoConfig(config: SsoConfig) {
    ssoConfigRepository
      .save(
        SsoConfigEntity(
          id = UUID.randomUUID(),
          organizationId = config.organizationId,
          keycloakRealm = config.companyIdentifier,
          status = config.status.toEntity(),
        ),
      )
  }

  override fun deleteSsoConfig(organizationId: UUID) = ssoConfigRepository.deleteByOrganizationId(organizationId)

  override fun getSsoConfig(organizationId: UUID): io.airbyte.config.SsoConfig? =
    ssoConfigRepository.findByOrganizationId(organizationId)?.toConfigModel()

  override fun getSsoConfigByCompanyIdentifier(companyIdentifier: String): io.airbyte.config.SsoConfig? =
    ssoConfigRepository.findByKeycloakRealm(companyIdentifier)?.toConfigModel()

  override fun getSsoConfigByRealmName(realmName: String): io.airbyte.config.SsoConfig? =
    ssoConfigRepository.findByKeycloakRealm(realmName)?.toConfigModel()

  override fun updateSsoConfigStatus(
    organizationId: UUID,
    status: SsoConfigStatus,
  ) {
    val entity =
      ssoConfigRepository.findByOrganizationId(organizationId) ?: throw ConfigNotFoundException(
        SsoConfig::class.toString(),
        organizationId.toString(),
      )
    entity.status = status.toEntity()
    ssoConfigRepository.update(entity)
  }
}
