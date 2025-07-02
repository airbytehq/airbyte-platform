/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.data

import io.airbyte.data.repositories.SsoConfigRepository
import io.airbyte.data.services.SsoConfigService
import io.airbyte.domain.models.SsoConfig
import jakarta.inject.Singleton
import java.util.UUID

@Singleton
open class SsoConfigServiceDataImpl internal constructor(
  private val ssoConfigRepository: SsoConfigRepository,
) : SsoConfigService {
  override fun createSsoConfig(config: SsoConfig) {
    ssoConfigRepository
      .save(
        io.airbyte.data.repositories.entities.SsoConfig(
          id = UUID.randomUUID(),
          organizationId = config.organizationId,
          keycloakRealm = config.companyIdentifier,
        ),
      )
  }
}
