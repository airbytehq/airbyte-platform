/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services

import io.airbyte.domain.models.SsoConfig
import io.airbyte.domain.models.SsoConfigStatus
import java.util.UUID

interface SsoConfigService {
  fun createSsoConfig(config: SsoConfig)

  fun deleteSsoConfig(organizationId: UUID)

  fun getSsoConfig(organizationId: UUID): io.airbyte.config.SsoConfig?

  fun getSsoConfigByCompanyIdentifier(companyIdentifier: String): io.airbyte.config.SsoConfig?

  fun getSsoConfigByRealmName(realmName: String): io.airbyte.config.SsoConfig?

  fun updateSsoConfigStatus(
    organizationId: UUID,
    status: SsoConfigStatus,
  )
}
