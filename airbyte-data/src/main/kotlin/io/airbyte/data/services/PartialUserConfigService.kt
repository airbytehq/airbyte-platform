/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services

import io.airbyte.data.repositories.entities.PartialUserConfig
import java.util.UUID

/**
 * A service that manages config templates
 */
interface PartialUserConfigService {
  fun getPartialUserConfig(partialUserConfigId: UUID): PartialUserConfig

  fun listPartialUserConfigs(workspaceId: UUID): List<PartialUserConfig>

  fun createPartialUserConfig(partialUserConfigCreate: PartialUserConfig): PartialUserConfig
}
