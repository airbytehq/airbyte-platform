/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services

import io.airbyte.config.PartialUserConfig
import io.airbyte.config.PartialUserConfigWithActorDetails
import java.util.UUID

/**
 * A service that manages config templates
 */
interface PartialUserConfigService {
  fun getPartialUserConfig(partialUserConfigId: UUID): PartialUserConfigWithActorDetails

  fun listPartialUserConfigs(workspaceId: UUID): List<PartialUserConfigWithActorDetails>

  fun createPartialUserConfig(partialUserConfigCreate: PartialUserConfig): PartialUserConfigWithActorDetails

  fun updatePartialUserConfig(partialUserConfig: PartialUserConfig): PartialUserConfigWithActorDetails
}
