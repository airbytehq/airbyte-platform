/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services

import io.airbyte.config.PartialUserConfig
import io.airbyte.config.PartialUserConfigWithActorDetails
import io.airbyte.config.PartialUserConfigWithConfigTemplateAndActorDetails
import java.util.UUID

/**
 * A service that manages config templates
 */
interface PartialUserConfigService {
  fun getPartialUserConfig(partialUserConfigId: UUID): PartialUserConfigWithConfigTemplateAndActorDetails

  fun listPartialUserConfigs(workspaceId: UUID): List<PartialUserConfigWithActorDetails>

  fun createPartialUserConfig(partialUserConfigCreate: PartialUserConfig): PartialUserConfigWithActorDetails

  fun updatePartialUserConfig(partialUserConfig: PartialUserConfig): PartialUserConfigWithActorDetails

  fun deletePartialUserConfig(partialUserConfigId: UUID)

  fun deletePartialUserConfigForSource(sourceId: UUID)
}
