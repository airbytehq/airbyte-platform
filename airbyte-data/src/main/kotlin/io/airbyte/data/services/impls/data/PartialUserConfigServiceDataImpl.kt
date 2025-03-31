/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.data

import io.airbyte.data.repositories.PartialUserConfigRepository
import io.airbyte.data.repositories.entities.PartialUserConfig
import io.airbyte.data.services.PartialUserConfigService
import jakarta.inject.Singleton
import java.util.UUID

@Singleton
open class PartialUserConfigServiceDataImpl(
  private val repository: PartialUserConfigRepository,
) : PartialUserConfigService {
  override fun getPartialUserConfig(partialUserConfigId: UUID): PartialUserConfig =
    repository.findById(partialUserConfigId).orElseThrow {
      throw RuntimeException("PartialUserConfig not found")
    }

  override fun listPartialUserConfigs(workspaceId: UUID): List<PartialUserConfig> = repository.findByWorkspaceId(workspaceId)

  override fun createPartialUserConfig(partialUserConfigCreate: PartialUserConfig): PartialUserConfig = repository.save(partialUserConfigCreate)
}
