/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.hydration

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.config.StandardDiscoverCatalogInput

class DiscoverCatalogInputHydrator(
  private val hydrator: ConnectorSecretsHydrator,
) {
  fun getHydratedStandardDiscoverInput(rawInput: StandardDiscoverCatalogInput): StandardDiscoverCatalogInput {
    val fullConfig: JsonNode? =
      hydrator.hydrateConfig(
        rawInput.connectionConfiguration,
        SecretHydrationContext(
          organizationId = rawInput.actorContext.organizationId,
          workspaceId = rawInput.actorContext.workspaceId,
        ),
      )

    return StandardDiscoverCatalogInput()
      .withActorContext(rawInput.actorContext)
      .withConfigHash(rawInput.configHash)
      .withSourceId(rawInput.sourceId)
      .withConnectionConfiguration(fullConfig)
  }
}
