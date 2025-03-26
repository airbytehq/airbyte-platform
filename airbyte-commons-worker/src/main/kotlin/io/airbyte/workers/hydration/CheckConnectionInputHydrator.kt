/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.hydration

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.config.StandardCheckConnectionInput

class CheckConnectionInputHydrator(
  private val hydrator: ConnectorSecretsHydrator,
) {
  fun getHydratedStandardCheckInput(rawInput: StandardCheckConnectionInput): StandardCheckConnectionInput {
    val fullConfig: JsonNode? =
      hydrator.hydrateConfig(
        rawInput.connectionConfiguration,
        SecretHydrationContext(
          organizationId = rawInput.actorContext.organizationId,
          workspaceId = rawInput.actorContext.workspaceId,
        ),
      )

    return StandardCheckConnectionInput()
      .withActorContext(rawInput.actorContext)
      .withActorId(rawInput.actorId)
      .withActorType(rawInput.actorType)
      .withConnectionConfiguration(fullConfig)
  }
}
