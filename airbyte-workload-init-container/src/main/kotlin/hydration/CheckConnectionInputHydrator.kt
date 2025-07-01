/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.initContainer.hydration

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.config.StandardCheckConnectionInput
import io.airbyte.config.secrets.InlinedConfigWithSecretRefs
import io.airbyte.config.secrets.toConfigWithRefs
import io.airbyte.workers.hydration.ConnectorSecretsHydrator
import io.airbyte.workers.hydration.SecretHydrationContext

class CheckConnectionInputHydrator(
  private val hydrator: ConnectorSecretsHydrator,
) {
  fun getHydratedStandardCheckInput(rawInput: StandardCheckConnectionInput): StandardCheckConnectionInput {
    val configWithSecretRefs =
      InlinedConfigWithSecretRefs(
        rawInput.connectionConfiguration,
      ).toConfigWithRefs()

    val fullConfig: JsonNode? =
      hydrator.hydrateConfig(
        configWithSecretRefs,
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
