/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.initContainer.hydration

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.config.StandardDiscoverCatalogInput
import io.airbyte.config.secrets.InlinedConfigWithSecretRefs
import io.airbyte.config.secrets.toConfigWithRefs
import io.airbyte.workers.hydration.ConnectorSecretsHydrator
import io.airbyte.workers.hydration.SecretHydrationContext

class DiscoverCatalogInputHydrator(
  private val hydrator: ConnectorSecretsHydrator,
) {
  fun getHydratedStandardDiscoverInput(rawInput: StandardDiscoverCatalogInput): StandardDiscoverCatalogInput {
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

    return StandardDiscoverCatalogInput()
      .withActorContext(rawInput.actorContext)
      .withConfigHash(rawInput.configHash)
      .withConnectorVersion(rawInput.connectorVersion)
      .withSourceId(rawInput.sourceId)
      .withConnectionConfiguration(fullConfig)
  }
}
