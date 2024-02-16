package io.airbyte.workers

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.config.StandardDiscoverCatalogInput
import java.util.UUID

class DiscoverCatalogInputHydrator(
  private val hydrator: ConnectorSecretsHydrator,
) {
  fun getHydratedStandardDiscoverInput(rawInput: StandardDiscoverCatalogInput): StandardDiscoverCatalogInput {
    val organizationId: UUID? = rawInput.actorContext.organizationId

    val fullConfig: JsonNode? = hydrator.hydrateConfig(rawInput.connectionConfiguration, organizationId)

    return StandardDiscoverCatalogInput()
      .withActorContext(rawInput.actorContext)
      .withConfigHash(rawInput.configHash)
      .withSourceId(rawInput.sourceId)
      .withConnectionConfiguration(fullConfig)
  }
}
