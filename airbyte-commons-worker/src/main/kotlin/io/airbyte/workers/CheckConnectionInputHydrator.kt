package io.airbyte.workers

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.config.StandardCheckConnectionInput
import java.util.UUID

class CheckConnectionInputHydrator(
  private val hydrator: ConnectorSecretsHydrator,
) {
  fun getHydratedStandardCheckInput(rawInput: StandardCheckConnectionInput): StandardCheckConnectionInput {
    val organizationId: UUID? = rawInput.actorContext.organizationId

    val fullConfig: JsonNode? = hydrator.hydrateConfig(rawInput.connectionConfiguration, organizationId)

    return StandardCheckConnectionInput()
      .withActorContext(rawInput.actorContext)
      .withActorId(rawInput.actorId)
      .withActorType(rawInput.actorType)
      .withConnectionConfiguration(fullConfig)
  }
}
