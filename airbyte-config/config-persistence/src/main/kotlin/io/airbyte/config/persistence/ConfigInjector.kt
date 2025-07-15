/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.persistence

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ObjectNode
import io.airbyte.config.ActorDefinitionConfigInjection
import io.airbyte.data.services.ConnectorBuilderService
import jakarta.inject.Singleton
import java.io.IOException
import java.util.UUID

/**
 * Loads configured config injections for a given actor definition id and injects them into the
 * given configuration under the specified path.
 */
@Singleton
class ConfigInjector(
  private val connectorBuilderService: ConnectorBuilderService,
) {
  /**
   * Performs the config injection.
   *
   * @param configuration The regular configuration object to inject config injections into
   * @param actorDefinitionId The actor definition id of the configuration
   * @return The configuration enriched with all config injections associated with the actor
   * definition id
   * @throws IOException exception while interacting with db
   */
  @Throws(IOException::class)
  fun injectConfig(
    configuration: JsonNode,
    actorDefinitionId: UUID,
  ): JsonNode {
    connectorBuilderService.getActorDefinitionConfigInjections(actorDefinitionId).forEach { injection: ActorDefinitionConfigInjection ->
      (configuration as ObjectNode).set<JsonNode>(injection.injectionPath, injection.jsonToInject)
    }
    return configuration
  }
}
