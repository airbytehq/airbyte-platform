/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.persistence;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.airbyte.data.services.ConnectorBuilderService;
import jakarta.inject.Singleton;
import java.io.IOException;
import java.util.UUID;

/**
 * Loads configured config injections for a given actor definition id and injects them into the
 * given configuration under the specified path.
 */
@Singleton
public class ConfigInjector {

  private final ConnectorBuilderService connectorBuilderService;

  public ConfigInjector(ConnectorBuilderService connectorBuilderService) {
    this.connectorBuilderService = connectorBuilderService;
  }

  /**
   * Performs the config injection.
   *
   * @param configuration The regular configuration object to inject config injections into
   * @param actorDefinitionId The actor definition id of the configuration
   * @return The configuration enriched with all config injections associated with the actor
   *         definition id
   * @throws IOException exception while interacting with db
   */
  public JsonNode injectConfig(final JsonNode configuration, final UUID actorDefinitionId) throws IOException {
    connectorBuilderService.getActorDefinitionConfigInjections(actorDefinitionId).forEach(injection -> {
      ((ObjectNode) configuration).set(injection.getInjectionPath(), injection.getJsonToInject());
    });
    return configuration;
  }

}
