/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.persistence;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Singleton;
import java.io.IOException;
import java.util.UUID;

/**
 * Loads configured config injections for a given actor definition id and injects them into the
 * given configuration under the specified path.
 */
@Singleton
@Requires(bean = ConfigRepository.class)
public class ConfigInjector {

  private final ConfigRepository configRepository;

  public ConfigInjector(final ConfigRepository configRepository) {
    this.configRepository = configRepository;
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
    configRepository.getActorDefinitionConfigInjections(actorDefinitionId).forEach(injection -> {
      ((ObjectNode) configuration).set(injection.getInjectionPath(), injection.getJsonToInject());
    });
    return configuration;
  }

}
