/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.init;

import io.airbyte.config.persistence.ConfigRepository;
import io.airbyte.validation.json.JsonValidationException;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Singleton;
import java.io.IOException;
import java.util.List;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;

/**
 * Helper class used to apply actor definitions from a DefinitionsProvider to the database. This is
 * here to enable easy reuse of definition application logic in bootloader and cron.
 */
@Singleton
@Requires(bean = ConfigRepository.class)
@Requires(bean = CdkVersionProvider.class)
@Slf4j
public class DeclarativeSourceUpdater {

  private final ConfigRepository configRepository;
  private final CdkVersionProvider cdkVersionProvider;

  public DeclarativeSourceUpdater(final ConfigRepository configRepository, final CdkVersionProvider cdkVersionProvider) {
    this.cdkVersionProvider = cdkVersionProvider;

    this.configRepository = configRepository;
  }

  /**
   * Update all declarative sources with the most recent builder CDK version.
   */
  public void apply() throws JsonValidationException, IOException {
    final String cdkVersion = cdkVersionProvider.getCdkVersion();
    if (cdkVersion == null) {
      throw new RuntimeException("Builder CDK version not provided");
    }
    final List<UUID> actorDefinitionsToUpdate = configRepository.getActorDefinitionIdsWithActiveDeclarativeManifest().toList();
    if (actorDefinitionsToUpdate.isEmpty()) {
      log.info("No declarative sources to update");
      return;
    }

    final int updatedDefinitions = configRepository.updateActorDefinitionsDockerImageTag(actorDefinitionsToUpdate, cdkVersion);
    log.info("Updated %d / %d declarative definitions to CDK version %s".formatted(updatedDefinitions, actorDefinitionsToUpdate.size(), cdkVersion));
  }

}
