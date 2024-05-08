/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */
package io.airbyte.config.init

import io.airbyte.data.services.ActorDefinitionService
import io.airbyte.data.services.ConnectorBuilderService
import io.airbyte.validation.json.JsonValidationException
import io.micronaut.context.annotation.Requires
import jakarta.inject.Singleton
import org.slf4j.LoggerFactory
import java.io.IOException

/**
 * Helper class used to apply actor definitions from a DefinitionsProvider to the database. This is
 * here to enable easy reuse of definition application logic in bootloader and cron.
 */
@Singleton
@Requires(bean = CdkVersionProvider::class)
class DeclarativeSourceUpdater(
  private val connectorBuilderService: ConnectorBuilderService,
  private val actorDefinitionService: ActorDefinitionService,
  private val cdkVersionProvider: CdkVersionProvider,
) {
  companion object {
    private val log = LoggerFactory.getLogger(DeclarativeSourceUpdater::class.java)
  }

  /**
   * Update all declarative sources with the most recent builder CDK version.
   */
  @Throws(JsonValidationException::class, IOException::class)
  fun apply() {
    val cdkVersion = cdkVersionProvider.cdkVersion
    val actorDefinitionsToUpdate = connectorBuilderService.actorDefinitionIdsWithActiveDeclarativeManifest.toList()
    if (actorDefinitionsToUpdate.isEmpty()) {
      log.info("No declarative sources to update")
      return
    }

    val updatedDefinitionsCount = actorDefinitionService.updateActorDefinitionsDockerImageTag(actorDefinitionsToUpdate, cdkVersion)
    log.info("Updated $updatedDefinitionsCount / ${actorDefinitionsToUpdate.size} declarative definitions to CDK version $cdkVersion")
  }
}
