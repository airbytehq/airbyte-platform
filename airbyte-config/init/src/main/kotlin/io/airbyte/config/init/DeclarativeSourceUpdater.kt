/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */
package io.airbyte.config.init

import io.airbyte.data.services.ActorDefinitionService
import io.airbyte.data.services.ConnectorBuilderService
import io.airbyte.validation.json.JsonValidationException
import jakarta.inject.Singleton
import org.slf4j.LoggerFactory
import java.io.IOException

/**
 * Helper class used to apply actor definitions from a DefinitionsProvider to the database. This is
 * here to enable easy reuse of definition application logic in bootloader and cron.
 */
@Singleton
class DeclarativeSourceUpdater(
  private val connectorBuilderService: ConnectorBuilderService,
  private val actorDefinitionService: ActorDefinitionService,
) {
  companion object {
    private val log = LoggerFactory.getLogger(DeclarativeSourceUpdater::class.java)
  }

  /**
   * Update all declarative sources with the most recent builder CDK version.
   */
  @Throws(JsonValidationException::class, IOException::class)
  fun apply() {
    TODO("this method is not called and is changed upstack")
  }
}
