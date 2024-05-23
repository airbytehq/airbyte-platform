/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */
package io.airbyte.config.init.config

import io.airbyte.config.init.DeclarativeManifestImageVersionsProvider
import io.airbyte.config.init.DeclarativeSourceUpdater
import io.airbyte.config.specs.DefinitionsProvider
import io.airbyte.config.specs.LocalDefinitionsProvider
import io.airbyte.config.specs.RemoteDefinitionsProvider
import io.airbyte.data.services.ActorDefinitionService
import io.airbyte.data.services.DeclarativeManifestImageVersionService
import io.micronaut.context.annotation.Factory
import io.micronaut.context.annotation.Value
import io.micronaut.core.util.StringUtils
import jakarta.inject.Named
import jakarta.inject.Singleton
import org.slf4j.Logger
import org.slf4j.LoggerFactory

/**
 * Micronaut bean factory for singletons related to seeding the database.
 */
@Factory
class SeedBeanFactory {
  /**
   * Creates a singleton [DefinitionsProvider] based on the seed provider specified in the
   * config, to be used when loading definitions into the DB in cron or bootloader.
   */
  @Singleton
  @Named("seedDefinitionsProvider")
  fun seedDefinitionsProvider(
    @Value("\${airbyte.connector-registry.seed-provider}") seedProvider: String,
    remoteDefinitionsProvider: RemoteDefinitionsProvider,
  ): DefinitionsProvider {
    if (StringUtils.isEmpty(seedProvider) || LOCAL_SEED_PROVIDER.equals(seedProvider, ignoreCase = true)) {
      LOGGER.info("Using local definitions provider for seeding")
      return LocalDefinitionsProvider()
    } else if (REMOTE_SEED_PROVIDER.equals(seedProvider, ignoreCase = true)) {
      LOGGER.info("Using remote definitions provider for seeding")
      return remoteDefinitionsProvider
    }

    throw IllegalArgumentException("Invalid seed provider: $seedProvider")
  }

  @Singleton
  @Named("localDeclarativeSourceUpdater")
  fun localDeclarativeSourceUpdater(
    @Named("localDeclarativeManifestImageVersionsProvider") declarativeManifestImageVersionsProvider: DeclarativeManifestImageVersionsProvider,
    declarativeManifestImageVersionService: DeclarativeManifestImageVersionService,
    actorDefinitionService: ActorDefinitionService,
  ): DeclarativeSourceUpdater {
    return DeclarativeSourceUpdater(declarativeManifestImageVersionsProvider, declarativeManifestImageVersionService, actorDefinitionService)
  }

  companion object {
    private val LOGGER: Logger = LoggerFactory.getLogger(SeedBeanFactory::class.java)

    private const val LOCAL_SEED_PROVIDER = "LOCAL"
    private const val REMOTE_SEED_PROVIDER = "REMOTE"
  }
}
