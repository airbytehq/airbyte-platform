/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.init.config

import io.airbyte.config.Configs.SeedDefinitionsProviderType
import io.airbyte.config.init.AirbyteCompatibleConnectorsValidator
import io.airbyte.config.init.DeclarativeManifestImageVersionsProvider
import io.airbyte.config.init.DeclarativeSourceUpdater
import io.airbyte.config.specs.DefinitionsProvider
import io.airbyte.config.specs.LocalDefinitionsProvider
import io.airbyte.config.specs.RemoteDefinitionsProvider
import io.airbyte.data.services.ActorDefinitionService
import io.airbyte.data.services.DeclarativeManifestImageVersionService
import io.airbyte.featureflag.FeatureFlagClient
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.Factory
import io.micronaut.context.annotation.Value
import io.micronaut.core.util.StringUtils
import jakarta.inject.Named
import jakarta.inject.Singleton
import okhttp3.OkHttpClient

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
    seedProvider: SeedDefinitionsProviderType,
    remoteDefinitionsProvider: RemoteDefinitionsProvider,
  ): DefinitionsProvider =
    when (seedProvider) {
      SeedDefinitionsProviderType.LOCAL -> {
        log.info { "Using local definitions provider for seeding" }
        LocalDefinitionsProvider()
      }
      SeedDefinitionsProviderType.REMOTE -> {
        log.info { "Using remote definitions provider for seeding" }
        remoteDefinitionsProvider
      }
    }

  @Singleton
  fun seedDefinitionsProviderType(
    @Value("\${airbyte.connector-registry.seed-provider}") seedProvider: String,
  ): SeedDefinitionsProviderType {
    if (StringUtils.isEmpty(seedProvider) || LOCAL_SEED_PROVIDER.equals(seedProvider, ignoreCase = true)) {
      return SeedDefinitionsProviderType.LOCAL
    } else if (REMOTE_SEED_PROVIDER.equals(seedProvider, ignoreCase = true)) {
      return SeedDefinitionsProviderType.REMOTE
    }

    throw IllegalArgumentException("Invalid seed provider: $seedProvider")
  }

  @Singleton
  @Named("remoteDeclarativeSourceUpdater")
  fun remoteDeclarativeSourceUpdater(
    @Named("remoteDeclarativeManifestImageVersionsProvider") declarativeManifestImageVersionsProvider: DeclarativeManifestImageVersionsProvider,
    declarativeManifestImageVersionService: DeclarativeManifestImageVersionService,
    actorDefinitionService: ActorDefinitionService,
    airbyteCompatibleConnectorsValidator: AirbyteCompatibleConnectorsValidator,
    featureFlagClient: FeatureFlagClient,
  ): DeclarativeSourceUpdater =
    DeclarativeSourceUpdater(
      declarativeManifestImageVersionsProvider,
      declarativeManifestImageVersionService,
      actorDefinitionService,
      airbyteCompatibleConnectorsValidator,
      featureFlagClient,
    )

  @Singleton
  @Named("localDeclarativeSourceUpdater")
  fun localDeclarativeSourceUpdater(
    @Named("localDeclarativeManifestImageVersionsProvider") declarativeManifestImageVersionsProvider: DeclarativeManifestImageVersionsProvider,
    declarativeManifestImageVersionService: DeclarativeManifestImageVersionService,
    actorDefinitionService: ActorDefinitionService,
    airbyteCompatibleConnectorsValidator: AirbyteCompatibleConnectorsValidator,
    featureFlagClient: FeatureFlagClient,
  ): DeclarativeSourceUpdater =
    DeclarativeSourceUpdater(
      declarativeManifestImageVersionsProvider,
      declarativeManifestImageVersionService,
      actorDefinitionService,
      airbyteCompatibleConnectorsValidator,
      featureFlagClient,
    )

  @Singleton
  @Named("dockerHubOkHttpClient")
  fun okHttpClient(): OkHttpClient = OkHttpClient()

  companion object {
    private val log = KotlinLogging.logger {}

    private const val LOCAL_SEED_PROVIDER = "LOCAL"
    private const val REMOTE_SEED_PROVIDER = "REMOTE"
  }
}
