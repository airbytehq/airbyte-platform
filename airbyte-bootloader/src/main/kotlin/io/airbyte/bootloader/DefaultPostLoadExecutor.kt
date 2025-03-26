/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.bootloader

import io.airbyte.config.init.ApplyDefinitionsHelper
import io.airbyte.config.init.DeclarativeSourceUpdater
import io.airbyte.config.init.PostLoadExecutor
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Named
import jakarta.inject.Singleton

private val log = KotlinLogging.logger {}

/**
 * Default implementation of the tasks that should be executed after a successful bootstrapping of
 * the Airbyte environment.
 *
 * This implementation performs the following tasks:
 * Applies the latest definitions from the provider to the repository
 * If enabled, migrates secrets
 */
@Singleton
class DefaultPostLoadExecutor(
  private val applyDefinitionsHelper: ApplyDefinitionsHelper,
  @param:Named("localDeclarativeSourceUpdater") private val declarativeSourceUpdater: DeclarativeSourceUpdater,
  private val authSecretInitializer: AuthKubernetesSecretInitializer?,
) : PostLoadExecutor {
  override fun execute() {
    log.info { "Updating connector definitions" }
    applyDefinitionsHelper.apply(false, true)
    log.info { "Done updating connector definitions" }
    declarativeSourceUpdater.apply()
    log.info { "Loaded seed data." }

    if (authSecretInitializer != null) {
      log.info { "Initializing auth secrets" }
      authSecretInitializer.initializeSecrets()
      log.info { "Done initializing auth secrets" }
    } else {
      log.info { "Auth secret initializer not present. Skipping." }
    }
  }
}
