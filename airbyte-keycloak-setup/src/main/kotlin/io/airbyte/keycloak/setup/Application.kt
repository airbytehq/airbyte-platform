/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.keycloak.setup

import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.runtime.Micronaut

private val log = KotlinLogging.logger {}

/**
 * Main application entry point responsible for setting up the Keycloak server with an Airbyte
 * client.
 */
fun main(args: Array<String>) {
  try {
    val applicationContext =
      Micronaut
        .build(*args)
        .deduceCloudEnvironment(false)
        .deduceEnvironment(false)
        .start()
    val keycloakSetup = applicationContext.getBean(KeycloakSetup::class.java)
    keycloakSetup.run()
    System.exit(0)
  } catch (e: Exception) {
    log.error("Unable to setup Keycloak.", e)
    System.exit(-1)
  }
}
