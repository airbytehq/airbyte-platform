/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.keycloak.setup

import io.airbyte.micronaut.runtime.AirbyteKeycloakConfig
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.http.HttpRequest
import io.micronaut.http.client.HttpClient
import jakarta.inject.Singleton
import java.sql.SQLException

/**
 * This class is responsible for setting up the Keycloak server. It initializes and configures the
 * server according to the provided specifications.
 */
@Singleton
class KeycloakSetup(
  private val httpClient: HttpClient,
  private val keycloakServer: KeycloakServer,
  private val keycloakConfiguration: AirbyteKeycloakConfig,
  private val configDbResetHelper: ConfigDbResetHelper,
) {
  fun run() {
    try {
      val keycloakUrl = keycloakServer.keycloakServerUrl
      val response =
        httpClient
          .toBlocking()
          .exchange(HttpRequest.GET<Any>(keycloakUrl), String::class.java)

      log.info { "Keycloak server response: ${response.status}" }
      log.info { "Starting admin Keycloak client with url: $keycloakUrl" }

      if (keycloakConfiguration.resetRealm) {
        keycloakServer.destroyAndRecreateAirbyteRealm()
        log.info { "Successfully destroyed and recreated Airbyte Realm. Now deleting Airbyte User/Permission records..." }
        try {
          configDbResetHelper.deleteConfigDbUsers()
        } catch (e: SQLException) {
          log.error(e) {
            "Encountered an error while cleaning up Airbyte User/Permission records. You likely need to re-run this KEYCLOAK_RESET_REALM operation."
          }
          throw RuntimeException(e)
        }
        log.info { "Successfully cleaned existing Airbyte User/Permission records. Reset finished successfully." }
      } else {
        keycloakServer.setupAirbyteRealm()
      }
    } finally {
      keycloakServer.closeKeycloakAdminClient()
    }
  }

  companion object {
    private val log = KotlinLogging.logger {}
  }
}
