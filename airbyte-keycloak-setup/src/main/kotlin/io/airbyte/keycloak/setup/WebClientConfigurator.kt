/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.keycloak.setup

import io.airbyte.commons.auth.config.AirbyteKeycloakConfiguration
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Named
import jakarta.inject.Singleton
import org.keycloak.admin.client.resource.RealmResource
import org.keycloak.representations.idm.ClientRepresentation

/**
 * This class provides a web client. It can create and configure the client based on specified
 * parameters.
 */
@Singleton
class WebClientConfigurator(
  @param:Named("airbyteUrl") private val airbyteUrl: String,
  private val keycloakConfiguration: AirbyteKeycloakConfiguration,
) {
  fun configureWebClient(keycloakRealm: RealmResource) {
    val clientConfig = clientRepresentationFromConfig

    val existingClient =
      keycloakRealm
        .clients()
        .findByClientId(clientConfig.clientId)
        .stream()
        .findFirst()

    if (existingClient.isPresent) {
      keycloakRealm.clients()[existingClient.get().id].update(applyConfigToExistingClientRepresentation(existingClient.get()))
      log.info(clientConfig.clientId + " client updated successfully.")
    } else {
      keycloakRealm.clients().create(clientConfig).use { response ->
        if (response.status == HTTP_STATUS_CREATED) {
          log.info(clientConfig.clientId + " client created successfully. Status: " + response.statusInfo)
        } else {
          val errorMessage =
            String.format(
              "Failed to create %s client.\nReason: %s\nResponse: %s",
              clientConfig.clientId,
              response.statusInfo.reasonPhrase,
              response.readEntity(String::class.java),
            )
          log.error(errorMessage)
          throw RuntimeException(errorMessage)
        }
      }
    }
  }

  val clientRepresentationFromConfig: ClientRepresentation
    get() {
      val client = ClientRepresentation()
      client.clientId = keycloakConfiguration.webClientId
      client.isPublicClient = true // Client authentication disabled
      client.isDirectAccessGrantsEnabled = true // Standard flow authentication
      client.redirectUris = getWebClientRedirectUris(airbyteUrl)
      client.attributes = clientAttributes

      return client
    }

  private fun applyConfigToExistingClientRepresentation(clientRepresentation: ClientRepresentation): ClientRepresentation {
    // only change the attributes that come from external configuration
    clientRepresentation.redirectUris = getWebClientRedirectUris(airbyteUrl)
    return clientRepresentation
  }

  private val clientAttributes: Map<String, String>
    get() {
      val attributeMap: MutableMap<String, String> = HashMap()
      attributeMap["access.token.lifespan"] = "180"
      return attributeMap
    }

  private fun getWebClientRedirectUris(airbyteUrl: String): List<String> {
    val normalizedWebappUrl = if (airbyteUrl.endsWith("/")) airbyteUrl else "$airbyteUrl/"
    return java.util.List.of("$normalizedWebappUrl*", LOCAL_OSS_DEV_URI, LOCAL_CLOUD_DEV_URI)
  }

  companion object {
    private val log = KotlinLogging.logger {}

    const val HTTP_STATUS_CREATED: Int = 201
    private const val LOCAL_OSS_DEV_URI = "https://localhost:3000/*"
    private const val LOCAL_CLOUD_DEV_URI = "https://localhost:3001/*"
  }
}
