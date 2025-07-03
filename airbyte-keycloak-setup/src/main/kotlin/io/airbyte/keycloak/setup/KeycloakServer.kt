/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.keycloak.setup

import io.airbyte.commons.auth.config.AirbyteKeycloakConfiguration
import io.airbyte.commons.auth.keycloak.ClientScopeConfigurator
import jakarta.inject.Named
import jakarta.inject.Singleton
import org.keycloak.admin.client.Keycloak
import org.keycloak.admin.client.resource.RealmResource
import org.keycloak.representations.idm.RealmRepresentation
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.lang.invoke.MethodHandles

/**
 * This class represents the Keycloak server. It contains methods to register an initial user, web
 * client and identity provider
 */
@Singleton
class KeycloakServer(
  private val keycloakAdminClientProvider: KeycloakAdminClientProvider,
  private val keycloakConfiguration: AirbyteKeycloakConfiguration,
  private val userConfigurator: UserConfigurator,
  private val webClientConfigurator: WebClientConfigurator,
  private val identityProvidersConfigurator: IdentityProvidersConfigurator,
  private val clientScopeConfigurator: ClientScopeConfigurator,
  @Named("airbyteUrl") airbyteUrl: String,
) {
  private val keycloakAdminClient: Keycloak
  private val airbyteUrl: String

  init {
    this.keycloakAdminClient = initializeKeycloakAdminClient()
    this.airbyteUrl = airbyteUrl
  }

  fun setupAirbyteRealm() {
    if (airbyteRealmDoesNotExist()) {
      log.info("Creating realm {}...", keycloakConfiguration.airbyteRealm)
      createRealm()
      log.info("Realm created successfully.")
    }
    configureRealm()
    log.info("Realm configured successfully.")
  }

  private fun airbyteRealmDoesNotExist(): Boolean =
    keycloakAdminClient
      .realms()
      .findAll()
      .stream()
      .noneMatch { realmRepresentation: RealmRepresentation -> realmRepresentation.realm == keycloakConfiguration.airbyteRealm }

  private fun createRealm() {
    log.info("Creating realm {}...", keycloakConfiguration.airbyteRealm)
    val airbyteRealmRepresentation = buildRealmRepresentation()
    keycloakAdminClient.realms().create(airbyteRealmRepresentation)
  }

  private fun configureRealm() {
    val airbyteRealm = keycloakAdminClient.realm(keycloakConfiguration.airbyteRealm)

    // ensure webapp-url is applied as the frontendUrl before other configurations are updated
    updateRealmFrontendUrl(airbyteRealm)

    userConfigurator.configureUser(airbyteRealm)
    webClientConfigurator.configureWebClient(airbyteRealm)
    identityProvidersConfigurator.configureIdp(airbyteRealm)
    clientScopeConfigurator.configureClientScope(airbyteRealm)
  }

  private fun buildRealmRepresentation(): RealmRepresentation {
    val airbyteRealmRepresentation = RealmRepresentation()
    airbyteRealmRepresentation.realm = keycloakConfiguration.airbyteRealm
    airbyteRealmRepresentation.isEnabled = true
    airbyteRealmRepresentation.loginTheme = "airbyte-keycloak-theme"
    return airbyteRealmRepresentation
  }

  private fun updateRealmFrontendUrl(realm: RealmResource) {
    val realmRep = realm.toRepresentation()
    val attributes = realmRep.attributesOrEmpty
    attributes[FRONTEND_URL_ATTRIBUTE] = airbyteUrl + keycloakConfiguration.basePath
    realmRep.attributes = attributes
    realm.update(realmRep)
  }

  private fun initializeKeycloakAdminClient(): Keycloak = keycloakAdminClientProvider.createKeycloakAdminClient(keycloakServerUrl)

  fun closeKeycloakAdminClient() {
    if (this.keycloakAdminClient != null) {
      keycloakAdminClient.close()
    }
  }

  val keycloakServerUrl: String
    get() {
      val basePath = keycloakConfiguration.basePath
      val basePathWithLeadingSlash = if (basePath.startsWith("/")) basePath else "/$basePath"
      return keycloakConfiguration.protocol + "://" + keycloakConfiguration.host + basePathWithLeadingSlash
    }

  // Should no longer be needed now that the realm is always updated on each run.
  // Leaving it in for now in case any issues pop up and users need a way to reset their realm
  // from scratch. We should remove this once we're confident that users no longer ever need to
  // do this hard reset.
  @Deprecated("")
  fun destroyAndRecreateAirbyteRealm() {
    if (airbyteRealmDoesNotExist()) {
      log.info("Ignoring reset because realm {} does not exist. Creating it...", keycloakConfiguration.airbyteRealm)
      setupAirbyteRealm()
      return
    }
    log.info("Recreating realm {}...", keycloakConfiguration.airbyteRealm)
    val airbyteRealm = keycloakAdminClient.realm(keycloakConfiguration.airbyteRealm)
    airbyteRealm.remove()
    log.info("Realm removed successfully. Recreating...")
    createRealm()
    log.info("Realm recreated successfully. Configuring...")
    configureRealm()
    log.info("Realm configured successfully.")
  }

  companion object {
    private val log: Logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass())

    private const val FRONTEND_URL_ATTRIBUTE = "frontendUrl"
  }
}
