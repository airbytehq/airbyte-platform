/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.keycloak.setup;

import io.airbyte.commons.auth.config.AirbyteKeycloakConfiguration;
import jakarta.inject.Singleton;
import lombok.extern.slf4j.Slf4j;
import org.keycloak.admin.client.Keycloak;
import org.keycloak.admin.client.resource.RealmResource;
import org.keycloak.representations.idm.RealmRepresentation;

/**
 * This class represents the Keycloak server. It contains methods to register an initial user, web
 * client and identity provider
 */
@Singleton
@Slf4j
public class KeycloakServer {

  private final Keycloak keycloakAdminClient;
  private final KeycloakAdminClientProvider keycloakAdminClientProvider;
  private final AirbyteKeycloakConfiguration keycloakConfiguration;
  private final UserCreator userCreator;
  private final WebClientCreator webClientCreator;
  private final IdentityProvidersCreator identityProvidersCreator;
  private final AccountClientUpdater accountClientUpdater;

  public KeycloakServer(final KeycloakAdminClientProvider keycloakAdminClientProvider,
                        final AirbyteKeycloakConfiguration keycloakConfiguration,
                        final UserCreator userCreator,
                        final WebClientCreator webClientCreator,
                        final IdentityProvidersCreator identityProvidersCreator,
                        final AccountClientUpdater accountClientUpdater) {
    this.keycloakAdminClientProvider = keycloakAdminClientProvider;
    this.keycloakConfiguration = keycloakConfiguration;
    this.userCreator = userCreator;
    this.webClientCreator = webClientCreator;
    this.identityProvidersCreator = identityProvidersCreator;
    this.accountClientUpdater = accountClientUpdater;
    this.keycloakAdminClient = initializeKeycloakAdminClient();
  }

  public void createAirbyteRealm() {
    if (doesRealmExist()) {
      log.info("Realm {} already exists.", keycloakConfiguration.getAirbyteRealm());

      if (keycloakConfiguration.getResetRealm()) {
        final RealmResource airbyteRealm = keycloakAdminClient.realm(keycloakConfiguration.getAirbyteRealm());
        userCreator.resetUser(airbyteRealm);
        identityProvidersCreator.resetIdentityProviders(airbyteRealm);
        log.info("Reset user and identity providers for realm {}.", keycloakConfiguration.getAirbyteRealm());
      }

      return;
    }
    createRealm();
    configureRealm();
    log.info("Configuration for realm {} created successfully.", keycloakConfiguration.getAirbyteRealm());
  }

  private boolean doesRealmExist() {
    return keycloakAdminClient.realms().findAll().stream()
        .anyMatch(realmRepresentation -> realmRepresentation.getRealm().equals(keycloakConfiguration.getAirbyteRealm()));
  }

  private void createRealm() {
    log.info("Creating realm {}...", keycloakConfiguration.getAirbyteRealm());
    final RealmRepresentation airbyteRealmRepresentation = buildRealmRepresentation();
    keycloakAdminClient.realms().create(airbyteRealmRepresentation);
  }

  private RealmRepresentation buildRealmRepresentation() {
    final RealmRepresentation airbyteRealmRepresentation = new RealmRepresentation();
    airbyteRealmRepresentation.setRealm(keycloakConfiguration.getAirbyteRealm());
    airbyteRealmRepresentation.setEnabled(true);
    airbyteRealmRepresentation.setLoginTheme("airbyte-keycloak-theme");
    return airbyteRealmRepresentation;
  }

  private void configureRealm() {
    final RealmResource airbyteRealm = keycloakAdminClient.realm(keycloakConfiguration.getAirbyteRealm());

    userCreator.createUser(airbyteRealm);
    webClientCreator.createWebClient(airbyteRealm);
    identityProvidersCreator.createIdps(airbyteRealm);
    accountClientUpdater.updateAccountClientHomeUrl(airbyteRealm);
  }

  private Keycloak initializeKeycloakAdminClient() {
    return keycloakAdminClientProvider.createKeycloakAdminClient(getKeycloakServerUrl());
  }

  public void closeKeycloakAdminClient() {
    if (this.keycloakAdminClient != null) {
      this.keycloakAdminClient.close();
    }
  }

  public final String getKeycloakServerUrl() {
    final String basePath = keycloakConfiguration.getBasePath();
    final String basePathWithLeadingSlash = basePath.startsWith("/") ? basePath : "/" + basePath;
    return keycloakConfiguration.getProtocol() + "://" + keycloakConfiguration.getHost() + basePathWithLeadingSlash;
  }

}
