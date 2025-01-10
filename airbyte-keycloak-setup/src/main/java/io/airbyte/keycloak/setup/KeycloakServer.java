/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.keycloak.setup;

import io.airbyte.commons.auth.config.AirbyteKeycloakConfiguration;
import io.airbyte.commons.auth.keycloak.ClientScopeConfigurator;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import java.lang.invoke.MethodHandles;
import java.util.Map;
import org.keycloak.admin.client.Keycloak;
import org.keycloak.admin.client.resource.RealmResource;
import org.keycloak.representations.idm.RealmRepresentation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class represents the Keycloak server. It contains methods to register an initial user, web
 * client and identity provider
 */
@Singleton
public class KeycloakServer {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final String FRONTEND_URL_ATTRIBUTE = "frontendUrl";

  private final Keycloak keycloakAdminClient;
  private final KeycloakAdminClientProvider keycloakAdminClientProvider;
  private final AirbyteKeycloakConfiguration keycloakConfiguration;
  private final UserConfigurator userConfigurator;
  private final WebClientConfigurator webClientConfigurator;
  private final IdentityProvidersConfigurator identityProvidersConfigurator;
  private final ClientScopeConfigurator clientScopeConfigurator;
  private final String airbyteUrl;

  public KeycloakServer(final KeycloakAdminClientProvider keycloakAdminClientProvider,
                        final AirbyteKeycloakConfiguration keycloakConfiguration,
                        final UserConfigurator userConfigurator,
                        final WebClientConfigurator webClientConfigurator,
                        final IdentityProvidersConfigurator identityProvidersConfigurator,
                        final ClientScopeConfigurator clientScopeConfigurator,
                        @Named("airbyteUrl") final String airbyteUrl) {
    this.keycloakAdminClientProvider = keycloakAdminClientProvider;
    this.keycloakConfiguration = keycloakConfiguration;
    this.userConfigurator = userConfigurator;
    this.webClientConfigurator = webClientConfigurator;
    this.identityProvidersConfigurator = identityProvidersConfigurator;
    this.clientScopeConfigurator = clientScopeConfigurator;
    this.keycloakAdminClient = initializeKeycloakAdminClient();
    this.airbyteUrl = airbyteUrl;
  }

  public void setupAirbyteRealm() {
    if (airbyteRealmDoesNotExist()) {
      log.info("Creating realm {}...", keycloakConfiguration.getAirbyteRealm());
      createRealm();
      log.info("Realm created successfully.");
    }
    configureRealm();
    log.info("Realm configured successfully.");
  }

  private boolean airbyteRealmDoesNotExist() {
    return keycloakAdminClient.realms().findAll().stream()
        .noneMatch(realmRepresentation -> realmRepresentation.getRealm().equals(keycloakConfiguration.getAirbyteRealm()));
  }

  private void createRealm() {
    log.info("Creating realm {}...", keycloakConfiguration.getAirbyteRealm());
    final RealmRepresentation airbyteRealmRepresentation = buildRealmRepresentation();
    keycloakAdminClient.realms().create(airbyteRealmRepresentation);
  }

  private void configureRealm() {
    final RealmResource airbyteRealm = keycloakAdminClient.realm(keycloakConfiguration.getAirbyteRealm());

    // ensure webapp-url is applied as the frontendUrl before other configurations are updated
    updateRealmFrontendUrl(airbyteRealm);

    userConfigurator.configureUser(airbyteRealm);
    webClientConfigurator.configureWebClient(airbyteRealm);
    identityProvidersConfigurator.configureIdp(airbyteRealm);
    clientScopeConfigurator.configureClientScope(airbyteRealm);
  }

  private RealmRepresentation buildRealmRepresentation() {
    final RealmRepresentation airbyteRealmRepresentation = new RealmRepresentation();
    airbyteRealmRepresentation.setRealm(keycloakConfiguration.getAirbyteRealm());
    airbyteRealmRepresentation.setEnabled(true);
    airbyteRealmRepresentation.setLoginTheme("airbyte-keycloak-theme");
    return airbyteRealmRepresentation;
  }

  private void updateRealmFrontendUrl(final RealmResource realm) {
    final RealmRepresentation realmRep = realm.toRepresentation();
    final Map<String, String> attributes = realmRep.getAttributesOrEmpty();
    attributes.put(FRONTEND_URL_ATTRIBUTE, airbyteUrl + keycloakConfiguration.getBasePath());
    realmRep.setAttributes(attributes);
    realm.update(realmRep);
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

  // Should no longer be needed now that the realm is always updated on each run.
  // Leaving it in for now in case any issues pop up and users need a way to reset their realm
  // from scratch. We should remove this once we're confident that users no longer ever need to
  // do this hard reset.
  @Deprecated
  public void destroyAndRecreateAirbyteRealm() {
    if (airbyteRealmDoesNotExist()) {
      log.info("Ignoring reset because realm {} does not exist. Creating it...", keycloakConfiguration.getAirbyteRealm());
      setupAirbyteRealm();
      return;
    }
    log.info("Recreating realm {}...", keycloakConfiguration.getAirbyteRealm());
    final RealmResource airbyteRealm = keycloakAdminClient.realm(keycloakConfiguration.getAirbyteRealm());
    airbyteRealm.remove();
    log.info("Realm removed successfully. Recreating...");
    createRealm();
    log.info("Realm recreated successfully. Configuring...");
    configureRealm();
    log.info("Realm configured successfully.");
  }

}
