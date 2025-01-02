/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.keycloak.setup;

import io.airbyte.commons.auth.config.OidcConfig;
import jakarta.inject.Singleton;
import jakarta.ws.rs.core.Response;
import java.lang.invoke.MethodHandles;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.keycloak.admin.client.resource.RealmResource;
import org.keycloak.representations.idm.IdentityProviderRepresentation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is responsible for configuring an identity provider. It creates and manages various
 * identity providers for authentication purposes.
 */
@Singleton
@SuppressWarnings("PMD.LiteralsFirstInComparisons")
public class IdentityProvidersConfigurator {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  static final String AIRBYTE_MANAGED_IDP_KEY = "airbyte-managed-idp";
  static final String AIRBYTE_MANAGED_IDP_VALUE = "true";
  private static final String KEYCLOAK_PROVIDER_ID = "oidc"; // OIDC is the only supported provider ID for now

  private final ConfigurationMapService configurationMapService;
  private final Optional<OidcConfig> oidcConfig;

  public IdentityProvidersConfigurator(final ConfigurationMapService configurationMapService,
                                       final Optional<OidcConfig> oidcConfig) {
    this.configurationMapService = configurationMapService;
    this.oidcConfig = oidcConfig;
  }

  public void configureIdp(final RealmResource keycloakRealm) {
    if (oidcConfig.isEmpty()) {
      log.info("No identity provider configuration found. Skipping IDP setup.");
      return;
    }

    final IdentityProviderRepresentation idp = buildIdpFromConfig(keycloakRealm, oidcConfig.get());

    final List<IdentityProviderRepresentation> existingIdps = keycloakRealm.identityProviders().findAll();
    // if no IDPs exist, create one and mark it as airbyte-managed
    if (existingIdps.isEmpty()) {
      log.info("No existing identity providers found. Creating new IDP.");
      createNewIdp(keycloakRealm, idp);
      return;
    }

    // Look for an IDP with the AIRBYTE_MANAGED_IDP_KEY/VALUE in its config. This allows keycloak-setup
    // to programmatically
    // configure a specific IDP, even if the realm contains multiple.
    final List<IdentityProviderRepresentation> existingManagedIdps = existingIdps.stream()
        .filter(existingIdp -> existingIdp.getConfig().getOrDefault(AIRBYTE_MANAGED_IDP_KEY, "false").equals(AIRBYTE_MANAGED_IDP_VALUE))
        .toList();

    int expNumManagedIdp = 1;
    if (existingManagedIdps.size() > expNumManagedIdp) {
      log.warn(
          "Found multiple IDPs with Config entry {}={}. This isn't supported, as keycloak-setup only supports one managed IDP. Skipping IDP update.",
          AIRBYTE_MANAGED_IDP_KEY, AIRBYTE_MANAGED_IDP_VALUE);
      return;
    }

    if (existingManagedIdps.size() == expNumManagedIdp) {
      log.info("Found existing managed IDP. Updating it.");
      updateExistingIdp(keycloakRealm, existingManagedIdps.getFirst(), idp);
      return;
    }

    // if no managed IDPs exist, but there is exactly one IDP, update it and mark it as airbyte-managed
    if (existingIdps.size() == expNumManagedIdp) {
      log.info("Found exactly one existing IDP. Updating it and marking it as airbyte-managed.");
      updateExistingIdp(keycloakRealm, existingIdps.getFirst(), idp);
      return;
    }

    // if there are multiple IDPs and none are managed, log a warning and do nothing.
    log.warn("Multiple identity providers exist and none are marked as airbyte-managed. Skipping IDP update. If you want your OIDC configuration to "
        + "apply to a specific IDP, please add a Config entry with key {} and value {} to that IDP and try again.",
        AIRBYTE_MANAGED_IDP_KEY, AIRBYTE_MANAGED_IDP_VALUE);
  }

  private void createNewIdp(final RealmResource keycloakRealm, final IdentityProviderRepresentation idp) {
    try (final Response response = keycloakRealm.identityProviders().create(idp)) {
      if (response.getStatus() == Response.Status.CREATED.getStatusCode()) {
        log.info("Identity Provider {} created successfully!", idp.getAlias());
      } else {
        final String errorMessage = String.format("Failed to create Identity Provider.\nReason: %s\nResponse: %s",
            response.getStatusInfo().getReasonPhrase(), response.readEntity(String.class));
        log.error(errorMessage);
        throw new RuntimeException(errorMessage);
      }
    }
  }

  private void updateExistingIdp(final RealmResource keycloakRealm,
                                 final IdentityProviderRepresentation existingIdp,
                                 final IdentityProviderRepresentation updatedIdp) {
    // In order to apply the updated IDP configuration to the existing IDP within Keycloak, we need to
    // set the internal ID of the existing IDP.
    updatedIdp.setInternalId(existingIdp.getInternalId());
    keycloakRealm.identityProviders().get(existingIdp.getAlias()).update(updatedIdp);
  }

  private IdentityProviderRepresentation buildIdpFromConfig(final RealmResource keycloakRealm, final OidcConfig oidcConfig) {
    final IdentityProviderRepresentation idp = new IdentityProviderRepresentation();
    idp.setDisplayName(oidcConfig.getDisplayName());
    idp.setAlias(oidcConfig.getAppName());
    idp.setProviderId(KEYCLOAK_PROVIDER_ID);
    idp.setEnabled(true);

    final Map<String, String> configMap = configurationMapService.importProviderFrom(keycloakRealm, oidcConfig, idp.getProviderId());
    final Map<String, String> config = configurationMapService.setupProviderConfig(oidcConfig, configMap);

    // mark the IDP as airbyte-managed so that it can be programmatically updated in the future.
    config.put(AIRBYTE_MANAGED_IDP_KEY, AIRBYTE_MANAGED_IDP_VALUE);
    idp.setConfig(config);

    return idp;
  }

}
