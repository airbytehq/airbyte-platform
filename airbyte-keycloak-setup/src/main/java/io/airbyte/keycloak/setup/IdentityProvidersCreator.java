/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.keycloak.setup;

import io.airbyte.commons.auth.config.IdentityProviderConfiguration;
import jakarta.inject.Singleton;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.ws.rs.core.Response;
import lombok.extern.slf4j.Slf4j;
import org.keycloak.admin.client.resource.RealmResource;
import org.keycloak.representations.idm.IdentityProviderRepresentation;

/**
 * This class is responsible for creating identity providers. It creates and manages various
 * identity providers for authentication purposes.
 */
@Singleton
@Slf4j
public class IdentityProvidersCreator {

  // static map of ProviderType to Keycloak provider id
  private static final Map<IdentityProviderConfiguration.ProviderType, String> PROVIDER_TYPE_TO_KEYCLOAK_PROVIDER_ID = new HashMap<>();

  static {
    PROVIDER_TYPE_TO_KEYCLOAK_PROVIDER_ID.put(IdentityProviderConfiguration.ProviderType.OKTA, "keycloak-oidc");
  }

  private final List<IdentityProviderConfiguration> identityProviderConfigurations;
  private final ConfigurationMapService configurationMapService;

  public IdentityProvidersCreator(final List<IdentityProviderConfiguration> identityProviderConfigurations,
                                  final ConfigurationMapService configurationMapService) {
    this.identityProviderConfigurations = identityProviderConfigurations;
    this.configurationMapService = configurationMapService;
  }

  public void createIdps(final RealmResource keycloakRealm) {
    // Create Identity Providers
    if (identityProviderConfigurations == null || identityProviderConfigurations.isEmpty()) {
      log.info("No identity providers configured. Skipping IDP setup.");
      return;
    }

    for (final IdentityProviderConfiguration provider : identityProviderConfigurations) {
      try {
        createIdp(keycloakRealm, provider);
      } catch (RuntimeException e) {
        log.error("Failed to create identity provider for provider: {}", provider.getAppName(), e);
        throw e;
      }
    }
    log.info("Identity providers created.");
  }

  private void createIdp(final RealmResource keycloakRealm, final IdentityProviderConfiguration provider) {
    log.info("Creating identity provider: {}", provider);

    final IdentityProviderRepresentation idp = new IdentityProviderRepresentation();
    idp.setAlias(provider.getAppName());
    idp.setProviderId(PROVIDER_TYPE_TO_KEYCLOAK_PROVIDER_ID.get(provider.getType()));
    idp.setEnabled(true);

    Map<String, String> configMap = configurationMapService.importProviderFrom(keycloakRealm, provider, idp.getProviderId());
    Map<String, String> config = configurationMapService.setupProviderConfig(provider, configMap);
    idp.setConfig(config);

    final Response idpResponse = keycloakRealm.identityProviders().create(idp);

    if (idpResponse.getStatus() == Response.Status.CREATED.getStatusCode()) {
      log.info("Identity Provider {} created successfully!", provider.getAppName());
    } else {
      String error = String.format("Failed to create Identity Provider. Status: %s", idpResponse.getStatusInfo().getReasonPhrase());
      log.error(error);
      throw new RuntimeException(error);
    }
  }

  /**
   * Delete existing identity providers and re-create them. This is useful when we want to update the
   * configuration of an existing identity provider.
   */
  public void resetIdentityProviders(RealmResource realmResource) {
    realmResource.identityProviders().findAll().forEach(idpRepresentation -> {
      realmResource.identityProviders().get(idpRepresentation.getInternalId()).remove();
    });

    createIdps(realmResource);
  }

}
