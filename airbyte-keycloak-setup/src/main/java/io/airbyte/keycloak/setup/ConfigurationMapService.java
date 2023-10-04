/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.keycloak.setup;

import io.airbyte.commons.auth.config.AirbyteKeycloakConfiguration;
import io.airbyte.commons.auth.config.IdentityProviderConfiguration;
import io.micronaut.context.annotation.Value;
import jakarta.inject.Singleton;
import java.util.HashMap;
import java.util.Map;
import org.keycloak.admin.client.resource.RealmResource;

/**
 * This class provides services for managing configuration maps. It includes methods for adding,
 * removing, and updating configuration settings.
 */
@Singleton
public class ConfigurationMapService {

  private final String webappUrl;
  private final AirbyteKeycloakConfiguration keycloakConfiguration;

  public ConfigurationMapService(@Value("${airbyte.webapp-url}") final String webappUrl,
                                 final AirbyteKeycloakConfiguration keycloakConfiguration) {
    this.webappUrl = webappUrl;
    this.keycloakConfiguration = keycloakConfiguration;
  }

  public Map<String, String> importProviderFrom(final RealmResource keycloakRealm,
                                                final IdentityProviderConfiguration provider,
                                                String keycloakProviderId) {
    Map<String, Object> map = new HashMap<>();
    map.put("providerId", keycloakProviderId);
    map.put("fromUrl", getProviderDiscoveryUrl(provider));
    return keycloakRealm.identityProviders().importFrom(map);
  }

  public Map<String, String> setupProviderConfig(final IdentityProviderConfiguration provider, Map<String, String> configMap) {
    Map<String, String> config = new HashMap<>();
    config.put("clientId", provider.getClientId());
    config.put("clientSecret", provider.getClientSecret());
    config.put("authorizationUrl", configMap.get("authorizationUrl"));
    config.put("tokenUrl", configMap.get("tokenUrl"));
    config.put("userInfoUrl", configMap.get("userInfoUrl"));
    config.put("logoutUrl", configMap.get("logoutUrl"));
    config.put("issuer", configMap.get("issuer"));
    config.put("defaultScope", "openid email profile");
    config.put("redirectUris", getProviderRedirectUrl(provider));
    config.put("jwksUrl", configMap.get("jwksUrl"));
    return config;
  }

  private String getProviderRedirectUrl(final IdentityProviderConfiguration provider) {
    final String webappUrlWithTrailingSlash = webappUrl.endsWith("/") ? webappUrl : webappUrl + "/";
    return webappUrlWithTrailingSlash + "auth/realms/" + keycloakConfiguration.getAirbyteRealm() + "/broker/" + provider.getAppName() + "/endpoint";
  }

  private String getProviderDiscoveryUrl(final IdentityProviderConfiguration provider) {
    final String domainWithTrailingSlash = provider.getDomain().endsWith("/") ? provider.getDomain() : provider.getDomain() + "/";
    return "https://" + domainWithTrailingSlash + ".well-known/openid-configuration";
  }

}
