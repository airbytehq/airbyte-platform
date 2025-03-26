/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.keycloak.setup;

import io.airbyte.commons.auth.config.AirbyteKeycloakConfiguration;
import io.airbyte.commons.auth.config.OidcConfig;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import java.util.HashMap;
import java.util.Map;
import org.keycloak.admin.client.resource.RealmResource;

/**
 * This class provides services for managing configuration maps. It includes methods for adding,
 * removing, and updating configuration settings.
 */
@Singleton
@SuppressWarnings("PMD.UseStringBufferForStringAppends")
public class ConfigurationMapService {

  public static final String HTTPS_PREFIX = "https://";
  public static final String WELL_KNOWN_OPENID_CONFIGURATION_SUFFIX = ".well-known/openid-configuration";
  private final String airbyteUrl;
  private final AirbyteKeycloakConfiguration keycloakConfiguration;

  public ConfigurationMapService(@Named("airbyteUrl") final String airbyteUrl,
                                 final AirbyteKeycloakConfiguration keycloakConfiguration) {
    this.airbyteUrl = airbyteUrl;
    this.keycloakConfiguration = keycloakConfiguration;
  }

  public Map<String, String> importProviderFrom(final RealmResource keycloakRealm,
                                                final OidcConfig oidcConfig,
                                                String keycloakProviderId) {
    Map<String, Object> map = new HashMap<>();
    map.put("providerId", keycloakProviderId);
    map.put("fromUrl", getProviderDiscoveryUrl(oidcConfig));
    return keycloakRealm.identityProviders().importFrom(map);
  }

  public Map<String, String> setupProviderConfig(final OidcConfig oidcConfig, Map<String, String> configMap) {
    // Copy all keys from configMap to the result map
    final Map<String, String> config = new HashMap<>(configMap);

    // Explicitly set required keys
    config.put("clientId", oidcConfig.getClientId());
    config.put("clientSecret", oidcConfig.getClientSecret());
    config.put("defaultScope", "openid email profile");
    config.put("redirectUris", getProviderRedirectUrl(oidcConfig));
    config.put("backchannelSupported", "true");
    config.put("backchannel_logout_session_supported", "true");

    return config;
  }

  private String getProviderRedirectUrl(final OidcConfig oidcConfig) {
    final String airbyteUrlWithTrailingSlash = airbyteUrl.endsWith("/") ? airbyteUrl : airbyteUrl + "/";
    return airbyteUrlWithTrailingSlash + "auth/realms/" + keycloakConfiguration.getAirbyteRealm() + "/broker/" + oidcConfig.getAppName()
        + "/endpoint";
  }

  private String getProviderDiscoveryUrl(final OidcConfig oidcConfig) {
    String domain = oidcConfig.getDomain();
    if (!domain.startsWith(HTTPS_PREFIX)) {
      domain = HTTPS_PREFIX + domain;
    }
    if (!domain.endsWith(WELL_KNOWN_OPENID_CONFIGURATION_SUFFIX)) {
      domain = domain.endsWith("/") ? domain : domain + "/";
      domain = domain + WELL_KNOWN_OPENID_CONFIGURATION_SUFFIX;
    }
    return domain;
  }

}
