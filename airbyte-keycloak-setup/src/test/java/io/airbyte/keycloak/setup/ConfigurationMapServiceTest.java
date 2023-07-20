/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.keycloak.setup;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

import io.airbyte.commons.auth.config.AirbyteKeycloakConfiguration;
import io.airbyte.commons.auth.config.IdentityProviderConfiguration;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.keycloak.admin.client.resource.IdentityProvidersResource;
import org.keycloak.admin.client.resource.RealmResource;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class ConfigurationMapServiceTest {

  private static final String WEBAPP_URL = "http://localhost:8000";
  @Mock
  private RealmResource realmResource;
  @Mock
  private IdentityProvidersResource identityProvidersResource;
  @Mock
  private IdentityProviderConfiguration identityProviderConfiguration;
  @Mock
  private AirbyteKeycloakConfiguration keycloakConfiguration;
  @InjectMocks
  private ConfigurationMapService configurationMapService;

  @BeforeEach
  public void setUp() {
    configurationMapService = new ConfigurationMapService(WEBAPP_URL, keycloakConfiguration);
  }

  @Test
  void testImportProviderFrom() {
    when(identityProviderConfiguration.getDomain()).thenReturn("trial-577.okta.com");
    when(realmResource.identityProviders()).thenReturn(identityProvidersResource);

    Map<String, Object> importFromMap = new HashMap<>();
    importFromMap.put("providerId", "keycloak-oidc");
    importFromMap.put("fromUrl", "https://trial-577.okta.com/.well-known/openid-configuration");

    Map<String, String> expected = new HashMap<>();
    expected.put("providerId", "keycloak-oidc");
    expected.put("fromUrl", "https://trial-577.okta.com/.well-known/openid-configuration");
    expected.put("authorizationUrl", "https://trial-577.okta.com/oauth2/v1/authorize");
    expected.put("tokenUrl", "https://trial-577.okta.com/oauth2/v1/token");
    expected.put("userInfoUrl", "https://trial-577.okta.com/oauth2/v1/userinfo");
    expected.put("logoutUrl", "https://trial-577.okta.com/oauth2/v1/logout");
    expected.put("issuer", "https://trial-577.okta.com/oauth2/default");
    expected.put("jwksUrl", "https://trial-577.okta.com/oauth2/default/v1/keys");

    when(identityProvidersResource.importFrom(importFromMap)).thenReturn(expected);

    Map<String, String> actual =
        configurationMapService.importProviderFrom(realmResource, identityProviderConfiguration, "keycloak-oidc");

    assertEquals(expected, actual);
  }

  @Test
  void testSetupProviderConfig() {
    Map<String, String> configMap = Map.of(
        "authorizationUrl", "https://trial-577.okta.com/oauth2/v1/authorize",
        "tokenUrl", "https://trial-577.okta.com/oauth2/v1/token",
        "userInfoUrl", "https://trial-577.okta.com/oauth2/v1/userinfo",
        "logoutUrl", "https://trial-577.okta.com/oauth2/v1/logout",
        "issuer", "https://trial-577.okta.com/oauth2/default",
        "jwksUrl", "https://trial-577.okta.com/oauth2/default/v1/keys");

    when(identityProviderConfiguration.getClientId()).thenReturn("clientId");
    when(identityProviderConfiguration.getClientSecret()).thenReturn("clientSecret");

    Map<String, String> result = configurationMapService.setupProviderConfig(identityProviderConfiguration, configMap);

    assertEquals("clientId", result.get("clientId"));
    assertEquals("clientSecret", result.get("clientSecret"));
    assertEquals("openid email profile", result.get("defaultScope"));
  }

}
