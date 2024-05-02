/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.auth.config;

import io.micronaut.context.BeanContext;
import io.micronaut.context.annotation.Property;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import java.util.Optional;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

@MicronautTest(rebuildContext = true)
public class OidcConfigFactoryTest {

  @Inject
  BeanContext beanContext;

  @Test
  void testCreateOidcConfigNoAuthPropertiesSet() {
    final Optional<OidcConfig> oidcConfig = beanContext.findBean(OidcConfig.class);
    Assertions.assertTrue(oidcConfig.isEmpty());
  }

  @Test
  // factory should prefer these properties
  @Property(name = "airbyte.auth.identity-provider.type",
            value = "oidc")
  @Property(name = "airbyte.auth.identity-provider.oidc.domain",
            value = "https://testdomain.com")
  @Property(name = "airbyte.auth.identity-provider.oidc.app-name",
            value = "testApp")
  @Property(name = "airbyte.auth.identity-provider.oidc.client-id",
            value = "testClientId")
  @Property(name = "airbyte.auth.identity-provider.oidc.client-secret",
            value = "testClientSecret")
  // below should all be ignored because they're from airbyte.yml
  @Property(name = "airbyte-yml.auth.identity-provider.type",
            value = "oidc")
  @Property(name = "airbyte-yml.auth.identity-provider.oidc.domain",
            value = "https://ignored.com")
  @Property(name = "airbyte-yml.auth.identity-provider.oidc.appName",
            value = "ignoredApp")
  @Property(name = "airbyte-yml.auth.identity-provider.oidc.clientId",
            value = "ignoredClientId")
  @Property(name = "airbyte-yml.auth.identity-provider.oidc.clientSecret",
            value = "ignoredClientSecret")
  @Property(name = "airbyte-yml.auth.identity-providers[0].domain",
            value = "https://ignored.com")
  @Property(name = "airbyte-yml.auth.identity-providers[0].appName",
            value = "ignoredApp")
  @Property(name = "airbyte-yml.auth.identity-providers[0].clientId",
            value = "ignoredClientId")
  @Property(name = "airbyte-yml.auth.identity-providers[0].clientSecret",
            value = "ignoredClientSecret")
  void testCreateOidcConfigFromEnvConfig() {
    final Optional<OidcConfig> oidcConfig = beanContext.findBean(OidcConfig.class);
    Assertions.assertTrue(oidcConfig.isPresent());
    Assertions.assertEquals("https://testdomain.com", oidcConfig.get().getDomain());
    Assertions.assertEquals("testApp", oidcConfig.get().getAppName());
    Assertions.assertEquals("testClientId", oidcConfig.get().getClientId());
    Assertions.assertEquals("testClientSecret", oidcConfig.get().getClientSecret());
  }

  @Test
  // factory should prefer these properties
  @Property(name = "airbyte-yml.auth.identity-provider.type",
            value = "oidc")
  @Property(name = "airbyte-yml.auth.identity-provider.oidc.domain",
            value = "https://testdomain.com")
  @Property(name = "airbyte-yml.auth.identity-provider.oidc.appName",
            value = "testApp")
  @Property(name = "airbyte-yml.auth.identity-provider.oidc.clientId",
            value = "testClientId")
  @Property(name = "airbyte-yml.auth.identity-provider.oidc.clientSecret",
            value = "testClientSecret")
  // below should all be ignored because they're list-style properties
  // which are ignored if the single-style properties are set in airbyte.yml
  @Property(name = "airbyte-yml.auth.identity-providers[0].domain",
            value = "https://ignoreddomain.com")
  @Property(name = "airbyte-yml.auth.identity-providers[0].appName",
            value = "ignoredApp")
  @Property(name = "airbyte-yml.auth.identity-providers[0].clientId",
            value = "ignoredClientId")
  @Property(name = "airbyte-yml.auth.identity-providers[0].clientSecret",
            value = "ignoredClientSecret")
  void testCreateOidcConfigFromSingleAirbyteYmlIdp() {
    final Optional<OidcConfig> oidcConfig = beanContext.findBean(OidcConfig.class);
    Assertions.assertTrue(oidcConfig.isPresent());
    Assertions.assertEquals("https://testdomain.com", oidcConfig.get().getDomain());
    Assertions.assertEquals("testApp", oidcConfig.get().getAppName());
    Assertions.assertEquals("testClientId", oidcConfig.get().getClientId());
    Assertions.assertEquals("testClientSecret", oidcConfig.get().getClientSecret());
  }

  @Test
  @Property(name = "airbyte-yml.auth.identity-providers[0].domain",
            value = "https://testdomain.com")
  @Property(name = "airbyte-yml.auth.identity-providers[0].appName",
            value = "testApp")
  @Property(name = "airbyte-yml.auth.identity-providers[0].clientId",
            value = "testClientId")
  @Property(name = "airbyte-yml.auth.identity-providers[0].clientSecret",
            value = "testClientSecret")
  void testCreateOidcConfigFromIdentityProviderConfigurations() {
    final Optional<OidcConfig> oidcConfig = beanContext.findBean(OidcConfig.class);
    Assertions.assertTrue(oidcConfig.isPresent());
    Assertions.assertEquals("https://testdomain.com", oidcConfig.get().getDomain());
    Assertions.assertEquals("testApp", oidcConfig.get().getAppName());
    Assertions.assertEquals("testClientId", oidcConfig.get().getClientId());
    Assertions.assertEquals("testClientSecret", oidcConfig.get().getClientSecret());
  }

  @Test
  @Property(name = "airbyte-yml.auth.identity-providers[0].domain",
            value = "https://testdomain.com")
  @Property(name = "airbyte-yml.auth.identity-providers[0].appName",
            value = "testApp")
  @Property(name = "airbyte-yml.auth.identity-providers[0].clientId",
            value = "testClientId")
  @Property(name = "airbyte-yml.auth.identity-providers[0].clientSecret",
            value = "testClientSecret")
  @Property(name = "airbyte-yml.auth.identity-providers[1].type",
            value = "oidc")
  @Property(name = "airbyte-yml.auth.identity-providers[1].domain",
            value = "https://testdomain2.com")
  @Property(name = "airbyte-yml.auth.identity-providers[1].appName",
            value = "testApp2")
  @Property(name = "airbyte-yml.auth.identity-providers[1].clientId",
            value = "testClientId2")
  @Property(name = "airbyte-yml.auth.identity-providers[1].clientSecret",
            value = "testClientSecret2")
  void testCreateOidcConfigFromIdentityProviderConfigurationsThrowsIfMultiple() {
    Assertions.assertThrows(RuntimeException.class, () -> beanContext.findBean(OidcConfig.class));
  }

}
