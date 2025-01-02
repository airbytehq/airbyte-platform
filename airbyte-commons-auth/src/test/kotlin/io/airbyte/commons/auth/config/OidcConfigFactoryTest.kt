/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */
package io.airbyte.commons.auth.config

import io.micronaut.context.BeanContext
import io.micronaut.context.annotation.Property
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import jakarta.inject.Inject
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test

@MicronautTest(rebuildContext = true)
class OidcConfigFactoryTest {
  @Inject
  lateinit var beanContext: BeanContext

  @Test
  fun testCreateOidcConfigNoAuthPropertiesSet() {
    val oidcConfig = beanContext.findBean(OidcConfig::class.java)
    Assertions.assertTrue(oidcConfig.isEmpty)
  }

  @Test // factory should prefer these properties
  @Property(name = "airbyte.auth.identity-provider.type", value = "oidc")
  @Property(name = "airbyte.auth.identity-provider.oidc.domain", value = "https://testdomain.com")
  @Property(name = "airbyte.auth.identity-provider.oidc.app-name", value = "testApp")
  @Property(name = "airbyte.auth.identity-provider.oidc.display-name", value = "testDisplayName")
  @Property(name = "airbyte.auth.identity-provider.oidc.client-id", value = "testClientId")
  @Property(
    name = "airbyte.auth.identity-provider.oidc.client-secret",
    value = "testClientSecret",
  ) // below should all be ignored because they're from airbyte.yml
  @Property(name = "airbyte-yml.auth.identity-provider.type", value = "oidc")
  @Property(name = "airbyte-yml.auth.identity-provider.oidc.domain", value = "https://ignored.com")
  @Property(name = "airbyte-yml.auth.identity-provider.oidc.appName", value = "ignoredApp")
  @Property(name = "airbyte-yml.auth.identity-provider.oidc.displayName", value = "ignoredDisplayName")
  @Property(name = "airbyte-yml.auth.identity-provider.oidc.clientId", value = "ignoredClientId")
  @Property(name = "airbyte-yml.auth.identity-provider.oidc.clientSecret", value = "ignoredClientSecret")
  @Property(name = "airbyte-yml.auth.identity-providers[0].domain", value = "https://ignored.com")
  @Property(name = "airbyte-yml.auth.identity-providers[0].appName", value = "ignoredApp")
  @Property(name = "airbyte-yml.auth.identity-providers.[0].displayName", value = "ignoredDisplayName")
  @Property(name = "airbyte-yml.auth.identity-providers[0].clientId", value = "ignoredClientId")
  @Property(name = "airbyte-yml.auth.identity-providers[0].clientSecret", value = "ignoredClientSecret")
  fun testCreateOidcConfigFromEnvConfig() {
    val oidcConfig = beanContext.findBean(OidcConfig::class.java)
    Assertions.assertTrue(oidcConfig.isPresent)
    Assertions.assertEquals("https://testdomain.com", oidcConfig.get().domain)
    Assertions.assertEquals("testApp", oidcConfig.get().appName)
    Assertions.assertEquals("testDisplayName", oidcConfig.get().displayName)
    Assertions.assertEquals("testClientId", oidcConfig.get().clientId)
    Assertions.assertEquals("testClientSecret", oidcConfig.get().clientSecret)
  }

  @Test // factory should prefer these properties
  @Property(name = "airbyte-yml.auth.identity-provider.type", value = "oidc")
  @Property(name = "airbyte-yml.auth.identity-provider.oidc.domain", value = "https://testdomain.com")
  @Property(name = "airbyte-yml.auth.identity-provider.oidc.appName", value = "testApp")
  @Property(name = "airbyte-yml.auth.identity-provider.oidc.displayName", value = "testDisplayName")
  @Property(name = "airbyte-yml.auth.identity-provider.oidc.clientId", value = "testClientId")
  @Property(
    name = "airbyte-yml.auth.identity-provider.oidc.clientSecret",
    value = "testClientSecret",
  ) // below should all be ignored because they're list-style properties
  // which are ignored if the single-style properties are set in airbyte.yml
  @Property(name = "airbyte-yml.auth.identity-providers[0].domain", value = "https://ignoreddomain.com")
  @Property(name = "airbyte-yml.auth.identity-providers[0].appName", value = "ignoredApp")
  @Property(name = "airbyte-yml.auth.identity-providers[0].displayName", value = "ignoredDisplayName")
  @Property(name = "airbyte-yml.auth.identity-providers[0].clientId", value = "ignoredClientId")
  @Property(name = "airbyte-yml.auth.identity-providers[0].clientSecret", value = "ignoredClientSecret")
  fun testCreateOidcConfigFromSingleAirbyteYmlIdp() {
    val oidcConfig = beanContext.findBean(OidcConfig::class.java)
    Assertions.assertTrue(oidcConfig.isPresent)
    Assertions.assertEquals("https://testdomain.com", oidcConfig.get().domain)
    Assertions.assertEquals("testApp", oidcConfig.get().appName)
    Assertions.assertEquals("testDisplayName", oidcConfig.get().displayName)
    Assertions.assertEquals("testClientId", oidcConfig.get().clientId)
    Assertions.assertEquals("testClientSecret", oidcConfig.get().clientSecret)
  }

  @Test
  @Property(name = "airbyte-yml.auth.identity-provider.type", value = "oidc")
  @Property(name = "airbyte-yml.auth.identity-provider.oidc.domain", value = "https://testdomain.com")
  @Property(name = "airbyte-yml.auth.identity-provider.oidc.appName", value = "testApp")
  // airbyte-yml.auth.identity-provider.oidc.displayName is intentionally omitted
  @Property(name = "airbyte-yml.auth.identity-provider.oidc.clientId", value = "testClientId")
  @Property(name = "airbyte-yml.auth.identity-provider.oidc.clientSecret", value = "testClientSecret")
  fun testCreateOidcConfigFromSingleAirbyteYmlIdpDisplayNameFallback() {
    val oidcConfig = beanContext.findBean(OidcConfig::class.java)
    Assertions.assertTrue(oidcConfig.isPresent)
    Assertions.assertEquals("https://testdomain.com", oidcConfig.get().domain)
    Assertions.assertEquals("testApp", oidcConfig.get().appName)
    Assertions.assertEquals("testApp", oidcConfig.get().displayName)
    Assertions.assertEquals("testClientId", oidcConfig.get().clientId)
    Assertions.assertEquals("testClientSecret", oidcConfig.get().clientSecret)
  }

  @Test
  @Property(name = "airbyte-yml.auth.identity-providers[0].domain", value = "https://testdomain.com")
  @Property(name = "airbyte-yml.auth.identity-providers[0].appName", value = "testApp")
  @Property(name = "airbyte-yml.auth.identity-providers[0].displayName", value = "testDisplayName")
  @Property(name = "airbyte-yml.auth.identity-providers[0].clientId", value = "testClientId")
  @Property(name = "airbyte-yml.auth.identity-providers[0].clientSecret", value = "testClientSecret")
  fun testCreateOidcConfigFromIdentityProviderConfigurations() {
    val oidcConfig = beanContext.findBean(OidcConfig::class.java)
    Assertions.assertTrue(oidcConfig.isPresent)
    Assertions.assertEquals("https://testdomain.com", oidcConfig.get().domain)
    Assertions.assertEquals("testApp", oidcConfig.get().appName)
    Assertions.assertEquals("testDisplayName", oidcConfig.get().displayName)
    Assertions.assertEquals("testClientId", oidcConfig.get().clientId)
    Assertions.assertEquals("testClientSecret", oidcConfig.get().clientSecret)
  }

  @Test
  @Property(name = "airbyte-yml.auth.identity-providers[0].domain", value = "https://testdomain.com")
  @Property(name = "airbyte-yml.auth.identity-providers[0].appName", value = "testApp")
  // airbyte-yml.auth.identity-providers[0].displayName is intentionally omitted
  @Property(name = "airbyte-yml.auth.identity-providers[0].clientId", value = "testClientId")
  @Property(name = "airbyte-yml.auth.identity-providers[0].clientSecret", value = "testClientSecret")
  fun testCreateOidcConfigFromIdentityProviderConfigurationsDisplayNameFallback() {
    val oidcConfig = beanContext.findBean(OidcConfig::class.java)
    Assertions.assertTrue(oidcConfig.isPresent)
    Assertions.assertEquals("https://testdomain.com", oidcConfig.get().domain)
    Assertions.assertEquals("testApp", oidcConfig.get().appName)
    Assertions.assertEquals("testApp", oidcConfig.get().displayName)
    Assertions.assertEquals("testClientId", oidcConfig.get().clientId)
    Assertions.assertEquals("testClientSecret", oidcConfig.get().clientSecret)
  }

  @Test
  @Property(name = "airbyte-yml.auth.identity-providers[0].domain", value = "https://testdomain.com")
  @Property(name = "airbyte-yml.auth.identity-providers[0].appName", value = "testApp")
  @Property(name = "airbyte-yml.auth.identity-providers[0].clientId", value = "testClientId")
  @Property(name = "airbyte-yml.auth.identity-providers[0].clientSecret", value = "testClientSecret")
  @Property(name = "airbyte-yml.auth.identity-providers[1].type", value = "oidc")
  @Property(name = "airbyte-yml.auth.identity-providers[1].domain", value = "https://testdomain2.com")
  @Property(name = "airbyte-yml.auth.identity-providers[1].appName", value = "testApp2")
  @Property(name = "airbyte-yml.auth.identity-providers[1].clientId", value = "testClientId2")
  @Property(name = "airbyte-yml.auth.identity-providers[1].clientSecret", value = "testClientSecret2")
  fun testCreateOidcConfigFromIdentityProviderConfigurationsThrowsIfMultiple() {
    Assertions.assertThrows(RuntimeException::class.java) {
      beanContext.findBean(
        OidcConfig::class.java,
      )
    }
  }
}
