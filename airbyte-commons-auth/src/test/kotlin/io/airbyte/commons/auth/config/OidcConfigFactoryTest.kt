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
  )
  fun testCreateOidcConfigFromEnvConfig() {
    val oidcConfig = beanContext.findBean(OidcConfig::class.java)
    Assertions.assertTrue(oidcConfig.isPresent)
    Assertions.assertEquals("https://testdomain.com", oidcConfig.get().domain)
    Assertions.assertEquals("testApp", oidcConfig.get().appName)
    Assertions.assertEquals("testDisplayName", oidcConfig.get().displayName)
    Assertions.assertEquals("testClientId", oidcConfig.get().clientId)
    Assertions.assertEquals("testClientSecret", oidcConfig.get().clientSecret)
  }
}
