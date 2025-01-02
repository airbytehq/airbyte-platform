/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */
package io.airbyte.commons.auth.config

import io.micronaut.context.annotation.Property
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import jakarta.inject.Inject
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test

@MicronautTest
class IdentityProviderConfigurationTest {
  @Inject
  lateinit var identityProviderConfiguration: IdentityProviderConfiguration

  @Test
  @Property(name = "airbyte-yml.auth.identity-providers[0].type", value = "oidc")
  @Property(name = "airbyte-yml.auth.identity-providers[0].domain", value = "testdomain")
  @Property(name = "airbyte-yml.auth.identity-providers[0].appName", value = "testApp")
  @Property(name = "airbyte-yml.auth.identity-providers[0].displayName", value = "testDisplayName")
  @Property(name = "airbyte-yml.auth.identity-providers[0].clientId", value = "testClientId")
  @Property(name = "airbyte-yml.auth.identity-providers[0].clientSecret", value = "testClientSecret")
  fun testToAuthOidcConfiguration() {
    val result = identityProviderConfiguration.toOidcConfig()
    Assertions.assertEquals("testdomain", result.domain)
    Assertions.assertEquals("testApp", result.appName)
    Assertions.assertEquals("testDisplayName", result.displayName)
    Assertions.assertEquals("testClientId", result.clientId)
    Assertions.assertEquals("testClientSecret", result.clientSecret)
  }
}
