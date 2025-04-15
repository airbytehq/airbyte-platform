/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.auth.config

import io.micronaut.context.annotation.Bean
import io.micronaut.context.annotation.Primary
import io.micronaut.context.annotation.Property
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import io.mockk.every
import io.mockk.mockk
import jakarta.inject.Inject
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test

@MicronautTest(rebuildContext = true)
@Property(name = "airbyte.edition", value = "community")
class AuthConfigsTest {
  @get:Primary
  @get:Bean
  val oidcConfig: OidcConfig =
    mockk {
      every { domain } returns "test-domain"
      every { appName } returns "test-app-name"
      every { clientId } returns "test-client-id"
      every { clientSecret } returns "test-client-secret"
    }

  @get:Primary
  @get:Bean
  val keycloakConfig: AirbyteKeycloakConfiguration =
    mockk {
      every { host } returns "test-host"
      every { realm } returns "test-realm"
    }

  @get:Primary
  @get:Bean
  val initialUserConfig: InitialUserConfig =
    mockk {
      every { email } returns "test-email"
      every { firstName } returns "test-first-name"
      every { lastName } returns "test-last-name"
      every { password } returns "test-password"
    }

  @Inject
  lateinit var authConfigs: AuthConfigs

  @Test
  fun `test default OSS AuthConfigs sets mode to NONE`() {
    Assertions.assertTrue(authConfigs.authMode == AuthMode.NONE)
  }

  @Test
  @Property(name = "airbyte.edition", value = "ENTERPRISE")
  fun `test Enterprise AuthConfigs sets mode to OIDC`() {
    Assertions.assertTrue(authConfigs.authMode == AuthMode.OIDC)
  }

  @Test
  @Property(name = "airbyte.edition", value = "ENTERPRISE")
  @Property(name = "airbyte.auth.identity-provider.type", value = SIMPLE)
  fun `test Enterprise AuthConfigs sets mode to SIMPLE when doing simple auth`() {
    Assertions.assertTrue(authConfigs.authMode == AuthMode.SIMPLE)
  }

  @Test
  @Property(name = "airbyte.auth")
  fun `test AuthConfigs inject subconfigurations`() {
    Assertions.assertTrue(authConfigs.keycloakConfig != null)
    Assertions.assertTrue(authConfigs.oidcConfig != null)
    Assertions.assertTrue(authConfigs.initialUserConfig != null)

    Assertions.assertEquals("test-host", authConfigs.keycloakConfig!!.host)
    Assertions.assertEquals("test-realm", authConfigs.keycloakConfig!!.realm)

    Assertions.assertEquals("test-client-id", authConfigs.oidcConfig!!.clientId)
    Assertions.assertEquals("test-client-secret", authConfigs.oidcConfig!!.clientSecret)
    Assertions.assertEquals("test-domain", authConfigs.oidcConfig!!.domain)

    Assertions.assertEquals("test-email", authConfigs.initialUserConfig!!.email)
    Assertions.assertEquals("test-password", authConfigs.initialUserConfig!!.password)
  }
}

@MicronautTest
class AuthConfigsForCloudTest {
  @Inject
  lateinit var authConfigs: AuthConfigs

  @Test
  @Property(name = "airbyte.edition", value = "cloud")
  fun `test cloud environment sets mode to OIDC`() {
    Assertions.assertTrue(authConfigs.authMode == AuthMode.OIDC)
  }
}

@MicronautTest
@Property(name = "airbyte.edition", value = "community")
@Property(name = "micronaut.security.enabled", value = "true")
class AuthConfigsForCommunityAuthTest {
  @Inject
  lateinit var authConfigs: AuthConfigs

  @Test
  fun `test community-auth environment sets mode to SIMPLE`() {
    Assertions.assertTrue(authConfigs.authMode == AuthMode.SIMPLE)
  }
}
