/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.auth.config

import io.airbyte.micronaut.runtime.AirbyteKeycloakConfig
import io.airbyte.micronaut.runtime.DEFAULT_AUTH_IDENTITY_PROVIDER_TYPE
import io.micronaut.context.annotation.Bean
import io.micronaut.context.annotation.Primary
import io.micronaut.context.annotation.Property
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import io.mockk.every
import io.mockk.mockk
import jakarta.inject.Inject
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Test

@MicronautTest(rebuildContext = true)
@Property(name = "airbyte.edition", value = "community")
class AuthConfigsTest {
  @get:Primary
  @get:Bean
  val keycloakConfig: AirbyteKeycloakConfig =
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
  @Property(name = "micronaut.security.enabled", value = "false")
  fun `test default OSS AuthConfigs sets mode to NONE`() {
    assertEquals(AuthMode.NONE, authConfigs.authMode)
  }

  @Test
  @Property(name = "micronaut.security.enabled", value = "true")
  fun `test OSS AuthConfigs when Micronaut Security enabled sets mode to SIMPLE`() {
    assertEquals(AuthMode.SIMPLE, authConfigs.authMode)
  }

  @Test
  fun `test AuthConfigs inject subconfigurations`() {
    assertNotNull(authConfigs.keycloakConfig)
    assertNotNull(authConfigs.initialUserConfig)
    assertEquals("test-host", authConfigs.keycloakConfig!!.host)
    assertEquals("test-realm", authConfigs.keycloakConfig!!.realm)
    assertEquals("test-email", authConfigs.initialUserConfig!!.email)
    assertEquals("test-password", authConfigs.initialUserConfig!!.password)
  }
}

@MicronautTest
@Property(name = "airbyte.edition", value = "cloud")
class AuthConfigsForCloudTest {
  @Inject
  lateinit var authConfigs: AuthConfigs

  @Test
  fun `test cloud environment sets mode to OIDC`() {
    assertEquals(AuthMode.OIDC, authConfigs.authMode)
  }
}

@MicronautTest(rebuildContext = true)
@Property(name = "airbyte.edition", value = "enterprise")
class AuthConfigsForEnterpriseTest {
  @get:Primary
  @get:Bean
  val oidcConfig: OidcConfig =
    mockk {
      every { domain } returns "test-domain"
      every { appName } returns "test-app-name"
      every { clientId } returns "test-client-id"
      every { clientSecret } returns "test-client-secret"
    }

  @Inject
  lateinit var authConfigs: AuthConfigs

  @Test
  @Property(name = "airbyte.auth.identity-provider.type", value = "oidc")
  fun `test Enterprise AuthConfigs sets mode to OIDC`() {
    assertEquals(AuthMode.OIDC, authConfigs.authMode)
    assertNotNull(authConfigs.oidcConfig)
    assertEquals("test-client-id", authConfigs.oidcConfig!!.clientId)
    assertEquals("test-client-secret", authConfigs.oidcConfig!!.clientSecret)
    assertEquals("test-domain", authConfigs.oidcConfig!!.domain)
    assertEquals("test-app-name", authConfigs.oidcConfig!!.appName)
  }

  @Test
  @Property(name = "airbyte.auth.identity-provider.type", value = DEFAULT_AUTH_IDENTITY_PROVIDER_TYPE)
  fun `test Enterprise AuthConfigs sets mode to SIMPLE when doing simple auth`() {
    assertEquals(AuthMode.SIMPLE, authConfigs.authMode)
  }
}
