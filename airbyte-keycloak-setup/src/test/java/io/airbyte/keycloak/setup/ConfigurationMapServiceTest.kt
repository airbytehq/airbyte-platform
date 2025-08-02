/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.keycloak.setup

import io.airbyte.commons.auth.config.AirbyteKeycloakConfiguration
import io.airbyte.commons.auth.config.OidcConfig
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import org.keycloak.admin.client.resource.IdentityProvidersResource
import org.keycloak.admin.client.resource.RealmResource
import org.mockito.Mock
import org.mockito.Mockito
import org.mockito.junit.jupiter.MockitoExtension

@ExtendWith(MockitoExtension::class)
internal class ConfigurationMapServiceTest {
  @Mock
  private lateinit var realmResource: RealmResource

  @Mock
  private lateinit var identityProvidersResource: IdentityProvidersResource

  @Mock
  private lateinit var oidcConfig: OidcConfig

  @Mock
  private lateinit var keycloakConfiguration: AirbyteKeycloakConfiguration

  private lateinit var configurationMapService: ConfigurationMapService

  @BeforeEach
  fun setUp() {
    configurationMapService = ConfigurationMapService(WEBAPP_URL, keycloakConfiguration)
  }

  @ParameterizedTest
  @ValueSource(
    strings = [
      "trial-577.okta.com",
      "https://trial-577.okta.com",
      "trial-577.okta.com/.well-known/openid-configuration",
      "https://trial-577.okta.com/.well-known/openid-configuration",
      "trial-577.okta.com/",
      "https://trial-577.okta.com/",
    ],
  )
  fun testImportProviderFrom(url: String?) {
    Mockito.`when`(oidcConfig.domain).thenReturn(url)
    Mockito
      .`when`(realmResource.identityProviders())
      .thenReturn(identityProvidersResource)

    val importFromMap =
      mutableMapOf<String, Any?>(
        "providerId" to "oidc",
        "fromUrl" to "https://trial-577.okta.com/.well-known/openid-configuration",
      )

    val expected =
      mutableMapOf(
        "providerId" to "oidc",
        "fromUrl" to "https://trial-577.okta.com/.well-known/openid-configuration",
        "authorizationUrl" to "https://trial-577.okta.com/oauth2/v1/authorize",
        "tokenUrl" to "https://trial-577.okta.com/oauth2/v1/token",
        "userInfoUrl" to "https://trial-577.okta.com/oauth2/v1/userinfo",
        "logoutUrl" to "https://trial-577.okta.com/oauth2/v1/logout",
        "issuer" to "https://trial-577.okta.com/oauth2/default",
        "jwksUrl" to "https://trial-577.okta.com/oauth2/default/v1/keys",
      )

    Mockito.`when`(identityProvidersResource.importFrom(importFromMap)).thenReturn(expected)

    val actual = configurationMapService.importProviderFrom(realmResource, oidcConfig, "oidc")

    Assertions.assertEquals(expected, actual)
  }

  @Test
  fun testSetupProviderConfig() {
    val configMap =
      mapOf(
        "authorizationUrl" to "https://trial-577.okta.com/oauth2/v1/authorize",
        "tokenUrl" to "https://trial-577.okta.com/oauth2/v1/token",
        "userInfoUrl" to "https://trial-577.okta.com/oauth2/v1/userinfo",
        "logoutUrl" to "https://trial-577.okta.com/oauth2/v1/logout",
        "issuer" to "https://trial-577.okta.com/oauth2/default",
        "jwksUrl" to "https://trial-577.okta.com/oauth2/default/v1/keys",
      )

    Mockito.`when`(oidcConfig.clientId).thenReturn("clientId")
    Mockito.`when`(oidcConfig.clientSecret).thenReturn("clientSecret")

    val result = configurationMapService.setupProviderConfig(oidcConfig, configMap)

    Assertions.assertEquals("clientId", result["clientId"])
    Assertions.assertEquals("clientSecret", result["clientSecret"])
    Assertions.assertEquals("openid email profile", result["defaultScope"])
  }

  companion object {
    private const val WEBAPP_URL = "http://localhost:8000"
  }
}
