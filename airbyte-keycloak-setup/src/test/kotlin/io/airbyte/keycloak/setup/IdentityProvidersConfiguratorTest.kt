/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.keycloak.setup

import io.airbyte.commons.auth.config.OidcConfig
import jakarta.ws.rs.core.Response
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.junit.jupiter.api.function.Executable
import org.keycloak.admin.client.resource.IdentityProviderResource
import org.keycloak.admin.client.resource.IdentityProvidersResource
import org.keycloak.admin.client.resource.RealmResource
import org.keycloak.representations.idm.IdentityProviderRepresentation
import org.mockito.ArgumentMatcher
import org.mockito.ArgumentMatchers
import org.mockito.Mock
import org.mockito.Mockito
import org.mockito.junit.jupiter.MockitoExtension
import java.util.Optional

@ExtendWith(MockitoExtension::class)
internal class IdentityProvidersConfiguratorTest {
  @Mock
  private lateinit var realmResource: RealmResource

  @Mock
  private lateinit var configurationMapService: ConfigurationMapService

  @Mock
  private lateinit var oidcConfig: OidcConfig

  @Mock
  private lateinit var identityProvidersResource: IdentityProvidersResource

  @Mock
  private lateinit var identityProviderResource: IdentityProviderResource

  @Mock
  private lateinit var identityProviderRepresentation: IdentityProviderRepresentation

  private lateinit var identityProvidersConfigurator: IdentityProvidersConfigurator

  @BeforeEach
  fun setUp() {
    identityProvidersConfigurator = IdentityProvidersConfigurator(configurationMapService, Optional.of(oidcConfig))
  }

  @Nested
  internal inner class ConfigureIdp {
    @Test
    fun testNoOidcConfig() {
      identityProvidersConfigurator = IdentityProvidersConfigurator(configurationMapService, Optional.empty())

      identityProvidersConfigurator.configureIdp(realmResource)

      Mockito.verifyNoInteractions(realmResource)
    }

    @Test
    fun testNoExistingIdp() {
      Mockito
        .`when`(realmResource.identityProviders())
        .thenReturn(identityProvidersResource)
      Mockito
        .`when`(identityProvidersResource.findAll())
        .thenReturn(mutableListOf())

      val response = Mockito.mock(Response::class.java)
      Mockito.`when`(response.status).thenReturn(201)
      Mockito
        .`when`(identityProvidersResource.create(ArgumentMatchers.any(IdentityProviderRepresentation::class.java)))
        .thenReturn(response)

      val importedMap = Mockito.mock(HashMap::class.java) as MutableMap<String, String>
      val configMap = Mockito.mock(HashMap::class.java) as MutableMap<String, String>
      Mockito
        .`when`(configurationMapService.importProviderFrom(realmResource, oidcConfig, "oidc"))
        .thenReturn(importedMap)
      Mockito
        .`when`(configurationMapService.setupProviderConfig(oidcConfig, importedMap))
        .thenReturn(configMap)

      identityProvidersConfigurator.configureIdp(realmResource)

      Mockito
        .verify(identityProvidersResource, Mockito.times(1))
        .create(
          ArgumentMatchers.argThat(
            ArgumentMatcher { idp: IdentityProviderRepresentation ->
              idp.config == configMap
            },
          ),
        )

      Mockito.verify(configMap, Mockito.times(1)).put(
        IdentityProvidersConfigurator.AIRBYTE_MANAGED_IDP_KEY,
        IdentityProvidersConfigurator.AIRBYTE_MANAGED_IDP_VALUE,
      )
    }

    @Test
    fun testOneExistingIdpNotMarked() {
      Mockito
        .`when`(realmResource.identityProviders())
        .thenReturn(identityProvidersResource)
      Mockito
        .`when`(identityProvidersResource.findAll())
        .thenReturn(listOf(identityProviderRepresentation))
      Mockito.`when`(identityProviderRepresentation.internalId).thenReturn("some-internal-id")
      Mockito.`when`(identityProviderRepresentation.alias).thenReturn("some-alias")
      Mockito
        .`when`(identityProvidersResource.get("some-alias"))
        .thenReturn(identityProviderResource)

      val importedMap = Mockito.mock(HashMap::class.java) as MutableMap<String, String>
      val configMap = Mockito.mock(HashMap::class.java) as MutableMap<String, String>
      Mockito
        .`when`(configurationMapService.importProviderFrom(realmResource, oidcConfig, "oidc"))
        .thenReturn(importedMap)
      Mockito
        .`when`(configurationMapService.setupProviderConfig(oidcConfig, importedMap))
        .thenReturn(configMap)

      identityProvidersConfigurator.configureIdp(realmResource)

      Mockito.verify(identityProviderResource, Mockito.times(1)).update(
        ArgumentMatchers.argThat(
          ArgumentMatcher { idp: IdentityProviderRepresentation ->
            idp.config == configMap && "some-internal-id" == idp.internalId
          },
        ),
      )

      Mockito.verify(configMap, Mockito.times(1)).put(
        IdentityProvidersConfigurator.AIRBYTE_MANAGED_IDP_KEY,
        IdentityProvidersConfigurator.AIRBYTE_MANAGED_IDP_VALUE,
      )
    }

    @Test
    fun testMultipleExistingIdpOnlyOneMarked() {
      val unmarkedIdp = Mockito.mock(IdentityProviderRepresentation::class.java)
      Mockito
        .`when`(unmarkedIdp.config)
        .thenReturn(mapOf()) // does not contain marked key
      Mockito.`when`(identityProviderRepresentation.config).thenReturn(
        mapOf(
          IdentityProvidersConfigurator.AIRBYTE_MANAGED_IDP_KEY to
            IdentityProvidersConfigurator.AIRBYTE_MANAGED_IDP_VALUE,
        ),
      )

      Mockito
        .`when`(realmResource.identityProviders())
        .thenReturn(identityProvidersResource)
      Mockito
        .`when`(identityProvidersResource.findAll())
        .thenReturn(listOf(unmarkedIdp, identityProviderRepresentation))
      Mockito.`when`(identityProviderRepresentation.internalId).thenReturn("some-internal-id")
      Mockito.`when`(identityProviderRepresentation.alias).thenReturn("some-alias")
      Mockito
        .`when`(identityProvidersResource.get("some-alias"))
        .thenReturn(identityProviderResource)

      val importedMap = Mockito.mock(HashMap::class.java) as MutableMap<String, String>
      val configMap = Mockito.mock(HashMap::class.java) as MutableMap<String, String>
      Mockito
        .`when`(configurationMapService.importProviderFrom(realmResource, oidcConfig, "oidc"))
        .thenReturn(importedMap)
      Mockito
        .`when`(configurationMapService.setupProviderConfig(oidcConfig, importedMap))
        .thenReturn(configMap)

      identityProvidersConfigurator.configureIdp(realmResource)

      Mockito.verify(identityProviderResource, Mockito.times(1)).update(
        ArgumentMatchers.argThat(
          ArgumentMatcher { idp: IdentityProviderRepresentation ->
            idp.config == configMap && "some-internal-id" == idp.internalId
          },
        ),
      )

      Mockito.verify(unmarkedIdp, Mockito.times(1)).config
      Mockito.verifyNoMoreInteractions(unmarkedIdp)
    }

    @Test
    fun testMultipleExistingIdpsMultipleMarked() {
      val otherMarkedIdp = Mockito.mock(IdentityProviderRepresentation::class.java)
      Mockito.`when`(otherMarkedIdp.config).thenReturn(
        mapOf(
          IdentityProvidersConfigurator.AIRBYTE_MANAGED_IDP_KEY to
            IdentityProvidersConfigurator.AIRBYTE_MANAGED_IDP_VALUE,
        ),
      )
      Mockito.`when`(identityProviderRepresentation.config).thenReturn(
        mapOf(
          IdentityProvidersConfigurator.AIRBYTE_MANAGED_IDP_KEY to
            IdentityProvidersConfigurator.AIRBYTE_MANAGED_IDP_VALUE,
        ),
      )

      Mockito
        .`when`(realmResource.identityProviders())
        .thenReturn(identityProvidersResource)
      Mockito
        .`when`(identityProvidersResource.findAll())
        .thenReturn(listOf(otherMarkedIdp, identityProviderRepresentation))

      identityProvidersConfigurator.configureIdp(realmResource)

      Mockito
        .verify(identityProviderResource, Mockito.never())
        .update(ArgumentMatchers.any(IdentityProviderRepresentation::class.java))

      Mockito
        .verify(identityProviderResource, Mockito.never())
        .update(ArgumentMatchers.any(IdentityProviderRepresentation::class.java))
    }

    @Test
    fun testMultipleExistingIdpsNoneMarked() {
      val otherUnmarkedIdp = Mockito.mock(IdentityProviderRepresentation::class.java)
      Mockito.`when`(otherUnmarkedIdp.config).thenReturn(mapOf())
      Mockito.`when`(identityProviderRepresentation.config).thenReturn(mapOf())

      Mockito
        .`when`(realmResource.identityProviders())
        .thenReturn(identityProvidersResource)
      Mockito
        .`when`(identityProvidersResource.findAll())
        .thenReturn(listOf(otherUnmarkedIdp, identityProviderRepresentation))

      identityProvidersConfigurator.configureIdp(realmResource)

      Mockito
        .verify(identityProviderResource, Mockito.never())
        .update(ArgumentMatchers.any(IdentityProviderRepresentation::class.java))

      Mockito
        .verify(identityProviderResource, Mockito.never())
        .update(ArgumentMatchers.any(IdentityProviderRepresentation::class.java))
    }

    @Test
    fun testCreateFailureThrows() {
      Mockito
        .`when`(realmResource.identityProviders())
        .thenReturn(identityProvidersResource)
      Mockito
        .`when`(identityProvidersResource.findAll())
        .thenReturn(mutableListOf())
      Mockito
        .`when`(identityProvidersResource.create(ArgumentMatchers.any(IdentityProviderRepresentation::class.java)))
        .thenReturn(Response.status(Response.Status.BAD_REQUEST).build())

      val configMap: MutableMap<String, String> = HashMap()
      Mockito
        .`when`(configurationMapService.importProviderFrom(realmResource, oidcConfig, "oidc"))
        .thenReturn(configMap)
      Mockito
        .`when`(configurationMapService.setupProviderConfig(oidcConfig, configMap))
        .thenReturn(configMap)

      Assertions.assertThrows(
        RuntimeException::class.java,
        Executable {
          identityProvidersConfigurator.configureIdp(realmResource)
        },
      )
    }
  }
}
