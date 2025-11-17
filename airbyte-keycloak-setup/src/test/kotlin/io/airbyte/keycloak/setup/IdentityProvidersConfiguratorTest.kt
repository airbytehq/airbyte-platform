/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.keycloak.setup

import io.airbyte.commons.auth.config.OidcConfig
import io.mockk.every
import io.mockk.impl.annotations.MockK
import io.mockk.junit5.MockKExtension
import io.mockk.mockk
import io.mockk.slot
import io.mockk.verify
import jakarta.ws.rs.core.Response
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.keycloak.admin.client.resource.IdentityProviderResource
import org.keycloak.admin.client.resource.IdentityProvidersResource
import org.keycloak.admin.client.resource.RealmResource
import org.keycloak.representations.idm.IdentityProviderRepresentation
import java.util.Optional

@ExtendWith(MockKExtension::class)
internal class IdentityProvidersConfiguratorTest {
  @MockK
  private lateinit var realmResource: RealmResource

  @MockK
  private lateinit var configurationMapService: ConfigurationMapService

  @MockK
  private lateinit var oidcConfig: OidcConfig

  @MockK
  private lateinit var identityProvidersResource: IdentityProvidersResource

  @MockK
  private lateinit var identityProviderResource: IdentityProviderResource

  @MockK
  private lateinit var identityProviderRepresentation: IdentityProviderRepresentation

  private lateinit var identityProvidersConfigurator: IdentityProvidersConfigurator

  @BeforeEach
  fun setUp() {
    every { oidcConfig.displayName } returns "Test IDP"
    every { oidcConfig.appName } returns "test-app"
    identityProvidersConfigurator = IdentityProvidersConfigurator(configurationMapService, Optional.of(oidcConfig))
  }

  @Nested
  internal inner class ConfigureIdp {
    @Test
    fun testNoOidcConfig() {
      identityProvidersConfigurator = IdentityProvidersConfigurator(configurationMapService, Optional.empty())

      identityProvidersConfigurator.configureIdp(realmResource)

      verify(exactly = 0) { realmResource.identityProviders() }
    }

    @Test
    fun testNoExistingIdp() {
      every { realmResource.identityProviders() } returns identityProvidersResource
      every { identityProvidersResource.findAll() } returns mutableListOf()

      val response = mockk<Response>()
      every { response.status } returns 201
      every { response.close() } returns Unit
      every { identityProvidersResource.create(any()) } returns response

      val importedMap = mutableMapOf<String, String>()
      val configMap = mutableMapOf<String, String>()
      every { configurationMapService.importProviderFrom(realmResource, oidcConfig, "oidc") } returns importedMap
      every { configurationMapService.setupProviderConfig(oidcConfig, importedMap) } returns configMap

      identityProvidersConfigurator.configureIdp(realmResource)

      val idpSlot = slot<IdentityProviderRepresentation>()
      verify(exactly = 1) { identityProvidersResource.create(capture(idpSlot)) }
      Assertions.assertEquals(configMap, idpSlot.captured.config)

      Assertions.assertEquals(
        IdentityProvidersConfigurator.AIRBYTE_MANAGED_IDP_VALUE,
        configMap[IdentityProvidersConfigurator.AIRBYTE_MANAGED_IDP_KEY],
      )
    }

    @Test
    fun testOneExistingIdpNotMarked() {
      every { realmResource.identityProviders() } returns identityProvidersResource
      every { identityProvidersResource.findAll() } returns listOf(identityProviderRepresentation)
      every { identityProviderRepresentation.config } returns mapOf()
      every { identityProviderRepresentation.internalId } returns "some-internal-id"
      every { identityProviderRepresentation.alias } returns "some-alias"
      every { identityProvidersResource.get("some-alias") } returns identityProviderResource
      every { identityProviderResource.update(any()) } returns Unit

      val importedMap = mutableMapOf<String, String>()
      val configMap = mutableMapOf<String, String>()
      every { configurationMapService.importProviderFrom(realmResource, oidcConfig, "oidc") } returns importedMap
      every { configurationMapService.setupProviderConfig(oidcConfig, importedMap) } returns configMap

      identityProvidersConfigurator.configureIdp(realmResource)

      val idpSlot = slot<IdentityProviderRepresentation>()
      verify(exactly = 1) { identityProviderResource.update(capture(idpSlot)) }
      Assertions.assertEquals(configMap, idpSlot.captured.config)
      Assertions.assertEquals("some-internal-id", idpSlot.captured.internalId)

      Assertions.assertEquals(
        IdentityProvidersConfigurator.AIRBYTE_MANAGED_IDP_VALUE,
        configMap[IdentityProvidersConfigurator.AIRBYTE_MANAGED_IDP_KEY],
      )
    }

    @Test
    fun testMultipleExistingIdpOnlyOneMarked() {
      val unmarkedIdp = mockk<IdentityProviderRepresentation>()
      every { unmarkedIdp.config } returns mapOf()
      every { identityProviderRepresentation.config } returns
        mapOf(
          IdentityProvidersConfigurator.AIRBYTE_MANAGED_IDP_KEY to
            IdentityProvidersConfigurator.AIRBYTE_MANAGED_IDP_VALUE,
        )

      every { realmResource.identityProviders() } returns identityProvidersResource
      every { identityProvidersResource.findAll() } returns listOf(unmarkedIdp, identityProviderRepresentation)
      every { identityProviderRepresentation.internalId } returns "some-internal-id"
      every { identityProviderRepresentation.alias } returns "some-alias"
      every { identityProvidersResource.get("some-alias") } returns identityProviderResource
      every { identityProviderResource.update(any()) } returns Unit

      val importedMap = mutableMapOf<String, String>()
      val configMap = mutableMapOf<String, String>()
      every { configurationMapService.importProviderFrom(realmResource, oidcConfig, "oidc") } returns importedMap
      every { configurationMapService.setupProviderConfig(oidcConfig, importedMap) } returns configMap

      identityProvidersConfigurator.configureIdp(realmResource)

      val idpSlot = slot<IdentityProviderRepresentation>()
      verify(exactly = 1) { identityProviderResource.update(capture(idpSlot)) }
      Assertions.assertEquals(configMap, idpSlot.captured.config)
      Assertions.assertEquals("some-internal-id", idpSlot.captured.internalId)

      verify(exactly = 1) { unmarkedIdp.config }
    }

    @Test
    fun testMultipleExistingIdpsMultipleMarked() {
      val otherMarkedIdp = mockk<IdentityProviderRepresentation>()
      every { otherMarkedIdp.config } returns
        mapOf(
          IdentityProvidersConfigurator.AIRBYTE_MANAGED_IDP_KEY to
            IdentityProvidersConfigurator.AIRBYTE_MANAGED_IDP_VALUE,
        )
      every { identityProviderRepresentation.config } returns
        mapOf(
          IdentityProvidersConfigurator.AIRBYTE_MANAGED_IDP_KEY to
            IdentityProvidersConfigurator.AIRBYTE_MANAGED_IDP_VALUE,
        )

      every { realmResource.identityProviders() } returns identityProvidersResource
      every { identityProvidersResource.findAll() } returns listOf(otherMarkedIdp, identityProviderRepresentation)

      val importedMap = mutableMapOf<String, String>()
      val configMap = mutableMapOf<String, String>()
      every { configurationMapService.importProviderFrom(realmResource, oidcConfig, "oidc") } returns importedMap
      every { configurationMapService.setupProviderConfig(oidcConfig, importedMap) } returns configMap

      identityProvidersConfigurator.configureIdp(realmResource)

      verify(exactly = 0) { identityProviderResource.update(any()) }
    }

    @Test
    fun testMultipleExistingIdpsNoneMarked() {
      val otherUnmarkedIdp = mockk<IdentityProviderRepresentation>()
      every { otherUnmarkedIdp.config } returns mapOf()
      every { identityProviderRepresentation.config } returns mapOf()

      every { realmResource.identityProviders() } returns identityProvidersResource
      every { identityProvidersResource.findAll() } returns listOf(otherUnmarkedIdp, identityProviderRepresentation)

      val importedMap = mutableMapOf<String, String>()
      val configMap = mutableMapOf<String, String>()
      every { configurationMapService.importProviderFrom(realmResource, oidcConfig, "oidc") } returns importedMap
      every { configurationMapService.setupProviderConfig(oidcConfig, importedMap) } returns configMap

      identityProvidersConfigurator.configureIdp(realmResource)

      verify(exactly = 0) { identityProviderResource.update(any()) }
    }

    @Test
    fun testCreateFailureThrows() {
      every { realmResource.identityProviders() } returns identityProvidersResource
      every { identityProvidersResource.findAll() } returns mutableListOf()
      every { identityProvidersResource.create(any()) } returns Response.status(Response.Status.BAD_REQUEST).build()

      val configMap: MutableMap<String, String> = HashMap()
      every { configurationMapService.importProviderFrom(realmResource, oidcConfig, "oidc") } returns configMap
      every { configurationMapService.setupProviderConfig(oidcConfig, configMap) } returns configMap

      Assertions.assertThrows(
        RuntimeException::class.java,
      ) {
        identityProvidersConfigurator.configureIdp(realmResource)
      }
    }
  }
}
