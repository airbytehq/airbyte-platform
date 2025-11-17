/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.keycloak.setup

import io.airbyte.commons.auth.keycloak.ClientScopeConfigurator
import io.airbyte.micronaut.runtime.AirbyteConfig
import io.airbyte.micronaut.runtime.AirbyteKeycloakConfig
import io.mockk.every
import io.mockk.impl.annotations.MockK
import io.mockk.junit5.MockKExtension
import io.mockk.slot
import io.mockk.verify
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.keycloak.admin.client.Keycloak
import org.keycloak.admin.client.resource.RealmResource
import org.keycloak.admin.client.resource.RealmsResource
import org.keycloak.representations.idm.RealmRepresentation

@ExtendWith(MockKExtension::class)
internal class KeycloakServerTest {
  @MockK
  private lateinit var keycloakAdminClientProvider: KeycloakAdminClientProvider

  @MockK
  private lateinit var keycloakConfiguration: AirbyteKeycloakConfig

  @MockK
  private lateinit var userConfigurator: UserConfigurator

  @MockK
  private lateinit var webClientConfigurator: WebClientConfigurator

  @MockK
  private lateinit var identityProvidersConfigurator: IdentityProvidersConfigurator

  @MockK
  private lateinit var clientScopeConfigurator: ClientScopeConfigurator

  @MockK
  private lateinit var keycloakAdminClient: Keycloak

  @MockK
  private lateinit var realmsResource: RealmsResource

  @MockK
  private lateinit var airbyteRealm: RealmResource

  @MockK
  private lateinit var airbyteRealmRep: RealmRepresentation

  private lateinit var keycloakServer: KeycloakServer

  @BeforeEach
  fun setUp() {
    every { keycloakConfiguration.basePath } returns "/io/airbyte/api/client/auth"
    every { keycloakConfiguration.protocol } returns "http"
    every { keycloakConfiguration.host } returns "localhost"
    every { keycloakConfiguration.airbyteRealm } returns REALM_NAME
    every { keycloakAdminClientProvider.createKeycloakAdminClient(any()) } returns keycloakAdminClient
    every { keycloakAdminClient.realms() } returns realmsResource
    every { realmsResource.findAll() } returns mutableListOf()
    every { keycloakAdminClient.realm(any()) } returns airbyteRealm
    every { airbyteRealm.toRepresentation() } returns airbyteRealmRep

    keycloakServer =
      KeycloakServer(
        keycloakAdminClientProvider,
        keycloakConfiguration,
        userConfigurator,
        webClientConfigurator,
        identityProvidersConfigurator,
        clientScopeConfigurator,
        AirbyteConfig(airbyteUrl = WEBAPP_URL),
      )
  }

  @Test
  fun testSetupAirbyteRealmWhenRealmDoesNotExist() {
    every { realmsResource.create(any()) } returns Unit
    every { userConfigurator.configureUser(airbyteRealm) } returns Unit
    every { webClientConfigurator.configureWebClient(airbyteRealm) } returns Unit
    every { identityProvidersConfigurator.configureIdp(airbyteRealm) } returns Unit
    every { clientScopeConfigurator.configureClientScope(airbyteRealm) } returns Unit
    every { airbyteRealmRep.attributesOrEmpty } returns mutableMapOf()
    every { airbyteRealmRep.setAttributes(any()) } returns Unit
    every { airbyteRealm.update(airbyteRealmRep) } returns Unit

    keycloakServer.setupAirbyteRealm()

    verify(exactly = 1) { realmsResource.findAll() }
    verify(exactly = 1) { realmsResource.create(any<RealmRepresentation>()) }

    verify(exactly = 1) { userConfigurator.configureUser(airbyteRealm) }
    verify(exactly = 1) { webClientConfigurator.configureWebClient(airbyteRealm) }
    verify(exactly = 1) { identityProvidersConfigurator.configureIdp(airbyteRealm) }

    val attributesSlot = slot<Map<String, String>>()
    verify(exactly = 1) { airbyteRealmRep.setAttributes(capture(attributesSlot)) }
    Assertions.assertEquals(WEBAPP_URL + AUTH_PATH, attributesSlot.captured[FRONTEND_URL_ATTRIBUTE])

    verify(exactly = 1) { airbyteRealm.update(airbyteRealmRep) }
  }

  @Test
  fun testCreateAirbyteRealmWhenRealmAlreadyExists() {
    val existingRealm = RealmRepresentation()
    existingRealm.setRealm(REALM_NAME)

    every { realmsResource.findAll() } returns mutableListOf(existingRealm)
    every { keycloakConfiguration.airbyteRealm } returns REALM_NAME
    every { userConfigurator.configureUser(airbyteRealm) } returns Unit
    every { webClientConfigurator.configureWebClient(airbyteRealm) } returns Unit
    every { identityProvidersConfigurator.configureIdp(airbyteRealm) } returns Unit
    every { clientScopeConfigurator.configureClientScope(airbyteRealm) } returns Unit
    every { airbyteRealmRep.attributesOrEmpty } returns mutableMapOf()
    every { airbyteRealmRep.setAttributes(any()) } returns Unit
    every { airbyteRealm.update(airbyteRealmRep) } returns Unit

    keycloakServer.setupAirbyteRealm()

    verify(exactly = 1) { realmsResource.findAll() }
    verify(exactly = 0) { realmsResource.create(any<RealmRepresentation>()) }

    verify(exactly = 1) { userConfigurator.configureUser(airbyteRealm) }
    verify(exactly = 1) { webClientConfigurator.configureWebClient(airbyteRealm) }
    verify(exactly = 1) { identityProvidersConfigurator.configureIdp(airbyteRealm) }

    val attributesSlot = slot<Map<String, String>>()
    verify(exactly = 1) { airbyteRealmRep.setAttributes(capture(attributesSlot)) }
    Assertions.assertEquals(WEBAPP_URL + AUTH_PATH, attributesSlot.captured[FRONTEND_URL_ATTRIBUTE])

    verify(exactly = 1) { airbyteRealm.update(airbyteRealmRep) }
  }

  @Test
  fun testBuildRealmRepresentation() {
    every { realmsResource.create(any()) } returns Unit
    every { userConfigurator.configureUser(airbyteRealm) } returns Unit
    every { webClientConfigurator.configureWebClient(airbyteRealm) } returns Unit
    every { identityProvidersConfigurator.configureIdp(airbyteRealm) } returns Unit
    every { clientScopeConfigurator.configureClientScope(airbyteRealm) } returns Unit
    every { airbyteRealmRep.attributesOrEmpty } returns mutableMapOf()
    every { airbyteRealmRep.setAttributes(any()) } returns Unit
    every { airbyteRealm.update(airbyteRealmRep) } returns Unit

    keycloakServer.setupAirbyteRealm()

    val realmSlot = slot<RealmRepresentation>()
    verify { realmsResource.create(capture(realmSlot)) }

    val createdRealm = realmSlot.captured
    Assertions.assertEquals(REALM_NAME, createdRealm.realm)
    Assertions.assertTrue(createdRealm.isEnabled)
    Assertions.assertEquals("airbyte-keycloak-theme", createdRealm.loginTheme)
  }

  @Test
  fun testRecreateAirbyteRealm() {
    val existingRealm = RealmRepresentation()
    existingRealm.setRealm(REALM_NAME)
    every { realmsResource.findAll() } returns mutableListOf(existingRealm)
    every { airbyteRealm.remove() } returns Unit
    every { realmsResource.create(any()) } returns Unit
    every { userConfigurator.configureUser(airbyteRealm) } returns Unit
    every { webClientConfigurator.configureWebClient(airbyteRealm) } returns Unit
    every { identityProvidersConfigurator.configureIdp(airbyteRealm) } returns Unit
    every { clientScopeConfigurator.configureClientScope(airbyteRealm) } returns Unit
    every { airbyteRealmRep.attributesOrEmpty } returns mutableMapOf()
    every { airbyteRealmRep.setAttributes(any()) } returns Unit
    every { airbyteRealm.update(airbyteRealmRep) } returns Unit

    keycloakServer.destroyAndRecreateAirbyteRealm()

    verify(exactly = 1) { airbyteRealm.remove() }
    verify(exactly = 1) { realmsResource.create(any<RealmRepresentation>()) }

    verify(exactly = 1) { userConfigurator.configureUser(airbyteRealm) }
    verify(exactly = 1) { webClientConfigurator.configureWebClient(airbyteRealm) }
    verify(exactly = 1) { identityProvidersConfigurator.configureIdp(airbyteRealm) }

    val attributesSlot = slot<Map<String, String>>()
    verify(exactly = 1) { airbyteRealmRep.setAttributes(capture(attributesSlot)) }
    Assertions.assertEquals(WEBAPP_URL + AUTH_PATH, attributesSlot.captured[FRONTEND_URL_ATTRIBUTE])

    verify(exactly = 1) { airbyteRealm.update(airbyteRealmRep) }
  }

  @Test
  fun testRecreateAirbyteRealmWhenRealmDoesNotExist() {
    every { realmsResource.findAll() } returns mutableListOf()
    every { realmsResource.create(any()) } returns Unit
    every { userConfigurator.configureUser(airbyteRealm) } returns Unit
    every { webClientConfigurator.configureWebClient(airbyteRealm) } returns Unit
    every { identityProvidersConfigurator.configureIdp(airbyteRealm) } returns Unit
    every { clientScopeConfigurator.configureClientScope(airbyteRealm) } returns Unit
    every { airbyteRealmRep.attributesOrEmpty } returns mutableMapOf()
    every { airbyteRealmRep.setAttributes(any()) } returns Unit
    every { airbyteRealm.update(airbyteRealmRep) } returns Unit

    keycloakServer.destroyAndRecreateAirbyteRealm()

    verify(exactly = 0) { airbyteRealm.remove() }
    verify(exactly = 1) { realmsResource.create(any<RealmRepresentation>()) }

    verify(exactly = 1) { userConfigurator.configureUser(airbyteRealm) }
    verify(exactly = 1) { webClientConfigurator.configureWebClient(airbyteRealm) }
    verify(exactly = 1) { identityProvidersConfigurator.configureIdp(airbyteRealm) }

    val attributesSlot = slot<Map<String, String>>()
    verify(exactly = 1) { airbyteRealmRep.setAttributes(capture(attributesSlot)) }
    Assertions.assertEquals(WEBAPP_URL + AUTH_PATH, attributesSlot.captured[FRONTEND_URL_ATTRIBUTE])

    verify(exactly = 1) { airbyteRealm.update(airbyteRealmRep) }
  }

  companion object {
    private const val REALM_NAME = "airbyte"
    private const val WEBAPP_URL = "http://localhost:8000"
    private const val AUTH_PATH = "/io/airbyte/api/client/auth"
    private const val FRONTEND_URL_ATTRIBUTE = "frontendUrl"
  }
}
