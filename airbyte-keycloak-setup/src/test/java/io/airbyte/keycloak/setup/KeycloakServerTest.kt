/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.keycloak.setup

import io.airbyte.commons.auth.config.AirbyteKeycloakConfiguration
import io.airbyte.commons.auth.keycloak.ClientScopeConfigurator
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.keycloak.admin.client.Keycloak
import org.keycloak.admin.client.resource.RealmResource
import org.keycloak.admin.client.resource.RealmsResource
import org.keycloak.representations.idm.RealmRepresentation
import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers
import org.mockito.Mock
import org.mockito.Mockito
import org.mockito.MockitoAnnotations

internal class KeycloakServerTest {
  @Mock
  private lateinit var keycloakAdminClientProvider: KeycloakAdminClientProvider

  @Mock
  private lateinit var keycloakConfiguration: AirbyteKeycloakConfiguration

  @Mock
  private lateinit var userConfigurator: UserConfigurator

  @Mock
  private lateinit var webClientConfigurator: WebClientConfigurator

  @Mock
  private lateinit var identityProvidersConfigurator: IdentityProvidersConfigurator

  @Mock
  private lateinit var clientScopeConfigurator: ClientScopeConfigurator

  @Mock
  private lateinit var keycloakAdminClient: Keycloak

  @Mock
  private lateinit var realmsResource: RealmsResource

  @Mock
  private lateinit var airbyteRealm: RealmResource

  @Mock
  private lateinit var airbyteRealmRep: RealmRepresentation

  private lateinit var keycloakServer: KeycloakServer

  @BeforeEach
  fun setUp() {
    MockitoAnnotations.openMocks(this)

    Mockito.`when`(keycloakConfiguration.basePath).thenReturn("/io/airbyte/api/client/auth")
    Mockito.`when`(keycloakConfiguration.protocol).thenReturn("http")
    Mockito.`when`(keycloakConfiguration.host).thenReturn("localhost")
    Mockito.`when`(keycloakConfiguration.airbyteRealm).thenReturn(REALM_NAME)
    Mockito
      .`when`(keycloakAdminClientProvider.createKeycloakAdminClient(ArgumentMatchers.anyString()))
      .thenReturn(keycloakAdminClient)
    Mockito.`when`(keycloakAdminClient.realms()).thenReturn(realmsResource)
    Mockito.`when`(realmsResource.findAll()).thenReturn(mutableListOf())
    Mockito.`when`(keycloakAdminClient.realm(ArgumentMatchers.anyString())).thenReturn(airbyteRealm)
    Mockito.`when`(airbyteRealm.toRepresentation()).thenReturn(airbyteRealmRep)

    keycloakServer =
      KeycloakServer(
        keycloakAdminClientProvider,
        keycloakConfiguration,
        userConfigurator,
        webClientConfigurator,
        identityProvidersConfigurator,
        clientScopeConfigurator,
        WEBAPP_URL,
      )
  }

  @Test
  fun testSetupAirbyteRealmWhenRealmDoesNotExist() {
    keycloakServer.setupAirbyteRealm()

    Mockito.verify(realmsResource, Mockito.times(1)).findAll()
    Mockito
      .verify(realmsResource, Mockito.times(1))
      .create(ArgumentMatchers.any(RealmRepresentation::class.java))

    Mockito.verify(userConfigurator, Mockito.times(1)).configureUser(airbyteRealm)
    Mockito.verify(webClientConfigurator, Mockito.times(1)).configureWebClient(airbyteRealm)
    Mockito.verify(identityProvidersConfigurator, Mockito.times(1)).configureIdp(airbyteRealm)

    Mockito.verify(airbyteRealmRep, Mockito.times(1)).setAttributes(
      ArgumentMatchers.argThat { map: Map<String, String> ->
        map[FRONTEND_URL_ATTRIBUTE] == WEBAPP_URL + AUTH_PATH
      },
    )

    Mockito.verify(airbyteRealm, Mockito.times(1)).update(airbyteRealmRep)
  }

  @Test
  fun testCreateAirbyteRealmWhenRealmAlreadyExists() {
    val existingRealm = RealmRepresentation()
    existingRealm.setRealm(REALM_NAME)

    Mockito.`when`(realmsResource.findAll()).thenReturn(mutableListOf(existingRealm))
    Mockito.`when`(keycloakConfiguration.airbyteRealm).thenReturn(REALM_NAME)

    keycloakServer.setupAirbyteRealm()

    Mockito.verify(realmsResource, Mockito.times(1)).findAll()
    Mockito
      .verify(realmsResource, Mockito.times(0))
      .create(ArgumentMatchers.any(RealmRepresentation::class.java))
    // create not called, but other configuration methods should be called every time

    Mockito
      .verify(userConfigurator, Mockito.times(1))
      .configureUser(airbyteRealm)

    Mockito
      .verify(webClientConfigurator, Mockito.times(1))
      .configureWebClient(airbyteRealm)

    Mockito
      .verify(identityProvidersConfigurator, Mockito.times(1))
      .configureIdp(airbyteRealm)

    Mockito.verify(airbyteRealmRep, Mockito.times(1)).setAttributes(
      ArgumentMatchers.argThat { map: Map<String, String> ->
        map[FRONTEND_URL_ATTRIBUTE] == WEBAPP_URL + AUTH_PATH
      },
    )

    Mockito.verify(airbyteRealm, Mockito.times(1)).update(airbyteRealmRep)
  }

  @Test
  fun testBuildRealmRepresentation() {
    keycloakServer.setupAirbyteRealm()

    val realmRepresentationCaptor = ArgumentCaptor.forClass(RealmRepresentation::class.java)
    Mockito.verify(realmsResource).create(realmRepresentationCaptor.capture())

    val createdRealm = realmRepresentationCaptor.value
    Assertions.assertEquals(REALM_NAME, createdRealm.realm)
    Assertions.assertTrue(createdRealm.isEnabled)
    Assertions.assertEquals("airbyte-keycloak-theme", createdRealm.loginTheme)
  }

  @Test
  fun testRecreateAirbyteRealm() {
    val existingRealm = RealmRepresentation()
    existingRealm.setRealm(REALM_NAME)
    Mockito.`when`(realmsResource.findAll()).thenReturn(mutableListOf(existingRealm))

    keycloakServer.destroyAndRecreateAirbyteRealm()

    Mockito.verify(airbyteRealm, Mockito.times(1)).remove()
    Mockito
      .verify(realmsResource, Mockito.times(1))
      .create(ArgumentMatchers.any(RealmRepresentation::class.java))

    Mockito.verify(userConfigurator, Mockito.times(1)).configureUser(airbyteRealm)
    Mockito.verify(webClientConfigurator, Mockito.times(1)).configureWebClient(airbyteRealm)
    Mockito.verify(identityProvidersConfigurator, Mockito.times(1)).configureIdp(airbyteRealm)

    Mockito.verify(airbyteRealmRep, Mockito.times(1)).setAttributes(
      ArgumentMatchers.argThat { map: Map<String, String> ->
        map[FRONTEND_URL_ATTRIBUTE] == WEBAPP_URL + AUTH_PATH
      },
    )

    Mockito.verify(airbyteRealm, Mockito.times(1)).update(airbyteRealmRep)
  }

  @Test
  fun testRecreateAirbyteRealmWhenRealmDoesNotExist() {
    Mockito.`when`(realmsResource.findAll()).thenReturn(mutableListOf())

    keycloakServer.destroyAndRecreateAirbyteRealm()

    Mockito.verify(airbyteRealm, Mockito.times(0)).remove()
    Mockito
      .verify(realmsResource, Mockito.times(1))
      .create(ArgumentMatchers.any(RealmRepresentation::class.java))

    Mockito.verify(userConfigurator, Mockito.times(1)).configureUser(airbyteRealm)
    Mockito.verify(webClientConfigurator, Mockito.times(1)).configureWebClient(airbyteRealm)
    Mockito.verify(identityProvidersConfigurator, Mockito.times(1)).configureIdp(airbyteRealm)

    Mockito.verify(airbyteRealmRep, Mockito.times(1)).setAttributes(
      ArgumentMatchers.argThat { map: Map<String, String> ->
        map[FRONTEND_URL_ATTRIBUTE] == WEBAPP_URL + AUTH_PATH
      },
    )

    Mockito.verify(airbyteRealm, Mockito.times(1)).update(airbyteRealmRep)
  }

  companion object {
    private const val REALM_NAME = "airbyte"
    private const val WEBAPP_URL = "http://localhost:8000"
    private const val AUTH_PATH = "/io/airbyte/api/client/auth"
    private const val FRONTEND_URL_ATTRIBUTE = "frontendUrl"
  }
}
