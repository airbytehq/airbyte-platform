/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.keycloak

import io.airbyte.commons.auth.keycloak.ClientScopeConfigurator
import io.airbyte.config.Application
import io.airbyte.config.AuthenticatedUser
import io.airbyte.micronaut.runtime.AirbyteAuthConfig
import io.airbyte.micronaut.runtime.AirbyteKeycloakConfig
import io.mockk.every
import io.mockk.mockk
import io.mockk.spyk
import jakarta.ws.rs.BadRequestException
import jakarta.ws.rs.core.Response
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.keycloak.admin.client.Keycloak
import org.keycloak.admin.client.resource.ClientsResource
import org.keycloak.admin.client.resource.RealmResource
import org.keycloak.admin.client.resource.UsersResource
import org.keycloak.representations.idm.ClientRepresentation
import java.net.URI
import java.util.UUID

internal class ApplicationServiceKeycloakImplTests {
  private var keycloakConfiguration: AirbyteKeycloakConfig? = null

  private val keycloakClient: Keycloak = mockk<Keycloak>()
  private val realmResource: RealmResource = mockk<RealmResource>()
  private val clientsResource: ClientsResource = mockk<ClientsResource>()
  private val usersResource: UsersResource = mockk<UsersResource>()
  private val clientScopeConfigurator: ClientScopeConfigurator = mockk<ClientScopeConfigurator>()

  private var apiKeyServiceKeycloakImpl: ApplicationServiceKeycloakImpl? = null

  @BeforeEach
  fun setUp() {
    keycloakConfiguration = AirbyteKeycloakConfig(protocol = "http", host = "localhost:8080", clientRealm = REALM_NAME)

    every { keycloakClient.realm(REALM_NAME) } returns realmResource
    every { realmResource.clients() } returns clientsResource
    every { realmResource.users() } returns usersResource
    every { clientScopeConfigurator.configureClientScope(any()) } returns Unit

    every {
      clientsResource.create(any(ClientRepresentation::class))
    } returns Response.created(URI.create("https://company.example")).build()

    apiKeyServiceKeycloakImpl =
      spyk(
        ApplicationServiceKeycloakImpl(
          keycloakClient,
          keycloakConfiguration!!,
          clientScopeConfigurator,
          AirbyteAuthConfig(),
        ),
        recordPrivateCalls = true,
      )
  }

  @Test
  fun testNoMoreThanTwoApiKeys() {
    val user = AuthenticatedUser().withUserId(UUID.fromString("6287ecb9-f9fb-4062-a12b-20479b6d2dde"))

    every {
      apiKeyServiceKeycloakImpl!!.listApplicationsByUser(user)
    } returns
      listOf(
        buildApplication(user, TEST_1, 0),
        buildApplication(user, TEST_2, 1),
      )

    Assertions.assertThrows(
      BadRequestException::class.java,
    ) { apiKeyServiceKeycloakImpl!!.createApplication(user, "test3") }
  }

  @Test
  fun testApiKeyNameAlreadyExists() {
    val user = AuthenticatedUser().withUserId(UUID.fromString("4bb2a760-a0b6-4936-aea0-a13fada349f4"))

    every {
      apiKeyServiceKeycloakImpl!!.listApplicationsByUser(user)
    } returns listOf(buildApplication(user, TEST_1, 0))

    every {
      clientsResource.findByClientId(
        buildClientId(
          "4bb2a760-a0b6-4936-aea0-a13fada349f4",
        ),
      )
    } returns mutableListOf(buildClientRepresentation(user, TEST_1, 0))

    Assertions.assertThrows(
      BadRequestException::class.java,
    ) { apiKeyServiceKeycloakImpl!!.createApplication(user, TEST_1) }
  }

  @Test
  fun testBadKeycloakCreateResponse() {
    val user = AuthenticatedUser().withUserId(UUID.fromString("b3600891-e7c7-4278-8a94-8b838985de2a"))
    every {
      clientsResource.create(any(ClientRepresentation::class))
    } returns Response.status(500).build()

    every {
      apiKeyServiceKeycloakImpl!!.listApplicationsByUser(user)
    } returns mutableListOf()

    every {
      clientsResource.findByClientId(
        buildClientId(
          "b3600891-e7c7-4278-8a94-8b838985de2a",
        ),
      )
    } returns listOf(buildClientRepresentation(user, TEST_1, 0))

    Assertions.assertThrows(
      BadRequestException::class.java,
    ) { apiKeyServiceKeycloakImpl!!.createApplication(user, TEST_1) }
    assert(
      apiKeyServiceKeycloakImpl!!
        .listApplicationsByUser(
          user,
        ).isEmpty(),
    )
  }

  @Test
  fun testListKeysForUser() {
    val user = AuthenticatedUser().withUserId(UUID.fromString("58b32b0c-acef-47b9-8e3d-1c83adc7ce59"))

    // Note: This can be quickly refactored into an integration test, but for now we mock creating.
    every {
      apiKeyServiceKeycloakImpl!!.listApplicationsByUser(user)
    } returns
      listOf(
        buildApplication(user, TEST_1, 0),
      )

    every {
      apiKeyServiceKeycloakImpl!!.createApplication(user, TEST_1)
    } returns Application()

    var apiKeys =
      apiKeyServiceKeycloakImpl!!.listApplicationsByUser(
        user,
      )
    assert(apiKeys.size == 1)

    every {
      apiKeyServiceKeycloakImpl!!.createApplication(user, TEST_2)
    } returns Application()

    every {
      apiKeyServiceKeycloakImpl!!.listApplicationsByUser(user)
    } returns
      listOf(
        buildApplication(user, TEST_1, 0),
        buildApplication(user, TEST_2, 1),
      )
    apiKeys =
      apiKeyServiceKeycloakImpl!!.listApplicationsByUser(
        user,
      )
    assert(apiKeys.size == 2)
  }

  private fun buildClientRepresentation(
    user: AuthenticatedUser,
    name: String?,
    index: Int,
  ): ClientRepresentation {
    val clientRepresentation = ClientRepresentation()
    clientRepresentation.setClientId("${user.userId}-$index")
    clientRepresentation.setName(name)
    clientRepresentation.setSecret("test")
    val attributes =
      hashMapOf(
        "user_id" to user.userId.toString(),
        "client.secret.creation.time" to "365",
      )
    clientRepresentation.setAttributes(attributes)
    return clientRepresentation
  }

  private fun buildApplication(
    user: AuthenticatedUser,
    name: String?,
    index: Int,
  ): Application =
    Application()
      .withClientId("${user.userId}-$index")
      .withName(name)
      .withClientSecret("test")
      .withId(UUID.randomUUID().toString())

  companion object {
    private const val TEST_1 = "test1"
    private const val TEST_2 = "test2"
    private const val REALM_NAME = "testRealm"

    private fun buildClientId(userId: String?): String = "$userId-0"
  }
}
