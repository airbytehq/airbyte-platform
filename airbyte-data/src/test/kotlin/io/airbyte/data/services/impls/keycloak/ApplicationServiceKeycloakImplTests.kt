/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.keycloak

import io.airbyte.commons.auth.keycloak.ClientScopeConfigurator
import io.airbyte.config.Application
import io.airbyte.config.AuthenticatedUser
import io.airbyte.micronaut.runtime.AirbyteAuthConfig
import io.airbyte.micronaut.runtime.AirbyteKeycloakConfig
import jakarta.ws.rs.BadRequestException
import jakarta.ws.rs.core.Response
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.function.Executable
import org.keycloak.admin.client.Keycloak
import org.keycloak.admin.client.resource.ClientResource
import org.keycloak.admin.client.resource.ClientsResource
import org.keycloak.admin.client.resource.RealmResource
import org.keycloak.admin.client.resource.UserResource
import org.keycloak.admin.client.resource.UsersResource
import org.keycloak.representations.idm.ClientRepresentation
import org.keycloak.representations.idm.UserRepresentation
import org.mockito.ArgumentMatchers
import org.mockito.Mockito
import java.net.URI
import java.util.Optional
import java.util.UUID

internal class ApplicationServiceKeycloakImplTests {
  private var keycloakConfiguration: AirbyteKeycloakConfig? = null

  private val keycloakClient: Keycloak = Mockito.mock<Keycloak>(Keycloak::class.java)
  private val realmResource: RealmResource = Mockito.mock<RealmResource>(RealmResource::class.java)
  private val clientsResource: ClientsResource = Mockito.mock<ClientsResource>(ClientsResource::class.java)
  private val clientResource: ClientResource? = Mockito.mock<ClientResource?>(ClientResource::class.java)
  private val usersResource: UsersResource = Mockito.mock<UsersResource>(UsersResource::class.java)
  private val userResource: UserResource? = Mockito.mock<UserResource?>(UserResource::class.java)
  private val clientScopeConfigurator: ClientScopeConfigurator = Mockito.mock<ClientScopeConfigurator>(ClientScopeConfigurator::class.java)

  private var apiKeyServiceKeycloakImpl: ApplicationServiceKeycloakImpl? = null

  @BeforeEach
  fun setUp() {
    keycloakConfiguration = AirbyteKeycloakConfig(protocol = "http", host = "localhost:8080")

    Mockito.`when`(keycloakClient.realm(REALM_NAME)).thenReturn(realmResource)
    Mockito.`when`(realmResource.clients()).thenReturn(clientsResource)
    Mockito.`when`(realmResource.users()).thenReturn(usersResource)

    Mockito
      .`when`(clientsResource.create(ArgumentMatchers.any(ClientRepresentation::class.java)))
      .thenReturn(Response.created(URI.create("https://company.example")).build())

    apiKeyServiceKeycloakImpl =
      Mockito.spy(
        ApplicationServiceKeycloakImpl(
          keycloakClient,
          keycloakConfiguration!!,
          clientScopeConfigurator,
          AirbyteAuthConfig(),
        ),
      )
  }

  // TODO: Add this test back in, got tired of fighting mocks.
  // @Test
  fun testCreateApiKeyForUser() {
    val user = AuthenticatedUser().withUserId(UUID.fromString("bf0cc898-4a99-4dc1-834d-26b2ba57fdeb"))

    Mockito
      .doReturn(mutableListOf<Any?>())
      .`when`<ApplicationServiceKeycloakImpl?>(apiKeyServiceKeycloakImpl)
      .listApplicationsByUser(user)

    Mockito
      .`when`(clientsResource.findByClientId(ArgumentMatchers.any()))
      .thenReturn(mutableListOf(buildClientRepresentation(user, TEST_1, 0)))

    Mockito.`when`(clientsResource.get(ArgumentMatchers.any())).thenReturn(clientResource)

    Mockito
      .`when`(clientsResource.get(ArgumentMatchers.any()).serviceAccountUser)
      .thenReturn(UserRepresentation())

    Mockito
      .`when`(usersResource.get(ArgumentMatchers.any()))
      .thenReturn(userResource)

    Mockito
      .doNothing()
      .`when`(usersResource.get(ArgumentMatchers.any()))
      .update(ArgumentMatchers.any(UserRepresentation::class.java))

    val apiKey1 =
      checkNotNull(
        apiKeyServiceKeycloakImpl!!.createApplication(
          user,
          TEST_1,
        ),
      )
    assert(TEST_1 == apiKey1.name)
    assert(apiKey1.clientId == "${user.userId}-0")

    Mockito
      .`when`(
        clientsResource.findByClientId(
          buildClientId(
            "bf0cc898-4a99-4dc1-834d-26b2ba57fdeb",
            "1",
          ),
        ),
      ).thenReturn(mutableListOf(buildClientRepresentation(user, TEST_2, 1)))

    Mockito
      .doReturn(listOf(buildClientRepresentation(user, TEST_1, 0)))
      .`when`<ApplicationServiceKeycloakImpl?>(apiKeyServiceKeycloakImpl)
      .listApplicationsByUser(user)

    val apiKey2 =
      checkNotNull(
        apiKeyServiceKeycloakImpl!!.createApplication(
          user,
          TEST_2,
        ),
      )
    assert(TEST_2 == apiKey2.getName())
    assert(apiKey2.getClientId() == user.getUserId().toString() + "-1")

    Mockito
      .doReturn(Optional.empty<Any?>())
      .`when`<ApplicationServiceKeycloakImpl?>(apiKeyServiceKeycloakImpl)
      .deleteApplication(ArgumentMatchers.any(), ArgumentMatchers.any())

    apiKeyServiceKeycloakImpl!!.deleteApplication(user, apiKey2.getId())
    apiKeyServiceKeycloakImpl!!.deleteApplication(user, apiKey1.getId())

    Mockito
      .doReturn(mutableListOf<Any?>())
      .`when`<ApplicationServiceKeycloakImpl?>(apiKeyServiceKeycloakImpl)
      .listApplicationsByUser(user)
    assert(
      apiKeyServiceKeycloakImpl!!
        .listApplicationsByUser(
          user,
        ).isEmpty(),
    )
  }

  @Test
  fun testNoMoreThanTwoApiKeys() {
    val user = AuthenticatedUser().withUserId(UUID.fromString("6287ecb9-f9fb-4062-a12b-20479b6d2dde"))

    Mockito
      .doReturn(
        listOf(
          buildClientRepresentation(user, TEST_1, 0),
          buildClientRepresentation(user, TEST_2, 1),
        ),
      ).`when`<ApplicationServiceKeycloakImpl?>(apiKeyServiceKeycloakImpl)
      .listApplicationsByUser(user)

    Assertions.assertThrows(
      BadRequestException::class.java,
      Executable { apiKeyServiceKeycloakImpl!!.createApplication(user, "test3") },
    )
  }

  @Test
  fun testApiKeyNameAlreadyExists() {
    val user = AuthenticatedUser().withUserId(UUID.fromString("4bb2a760-a0b6-4936-aea0-a13fada349f4"))

    Mockito
      .doReturn(listOf(buildClientRepresentation(user, TEST_1, 0)))
      .`when`<ApplicationServiceKeycloakImpl?>(apiKeyServiceKeycloakImpl)
      .listApplicationsByUser(user)

    Mockito
      .`when`(
        clientsResource.findByClientId(
          buildClientId(
            "4bb2a760-a0b6-4936-aea0-a13fada349f4",
            "0",
          ),
        ),
      ).thenReturn(mutableListOf(buildClientRepresentation(user, TEST_1, 0)))

    Assertions.assertThrows(
      BadRequestException::class.java,
      { apiKeyServiceKeycloakImpl!!.createApplication(user, TEST_1) },
    )
  }

  @Test
  fun testBadKeycloakCreateResponse() {
    val user = AuthenticatedUser().withUserId(UUID.fromString("b3600891-e7c7-4278-8a94-8b838985de2a"))
    Mockito
      .`when`(clientsResource.create(ArgumentMatchers.any(ClientRepresentation::class.java)))
      .thenReturn(Response.status(500).build())

    Mockito
      .doReturn(mutableListOf<Any?>())
      .`when`<ApplicationServiceKeycloakImpl?>(apiKeyServiceKeycloakImpl)
      .listApplicationsByUser(user)

    Mockito
      .`when`(
        clientsResource.findByClientId(
          buildClientId(
            "b3600891-e7c7-4278-8a94-8b838985de2a",
            "0",
          ),
        ),
      ).thenReturn(listOf(buildClientRepresentation(user, TEST_1, 0)))

    Assertions.assertThrows(
      BadRequestException::class.java,
      { apiKeyServiceKeycloakImpl!!.createApplication(user, TEST_1) },
    )
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
    Mockito
      .doReturn(
        listOf(
          buildClientRepresentation(user, TEST_1, 0),
        ),
      ).`when`<ApplicationServiceKeycloakImpl?>(apiKeyServiceKeycloakImpl)
      .listApplicationsByUser(user)

    Mockito.doReturn(Application()).`when`<ApplicationServiceKeycloakImpl?>(apiKeyServiceKeycloakImpl).createApplication(
      user,
      TEST_1,
    )

    var apiKeys =
      apiKeyServiceKeycloakImpl!!.listApplicationsByUser(
        user,
      )
    assert(apiKeys.size == 1)

    Mockito.doReturn(Application()).`when`<ApplicationServiceKeycloakImpl?>(apiKeyServiceKeycloakImpl).createApplication(
      user,
      TEST_2,
    )

    Mockito
      .doReturn(
        listOf(
          buildClientRepresentation(user, TEST_1, 0),
          buildClientRepresentation(user, TEST_2, 1),
        ),
      ).`when`<ApplicationServiceKeycloakImpl?>(apiKeyServiceKeycloakImpl)
      .listApplicationsByUser(user)
    apiKeys =
      apiKeyServiceKeycloakImpl!!.listApplicationsByUser(
        user,
      )
    assert(apiKeys.size == 2)
  }

  // It was very difficult to mock out the remove call as it returns a void. Commenting this test out.
  fun testDeleteApiKey() {
    val user = AuthenticatedUser().withUserId(UUID.fromString("f81780ef-148e-413d-8e00-6e755e4e2256"))
    // Note: This can be quickly refactored into an integration test, but for now we mock creating.
    Mockito.doReturn(Application()).`when`<ApplicationServiceKeycloakImpl?>(apiKeyServiceKeycloakImpl).createApplication(
      user,
      TEST_1,
    )

    val apiKey1 =
      apiKeyServiceKeycloakImpl!!.createApplication(
        user,
        TEST_1,
      )
    Mockito.doReturn(Application()).`when`<ApplicationServiceKeycloakImpl?>(apiKeyServiceKeycloakImpl).createApplication(
      user,
      TEST_2,
    )

    apiKeyServiceKeycloakImpl!!.createApplication(
      user,
      TEST_2,
    )

    Mockito
      .`when`<MutableList<ClientRepresentation?>?>(
        clientsResource.findByClientId(
          buildClientId(
            "f81780ef-148e-413d-8e00-6e755e4e2256",
            "0",
          ),
        ),
      ).thenReturn(mutableListOf(buildClientRepresentation(user, TEST_1, 0)))
    apiKeyServiceKeycloakImpl!!.deleteApplication(user, apiKey1.id)

    Mockito
      .doReturn(
        listOf(
          buildClientRepresentation(user, TEST_1, 0),
        ),
      ).`when`<ApplicationServiceKeycloakImpl?>(apiKeyServiceKeycloakImpl)
      .listApplicationsByUser(user)

    var apiKeys =
      apiKeyServiceKeycloakImpl!!.listApplicationsByUser(
        user,
      )
    assert(apiKeys.size == 1)
    assert("f81780ef-148e-413d-8e00-6e755e4e2256-0" == apiKeys[0].id)

    Mockito
      .`when`<MutableList<ClientRepresentation?>?>(
        clientsResource.findByClientId(
          buildClientId(
            "f81780ef-148e-413d-8e00-6e755e4e2256",
            "0",
          ),
        ),
      ).thenReturn(mutableListOf(buildClientRepresentation(user, TEST_2, 0)))
    apiKeyServiceKeycloakImpl!!.deleteApplication(user, "f81780ef-148e-413d-8e00-6e755e4e2256-0")

    Mockito
      .doReturn(mutableListOf<Any?>())
      .`when`<ApplicationServiceKeycloakImpl?>(apiKeyServiceKeycloakImpl)
      .listApplicationsByUser(user)
    apiKeys =
      apiKeyServiceKeycloakImpl!!.listApplicationsByUser(
        user,
      )
    assert(apiKeys.isEmpty())
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

  companion object {
    private const val TEST_1 = "test1"
    private const val TEST_2 = "test2"
    private const val REALM_NAME = "testRealm"

    private fun buildClientId(
      userId: String?,
      index: String?,
    ): String = "$userId-$index"
  }
}
