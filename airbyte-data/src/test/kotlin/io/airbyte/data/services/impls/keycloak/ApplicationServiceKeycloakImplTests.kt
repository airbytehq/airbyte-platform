/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.keycloak

import io.airbyte.commons.auth.keycloak.ClientScopeConfigurator
import io.airbyte.config.Application
import io.airbyte.config.AuthenticatedUser
import io.airbyte.data.services.ScimAuthUserOwnershipService
import io.airbyte.micronaut.runtime.AirbyteAuthConfig
import io.airbyte.micronaut.runtime.AirbyteKeycloakConfig
import io.mockk.every
import io.mockk.mockk
import io.mockk.mockkStatic
import io.mockk.spyk
import io.mockk.unmockkStatic
import io.mockk.verify
import jakarta.ws.rs.BadRequestException
import jakarta.ws.rs.NotAuthorizedException
import jakarta.ws.rs.client.Client
import jakarta.ws.rs.client.ClientBuilder
import jakarta.ws.rs.core.Response
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import org.keycloak.admin.client.ClientBuilderWrapper
import org.keycloak.admin.client.Keycloak
import org.keycloak.admin.client.KeycloakBuilder
import org.keycloak.admin.client.resource.ClientResource
import org.keycloak.admin.client.resource.ClientsResource
import org.keycloak.admin.client.resource.RealmResource
import org.keycloak.admin.client.resource.UserResource
import org.keycloak.admin.client.resource.UsersResource
import org.keycloak.representations.idm.ClientRepresentation
import org.keycloak.representations.idm.UserRepresentation
import java.net.URI
import java.util.UUID
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

internal class ApplicationServiceKeycloakImplTests {
  private var keycloakConfiguration: AirbyteKeycloakConfig? = null

  private val keycloakClient: Keycloak = mockk<Keycloak>()
  private val realmResource: RealmResource = mockk<RealmResource>()
  private val clientsResource: ClientsResource = mockk<ClientsResource>()
  private val usersResource: UsersResource = mockk<UsersResource>()
  private val clientScopeConfigurator: ClientScopeConfigurator = mockk<ClientScopeConfigurator>()
  private val authUserOwnershipService: ScimAuthUserOwnershipService = mockk()

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
    every {
      authUserOwnershipService.withUniqueOwner<Any?>(any(), any(), any())
    } answers {
      thirdArg<() -> Any?>().invoke()
    }
    every { authUserOwnershipService.uniqueOwner(any()) } returns USER_ID

    apiKeyServiceKeycloakImpl =
      spyk(
        ApplicationServiceKeycloakImpl(
          keycloakClient,
          keycloakConfiguration!!,
          clientScopeConfigurator,
          AirbyteAuthConfig(),
          authUserOwnershipService,
        ),
        recordPrivateCalls = true,
      )
  }

  @Test
  fun testNoMoreThanTwoApiKeys() {
    val user =
      AuthenticatedUser()
        .withUserId(UUID.fromString("6287ecb9-f9fb-4062-a12b-20479b6d2dde"))
        .withAuthUserId("max-credentials-auth-user")

    every {
      apiKeyServiceKeycloakImpl!!["listApplicationsByAuthUserId"](user.authUserId)
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
    val user =
      AuthenticatedUser()
        .withUserId(UUID.fromString("4bb2a760-a0b6-4936-aea0-a13fada349f4"))
        .withAuthUserId("duplicate-name-auth-user")

    every {
      apiKeyServiceKeycloakImpl!!["listApplicationsByAuthUserId"](user.authUserId)
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
    val user =
      AuthenticatedUser()
        .withUserId(UUID.fromString("b3600891-e7c7-4278-8a94-8b838985de2a"))
        .withAuthUserId("bad-create-auth-user")
    every {
      clientsResource.create(any(ClientRepresentation::class))
    } returns Response.status(500).build()

    every {
      apiKeyServiceKeycloakImpl!!["listApplicationsByAuthUserId"](user.authUserId)
    } returns emptyList<Application>()

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
  fun `create serializes complete Keycloak provisioning against authentication ownership changes`() {
    val user =
      AuthenticatedUser()
        .withUserId(USER_ID)
        .withAuthUserId(AUTH_USER_ID)
    val clientResource = mockk<ClientResource>()
    val userResource = mockk<UserResource>()
    val serviceAccountUser =
      UserRepresentation().apply {
        id = SERVICE_ACCOUNT_ID
      }
    val identityMonitor = Any()
    val createReachedKeycloak = CountDownLatch(1)
    val allowKeycloakCreate = CountDownLatch(1)
    val ownershipChangeStarted = CountDownLatch(1)
    val ownershipChangeCompleted = CountDownLatch(1)

    every {
      authUserOwnershipService.withUniqueOwner<Any>(AUTH_USER_ID, USER_ID, any())
    } answers {
      synchronized(identityMonitor) {
        thirdArg<() -> Any>().invoke()
      }
    }
    every { usersResource.searchByAttributes("user_id:$AUTH_USER_ID") } returns emptyList()
    every { clientsResource.create(any(ClientRepresentation::class)) } answers {
      createReachedKeycloak.countDown()
      check(allowKeycloakCreate.await(10, TimeUnit.SECONDS))
      Response.created(URI.create("https://company.example")).build()
    }
    every { clientsResource.findByClientId(any()) } answers {
      listOf(
        ClientRepresentation().apply {
          id = CLIENT_INTERNAL_ID
          clientId = firstArg()
          name = TEST_1
          secret = "secret"
          attributes = mapOf("client.secret.creation.time" to "365")
        },
      )
    }
    every { clientsResource[CLIENT_INTERNAL_ID] } returns clientResource
    every { clientResource.serviceAccountUser } returns serviceAccountUser
    every { usersResource[SERVICE_ACCOUNT_ID] } returns userResource
    every { userResource.update(any()) } returns Unit

    val executor = Executors.newFixedThreadPool(2)
    try {
      val create = executor.submit<Application> { apiKeyServiceKeycloakImpl!!.createApplication(user, TEST_1) }
      Assertions.assertTrue(createReachedKeycloak.await(10, TimeUnit.SECONDS))
      val ownershipChange =
        executor.submit {
          ownershipChangeStarted.countDown()
          synchronized(identityMonitor) {
            ownershipChangeCompleted.countDown()
          }
        }
      Assertions.assertTrue(ownershipChangeStarted.await(10, TimeUnit.SECONDS))
      Assertions.assertFalse(
        ownershipChangeCompleted.await(250, TimeUnit.MILLISECONDS),
        "Authentication ownership must not change while Keycloak client provisioning is incomplete.",
      )

      allowKeycloakCreate.countDown()
      create.get(10, TimeUnit.SECONDS)
      ownershipChange.get(10, TimeUnit.SECONDS)
      Assertions.assertTrue(ownershipChangeCompleted.await(10, TimeUnit.SECONDS))
    } finally {
      allowKeycloakCreate.countDown()
      executor.shutdownNow()
    }
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

  @Test
  fun testGetTokenThrowsInvalidClientCredentialsOnKeycloak401() {
    val builder = mockk<KeycloakBuilder>()
    val userKeycloakClient = mockk<Keycloak>(relaxed = true)

    every { builder.serverUrl(any()) } returns builder
    every { builder.realm(any()) } returns builder
    every { builder.grantType(any()) } returns builder
    every { builder.clientId(any()) } returns builder
    every { builder.clientSecret(any()) } returns builder
    every { builder.resteasyClient(any()) } returns builder
    every { builder.build() } returns userKeycloakClient
    every { userKeycloakClient.tokenManager().accessTokenString } throws
      NotAuthorizedException(Response.status(Response.Status.UNAUTHORIZED).build())

    mockkStatic(KeycloakBuilder::class)
    every { KeycloakBuilder.builder() } returns builder
    every {
      apiKeyServiceKeycloakImpl!!["authUserIdForClient"]("bad-client-id")
    } returns "auth-user-id"
    every { authUserOwnershipService.uniqueOwner("auth-user-id") } returns USER_ID

    try {
      Assertions.assertThrows(
        InvalidClientCredentialsException::class.java,
      ) { apiKeyServiceKeycloakImpl!!.getToken("bad-client-id", "bad-client-secret") }
    } finally {
      unmockkStatic(KeycloakBuilder::class)
    }
  }

  @ParameterizedTest
  @ValueSource(strings = ["", " ", "  \t"])
  fun `token rejects blank client id before calling Keycloak`(clientId: String) {
    Assertions.assertThrows(InvalidClientCredentialsException::class.java) {
      apiKeyServiceKeycloakImpl!!.getToken(clientId, "secret")
    }

    verify(exactly = 0) { keycloakClient.realm(any()) }
  }

  @Test
  fun `auth user lookup uses exact bounded client search`() {
    val builder = mockk<KeycloakBuilder>()
    val userKeycloakClient = mockk<Keycloak>(relaxed = true)
    val clientResource = mockk<ClientResource>()
    every { builder.serverUrl(any()) } returns builder
    every { builder.realm(any()) } returns builder
    every { builder.grantType(any()) } returns builder
    every { builder.clientId(any()) } returns builder
    every { builder.clientSecret(any()) } returns builder
    every { builder.resteasyClient(any()) } returns builder
    every { builder.build() } returns userKeycloakClient
    every { userKeycloakClient.tokenManager().accessTokenString } returns "token"
    every {
      clientsResource.findAll("client-id", false, false, 0, 2)
    } returns
      listOf(
        ClientRepresentation().apply {
          id = CLIENT_INTERNAL_ID
          clientId = "client-id"
        },
      )
    every { clientsResource[CLIENT_INTERNAL_ID] } returns clientResource
    every { clientResource.serviceAccountUser } returns
      UserRepresentation().apply {
        attributes = mapOf("user_id" to listOf(AUTH_USER_ID))
      }

    mockkStatic(KeycloakBuilder::class)
    every { KeycloakBuilder.builder() } returns builder

    try {
      Assertions.assertEquals("token", apiKeyServiceKeycloakImpl!!.getToken("client-id", "secret"))
      verify(exactly = 1) {
        clientsResource.findAll("client-id", false, false, 0, 2)
      }
      verify(exactly = 0) { clientsResource.findByClientId(any()) }
    } finally {
      unmockkStatic(KeycloakBuilder::class)
    }
  }

  @Test
  fun `auth user lookup rejects missing exact client`() {
    every {
      clientsResource.findAll("missing-client", false, false, 0, 2)
    } returns emptyList()

    Assertions.assertThrows(InvalidClientCredentialsException::class.java) {
      apiKeyServiceKeycloakImpl!!.getToken("missing-client", "secret")
    }
  }

  @Test
  fun `auth user lookup rejects ambiguous exact clients`() {
    every {
      clientsResource.findAll("ambiguous-client", false, false, 0, 2)
    } returns
      listOf(
        ClientRepresentation().apply {
          id = "first-client"
          clientId = "ambiguous-client"
        },
        ClientRepresentation().apply {
          id = "second-client"
          clientId = "ambiguous-client"
        },
      )

    Assertions.assertThrows(InvalidClientCredentialsException::class.java) {
      apiKeyServiceKeycloakImpl!!.getToken("ambiguous-client", "secret")
    }
  }

  @Test
  fun `token minting runs after the ownership transaction callback has ended`() {
    val builder = mockk<KeycloakBuilder>()
    val userKeycloakClient = mockk<Keycloak>(relaxed = true)
    var ownershipCallbackActive = false

    every {
      apiKeyServiceKeycloakImpl!!["authUserIdForClient"]("client-id")
    } returns AUTH_USER_ID
    every {
      authUserOwnershipService.withUniqueOwner<String>(AUTH_USER_ID, null, any())
    } answers {
      ownershipCallbackActive = true
      try {
        thirdArg<() -> String>().invoke()
      } finally {
        ownershipCallbackActive = false
      }
    }
    every { builder.serverUrl(any()) } returns builder
    every { builder.realm(any()) } returns builder
    every { builder.grantType(any()) } returns builder
    every { builder.clientId(any()) } returns builder
    every { builder.clientSecret(any()) } returns builder
    every { builder.resteasyClient(any()) } returns builder
    every { builder.build() } returns userKeycloakClient
    every { userKeycloakClient.tokenManager().accessTokenString } returns "token"

    mockkStatic(KeycloakBuilder::class)
    every { KeycloakBuilder.builder() } answers {
      Assertions.assertFalse(
        ownershipCallbackActive,
        "Keycloak token minting must not run inside the ownership transaction callback.",
      )
      builder
    }

    try {
      Assertions.assertEquals("token", apiKeyServiceKeycloakImpl!!.getToken("client-id", "secret"))
      Assertions.assertFalse(ownershipCallbackActive)
    } finally {
      unmockkStatic(KeycloakBuilder::class)
    }
  }

  @Test
  fun `token is discarded when unique owner changes during minting`() {
    val builder = mockk<KeycloakBuilder>()
    val userKeycloakClient = mockk<Keycloak>(relaxed = true)
    val changedOwner = UUID.randomUUID()

    every {
      apiKeyServiceKeycloakImpl!!["authUserIdForClient"]("client-id")
    } returns AUTH_USER_ID
    every { authUserOwnershipService.uniqueOwner(AUTH_USER_ID) } returnsMany
      listOf(USER_ID, changedOwner)
    every { builder.serverUrl(any()) } returns builder
    every { builder.realm(any()) } returns builder
    every { builder.grantType(any()) } returns builder
    every { builder.clientId(any()) } returns builder
    every { builder.clientSecret(any()) } returns builder
    every { builder.resteasyClient(any()) } returns builder
    every { builder.build() } returns userKeycloakClient
    every { userKeycloakClient.tokenManager().accessTokenString } returns "stale-token"

    mockkStatic(KeycloakBuilder::class)
    every { KeycloakBuilder.builder() } returns builder

    try {
      val exception =
        Assertions.assertThrows(InvalidClientCredentialsException::class.java) {
          apiKeyServiceKeycloakImpl!!.getToken("client-id", "secret")
        }
      Assertions.assertEquals("Invalid client_id or client_secret", exception.message)
      verify(exactly = 2) { authUserOwnershipService.uniqueOwner(AUTH_USER_ID) }
    } finally {
      unmockkStatic(KeycloakBuilder::class)
    }
  }

  @Test
  fun `token is discarded when ownership disappears during minting`() {
    val builder = mockk<KeycloakBuilder>()
    val userKeycloakClient = mockk<Keycloak>(relaxed = true)
    var snapshotCount = 0

    every {
      apiKeyServiceKeycloakImpl!!["authUserIdForClient"]("client-id")
    } returns AUTH_USER_ID
    every { authUserOwnershipService.uniqueOwner(AUTH_USER_ID) } answers {
      if (++snapshotCount == 1) {
        USER_ID
      } else {
        throw IllegalStateException("Authentication identity $AUTH_USER_ID has no unique owner")
      }
    }
    every { builder.serverUrl(any()) } returns builder
    every { builder.realm(any()) } returns builder
    every { builder.grantType(any()) } returns builder
    every { builder.clientId(any()) } returns builder
    every { builder.clientSecret(any()) } returns builder
    every { builder.resteasyClient(any()) } returns builder
    every { builder.build() } returns userKeycloakClient

    mockkStatic(KeycloakBuilder::class)
    every { KeycloakBuilder.builder() } returns builder

    try {
      val exception =
        Assertions.assertThrows(InvalidClientCredentialsException::class.java) {
          apiKeyServiceKeycloakImpl!!.getToken("client-id", "secret")
        }
      Assertions.assertEquals("Invalid client_id or client_secret", exception.message)
      verify(exactly = 2) { authUserOwnershipService.uniqueOwner(AUTH_USER_ID) }
    } finally {
      unmockkStatic(KeycloakBuilder::class)
    }
  }

  @Test
  fun `token client uses configured connect and read timeouts`() {
    val httpClientBuilder = mockk<ClientBuilder>()
    val httpClient = mockk<Client>()
    val keycloakBuilder = mockk<KeycloakBuilder>()
    val userKeycloakClient = mockk<Keycloak>(relaxed = true)

    mockkStatic(ClientBuilderWrapper::class)
    mockkStatic(KeycloakBuilder::class)
    every {
      apiKeyServiceKeycloakImpl!!["authUserIdForClient"]("client-id")
    } returns AUTH_USER_ID
    every { ClientBuilderWrapper.create(null, false) } returns httpClientBuilder
    every { httpClientBuilder.register(any<Class<*>>(), any<Int>()) } returns httpClientBuilder
    every { httpClientBuilder.connectTimeout(any(), any()) } returns httpClientBuilder
    every { httpClientBuilder.readTimeout(any(), any()) } returns httpClientBuilder
    every { httpClientBuilder.build() } returns httpClient
    every { keycloakBuilder.serverUrl(any()) } returns keycloakBuilder
    every { keycloakBuilder.realm(any()) } returns keycloakBuilder
    every { keycloakBuilder.grantType(any()) } returns keycloakBuilder
    every { keycloakBuilder.clientId(any()) } returns keycloakBuilder
    every { keycloakBuilder.clientSecret(any()) } returns keycloakBuilder
    every { keycloakBuilder.resteasyClient(any()) } returns keycloakBuilder
    every { keycloakBuilder.build() } returns userKeycloakClient
    every { userKeycloakClient.tokenManager().accessTokenString } returns "token"
    every { KeycloakBuilder.builder() } returns keycloakBuilder

    try {
      Assertions.assertEquals("token", apiKeyServiceKeycloakImpl!!.getToken("client-id", "secret"))
      verify(exactly = 1) { httpClientBuilder.connectTimeout(5_000, TimeUnit.MILLISECONDS) }
      verify(exactly = 1) { httpClientBuilder.readTimeout(5_000, TimeUnit.MILLISECONDS) }
      verify(exactly = 1) { keycloakBuilder.resteasyClient(httpClient) }
    } finally {
      unmockkStatic(KeycloakBuilder::class)
      unmockkStatic(ClientBuilderWrapper::class)
    }
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
    private const val AUTH_USER_ID = "keycloak-create-auth-user"
    private const val CLIENT_INTERNAL_ID = "keycloak-client-id"
    private const val SERVICE_ACCOUNT_ID = "service-account-id"
    private val USER_ID = UUID.fromString("624b9a6a-bc22-4734-9f2b-40dbf366adf7")

    private fun buildClientId(userId: String?): String = "$userId-0"
  }
}
