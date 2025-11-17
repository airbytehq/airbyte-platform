/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.keycloak.setup

import io.airbyte.commons.auth.config.InitialUserConfig
import io.mockk.every
import io.mockk.impl.annotations.MockK
import io.mockk.junit5.MockKExtension
import io.mockk.slot
import io.mockk.verify
import jakarta.ws.rs.core.Response
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.keycloak.admin.client.resource.RealmResource
import org.keycloak.admin.client.resource.UserResource
import org.keycloak.admin.client.resource.UsersResource
import org.keycloak.representations.idm.CredentialRepresentation
import org.keycloak.representations.idm.UserRepresentation

@ExtendWith(MockKExtension::class)
internal class UserConfiguratorTest {
  private lateinit var userConfigurator: UserConfigurator

  @MockK
  private lateinit var initialUserConfig: InitialUserConfig

  @MockK
  private lateinit var realmResource: RealmResource

  @MockK
  private lateinit var usersResource: UsersResource

  @MockK
  private lateinit var userResource: UserResource

  @MockK
  private lateinit var response: Response

  @BeforeEach
  fun setUp() {
    every { initialUserConfig.email } returns EMAIL
    every { initialUserConfig.firstName } returns FIRST_NAME
    every { initialUserConfig.lastName } returns LAST_NAME
    every { initialUserConfig.password } returns PASSWORD

    every { realmResource.users() } returns usersResource
    every { usersResource.create(any()) } returns response

    every { usersResource.get(KEYCLOAK_USER_ID) } returns userResource
    every { response.statusInfo } returns Response.Status.OK

    userConfigurator = UserConfigurator(initialUserConfig)
  }

  @Test
  fun testConfigureUser() {
    every { response.status } returns 201
    every { response.close() } returns Unit
    every { usersResource.searchByEmail(EMAIL, true) } returns mutableListOf()

    userConfigurator.configureUser(realmResource)

    val userSlot = slot<UserRepresentation>()
    verify { usersResource.create(capture(userSlot)) }

    val userRepresentation = userSlot.captured
    Assertions.assertNull(userRepresentation.id)
    Assertions.assertEquals(EMAIL, userRepresentation.username)
    Assertions.assertEquals(EMAIL, userRepresentation.email)
    Assertions.assertEquals(FIRST_NAME, userRepresentation.firstName)
    Assertions.assertEquals(LAST_NAME, userRepresentation.lastName)
    Assertions.assertTrue(userRepresentation.isEnabled)
    Assertions.assertEquals(1, userRepresentation.credentials.size)
    Assertions.assertEquals(CredentialRepresentation.PASSWORD, userRepresentation.credentials.first().type)
    Assertions.assertEquals(PASSWORD, userRepresentation.credentials.first().value)
    Assertions.assertFalse(userRepresentation.credentials.first().isTemporary)
    Assertions.assertEquals(USER_REPRESENTATION.credentials, userRepresentation.credentials)
  }

  @Test
  fun testConfigureUserAlreadyExists() {
    every { usersResource.searchByEmail(EMAIL, true) } returns mutableListOf(USER_REPRESENTATION)
    every { userResource.update(any()) } returns Unit

    userConfigurator.configureUser(realmResource)

    verify(exactly = 0) { usersResource.create(any()) }

    val userSlot = slot<UserRepresentation>()
    verify { userResource.update(capture(userSlot)) }

    val userRepresentation = userSlot.captured
    Assertions.assertEquals(USER_REPRESENTATION.id, userRepresentation.id)
    Assertions.assertEquals(USER_REPRESENTATION.username, userRepresentation.username)
    Assertions.assertEquals(USER_REPRESENTATION.email, userRepresentation.email)
    Assertions.assertEquals(USER_REPRESENTATION.firstName, userRepresentation.firstName)
    Assertions.assertEquals(USER_REPRESENTATION.lastName, userRepresentation.lastName)
    Assertions.assertEquals(USER_REPRESENTATION.isEnabled, userRepresentation.isEnabled)
    Assertions.assertEquals(USER_REPRESENTATION.credentials, userRepresentation.credentials)
  }

  @Test
  fun testConfigureUserRepresentation() {
    every { initialUserConfig.email } returns EMAIL
    every { initialUserConfig.firstName } returns FIRST_NAME
    every { initialUserConfig.lastName } returns LAST_NAME

    val userRepresentation = userConfigurator.userRepresentationFromConfig

    Assertions.assertEquals(EMAIL, userRepresentation.username)
    Assertions.assertEquals(EMAIL, userRepresentation.email)
    Assertions.assertEquals(FIRST_NAME, userRepresentation.firstName)
    Assertions.assertEquals(LAST_NAME, userRepresentation.lastName)
  }

  @Test
  fun testCreateCredentialRepresentation() {
    every { initialUserConfig.password } returns PASSWORD

    val credentialRepresentation = userConfigurator.createCredentialRepresentation()

    Assertions.assertFalse(credentialRepresentation.isTemporary)
    Assertions.assertEquals(CredentialRepresentation.PASSWORD, credentialRepresentation.type)
    Assertions.assertEquals(PASSWORD, credentialRepresentation.value)
  }

  companion object {
    private const val EMAIL = "jon@airbyte.io"
    private const val FIRST_NAME = "Jon"
    private const val LAST_NAME = "Smith"
    private const val PASSWORD = "airbytePassword"
    private const val KEYCLOAK_USER_ID = "some-id"

    private val USER_REPRESENTATION = UserRepresentation()

    init {
      USER_REPRESENTATION.id = KEYCLOAK_USER_ID
      USER_REPRESENTATION.username = EMAIL
      USER_REPRESENTATION.email = EMAIL
      USER_REPRESENTATION.firstName = FIRST_NAME
      USER_REPRESENTATION.lastName = LAST_NAME
      USER_REPRESENTATION.isEnabled = true

      val credentialRepresentation = CredentialRepresentation()
      credentialRepresentation.type = CredentialRepresentation.PASSWORD
      credentialRepresentation.value = PASSWORD
      credentialRepresentation.isTemporary = false

      USER_REPRESENTATION.credentials = mutableListOf(credentialRepresentation)
    }
  }
}
