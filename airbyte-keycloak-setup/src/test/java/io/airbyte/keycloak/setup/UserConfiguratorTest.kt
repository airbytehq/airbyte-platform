/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.keycloak.setup

import io.airbyte.commons.auth.config.InitialUserConfig
import jakarta.ws.rs.core.Response
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.keycloak.admin.client.resource.RealmResource
import org.keycloak.admin.client.resource.UserResource
import org.keycloak.admin.client.resource.UsersResource
import org.keycloak.representations.idm.CredentialRepresentation
import org.keycloak.representations.idm.UserRepresentation
import org.mockito.ArgumentMatcher
import org.mockito.ArgumentMatchers
import org.mockito.Mock
import org.mockito.Mockito
import org.mockito.MockitoAnnotations

internal class UserConfiguratorTest {
  private lateinit var userConfigurator: UserConfigurator

  @Mock
  private lateinit var initialUserConfig: InitialUserConfig

  @Mock
  private lateinit var realmResource: RealmResource

  @Mock
  private lateinit var usersResource: UsersResource

  @Mock
  private lateinit var userResource: UserResource

  @Mock
  private lateinit var response: Response

  @BeforeEach
  fun setUp() {
    MockitoAnnotations.openMocks(this)

    Mockito.`when`(initialUserConfig.email).thenReturn(EMAIL)
    Mockito.`when`(initialUserConfig.firstName).thenReturn(FIRST_NAME)
    Mockito.`when`(initialUserConfig.lastName).thenReturn(LAST_NAME)
    Mockito.`when`(initialUserConfig.password).thenReturn(PASSWORD)

    Mockito.`when`(realmResource.users()).thenReturn(usersResource)
    Mockito
      .`when`(usersResource.create(ArgumentMatchers.any(UserRepresentation::class.java)))
      .thenReturn(response)

    Mockito.`when`(usersResource.get(KEYCLOAK_USER_ID)).thenReturn(userResource)
    Mockito.`when`(response.statusInfo).thenReturn(Response.Status.OK)

    userConfigurator = UserConfigurator(initialUserConfig)
  }

  @Test
  fun testConfigureUser() {
    Mockito.`when`(response.status).thenReturn(201)

    userConfigurator.configureUser(realmResource)

    Mockito
      .verify(usersResource)
      .create(
        ArgumentMatchers.argThat(
          ArgumentMatcher { userRepresentation: UserRepresentation ->
            userRepresentation.id == null &&
              userRepresentation.username == EMAIL &&
              userRepresentation.email == EMAIL &&
              userRepresentation.firstName == FIRST_NAME &&
              userRepresentation.lastName == LAST_NAME &&
              userRepresentation.isEnabled &&
              userRepresentation.credentials.size == 1 &&
              userRepresentation.credentials
                .first()
                .type == CredentialRepresentation.PASSWORD &&
              userRepresentation.credentials.first().value == PASSWORD &&
              !userRepresentation.credentials
                .first()
                .isTemporary &&
              userRepresentation.credentials == USER_REPRESENTATION.credentials
          },
        ),
      )
  }

  @Test
  fun testConfigureUserAlreadyExists() {
    Mockito.`when`(usersResource.searchByEmail(EMAIL, true)).thenReturn(
      mutableListOf(USER_REPRESENTATION),
    )

    userConfigurator.configureUser(realmResource)

    Mockito.verify(usersResource, Mockito.never()).create(ArgumentMatchers.any())
    Mockito
      .verify(userResource)
      .update(
        ArgumentMatchers.argThat(
          ArgumentMatcher { userRepresentation: UserRepresentation ->
            userRepresentation.id == USER_REPRESENTATION.id &&
              userRepresentation.username == USER_REPRESENTATION.username &&
              userRepresentation.email == USER_REPRESENTATION.email &&
              userRepresentation.firstName == USER_REPRESENTATION.firstName &&
              userRepresentation.lastName == USER_REPRESENTATION.lastName &&
              userRepresentation.isEnabled == USER_REPRESENTATION.isEnabled &&
              userRepresentation.credentials == USER_REPRESENTATION.credentials
          },
        ),
      )
  }

  @Test
  fun testConfigureUserRepresentation() {
    Mockito.`when`(initialUserConfig.email).thenReturn(EMAIL)
    Mockito.`when`(initialUserConfig.firstName).thenReturn(FIRST_NAME)
    Mockito.`when`(initialUserConfig.lastName).thenReturn(LAST_NAME)

    val userRepresentation = userConfigurator.userRepresentationFromConfig

    Assertions.assertEquals(EMAIL, userRepresentation.username) // we want to set the username to the configured email
    Assertions.assertEquals(EMAIL, userRepresentation.email)
    Assertions.assertEquals(FIRST_NAME, userRepresentation.firstName)
    Assertions.assertEquals(LAST_NAME, userRepresentation.lastName)
  }

  @Test
  fun testCreateCredentialRepresentation() {
    Mockito.`when`(initialUserConfig.password).thenReturn(PASSWORD)

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

    // set up a static Keycloak UserRepresentation based on the constants above
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
