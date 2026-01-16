/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.keycloak

import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import jakarta.ws.rs.NotFoundException
import org.junit.jupiter.api.Test
import org.keycloak.admin.client.Keycloak
import org.keycloak.admin.client.resource.RealmResource
import org.keycloak.admin.client.resource.RealmsResource
import org.keycloak.admin.client.resource.UserResource
import org.keycloak.admin.client.resource.UsersResource
import org.keycloak.representations.idm.RealmRepresentation
import org.keycloak.representations.idm.UserRepresentation

class ExternalUserServiceKeycloakImplTest {
  private var keycloakAdminClient: Keycloak = mockk()
  private var realmResource: RealmResource = mockk()
  private var usersResource: UsersResource = mockk()
  private var service: ExternalUserServiceKeycloakImpl = ExternalUserServiceKeycloakImpl(keycloakAdminClient)

  @Test
  fun `test deleteUserByExternalId`() {
    val authUserId = "authUserId"
    val realm = "keycloak-realm"

    every { keycloakAdminClient.realm(realm) } returns realmResource
    every { realmResource.users() } returns usersResource
    every { usersResource.delete(authUserId) } returns mockk()

    service.deleteUserByExternalId(authUserId, realm)

    verify(exactly = 1) { usersResource.delete(authUserId) }
  }

  @Test
  fun `test deleteUserByEmailOnOtherRealms`() {
    val email = "email@airbyte.io"

    // set up mocks
    val realm1 = RealmRepresentation().apply { realm = "realm1" }
    val realm2 = RealmRepresentation().apply { realm = "realm2" }
    val realmToKeep = RealmRepresentation().apply { realm = "realm3" }
    val userToDelete = UserRepresentation().apply { id = "userToDelete" }

    val usersResource1: UsersResource = mockk()
    val usersResource2: UsersResource = mockk()
    val usersResource3: UsersResource = mockk()

    every { keycloakAdminClient.realms() } returns
      mockk<RealmsResource> {
        every { findAll() } returns listOf(realm1, realm2, realmToKeep)
      }

    val realm1Resource = mockk<RealmResource>()
    val realm2Resource = mockk<RealmResource>()
    val realm3Resource = mockk<RealmResource>()

    every { keycloakAdminClient.realm(realm1.realm) } returns realm1Resource
    every { keycloakAdminClient.realm(realm2.realm) } returns realm2Resource
    every { keycloakAdminClient.realm(realmToKeep.realm) } returns realm3Resource

    every { realm1Resource.users() } returns usersResource1
    every { realm2Resource.users() } returns usersResource2
    every { realm3Resource.users() } returns usersResource3

    every { usersResource1.search(email) } returns listOf(userToDelete)
    every { usersResource2.search(email) } throws NotFoundException() // keycloak throws NotFoundException if no user has the email

    every { usersResource1.delete(any()) } returns mockk()

    // call service method
    service.deleteUserByEmailOnOtherRealms(email, realmToKeep.realm)

    // should not search on realm to keep (realm3)
    verify(exactly = 0) { usersResource3.search(any()) }

    // should only delete on realm1
    verify(exactly = 1) { usersResource1.delete(userToDelete.id) }
    verify(exactly = 0) { usersResource2.delete(any()) }
    verify(exactly = 0) { usersResource3.delete(any()) }
  }

  @Test
  fun `test getRealmByAuthUserId`() {
    val authUserId = "authUserId"

    // set up mocks
    val realm1 = RealmRepresentation().apply { realm = "realm1" }
    val realm2 = RealmRepresentation().apply { realm = "realm2" }

    val usersResource1: UsersResource = mockk()
    val usersResource2: UsersResource = mockk()

    every { keycloakAdminClient.realms() } returns
      mockk<RealmsResource> {
        every { findAll() } returns listOf(realm1, realm2)
      }

    val realm1Resource = mockk<RealmResource>()
    val realm2Resource = mockk<RealmResource>()

    every { keycloakAdminClient.realm(realm1.realm) } returns realm1Resource
    every { keycloakAdminClient.realm(realm2.realm) } returns realm2Resource

    every { realm1Resource.users() } returns usersResource1
    every { realm2Resource.users() } returns usersResource2

    val userResource1 = mockk<UserResource>()
    val userResource2 = mockk<UserResource>()

    every { usersResource1.get(authUserId) } returns userResource1
    every { usersResource2.get(authUserId) } returns userResource2

    every { userResource1.toRepresentation() } throws NotFoundException() // keycloak throws NotFoundException if no user has the id
    every { userResource2.toRepresentation() } returns mockk<UserRepresentation>()

    // call service method
    val realm = service.getRealmByAuthUserId(authUserId)

    // should only search in realm1
    verify(exactly = 1) { usersResource1.get(authUserId) }
    verify(exactly = 1) { usersResource2.get(authUserId) }

    // should return realm21
    assert(realm == realm2.realm)
  }
}
