package io.airbyte.data.services.impls.keycloak

import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Test
import org.keycloak.admin.client.Keycloak
import org.keycloak.admin.client.resource.RealmResource
import org.keycloak.admin.client.resource.UsersResource

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
}
