package io.airbyte.data.services.impls.keycloak

import io.airbyte.data.services.ExternalUserService
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Singleton
import jakarta.ws.rs.NotFoundException
import org.keycloak.admin.client.Keycloak

private val logger = KotlinLogging.logger {}

@Singleton
class ExternalUserServiceKeycloakImpl(
  private val keycloakAdminClient: Keycloak,
) : ExternalUserService {
  override fun deleteUserByExternalId(
    authUserId: String,
    realm: String,
  ) {
    logger.info { "Deleting user with authUserId: $authUserId in Keycloak realm: $realm" }
    keycloakAdminClient.realm(realm).users().delete(authUserId)
  }

  override fun deleteUserByEmailOnOtherRealms(
    email: String,
    realmToKeep: String,
  ) {
    keycloakAdminClient.realms().findAll().forEach {
        realm ->
      run {
        if (realm.id != realmToKeep) {
          deleteUserByEmailInRealm(email, realm.id)
        }
      }
    }
  }

  override fun getRealmByAuthUserId(authUserId: String): String? {
    val realms = keycloakAdminClient.realms().findAll()
    for (realm in realms) {
      try {
        val user = keycloakAdminClient.realm(realm.id).users().get(authUserId)
        if (user != null) {
          return realm.id
        }
      } catch (e: NotFoundException) {
        continue
      }
    }

    return null
  }

  private fun deleteUserByEmailInRealm(
    email: String,
    realmId: String,
  ) {
    try {
      keycloakAdminClient.realm(realmId).users().search(email).forEach { user ->
        keycloakAdminClient.realm(realmId).users().delete(user.id)
        logger.info { "Successfully deleted user with ID ${user.id} in realm $realmId" }
      }
    } catch (e: NotFoundException) {
      logger.info { "User with email $email not found in realm $realmId" }
    }
  }
}
