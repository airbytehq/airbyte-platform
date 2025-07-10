/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

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
    keycloakAdminClient.realms().findAll().forEach { realm ->
      run {
        if (realm.realm != realmToKeep) {
          deleteUserByEmailInRealm(email, realm.realm)
        }
      }
    }
  }

  override fun getRealmByAuthUserId(authUserId: String): String? {
    val realms = keycloakAdminClient.realms().findAll()
    for (realm in realms) {
      try {
        val user =
          keycloakAdminClient
            .realm(realm.realm)
            .users()
            .get(authUserId)
            .toRepresentation()
        if (user != null) {
          logger.info { "Auth user found in realm ${realm.realm} (id: ${user.id})" }
          return realm.realm
        }
      } catch (_: NotFoundException) {
        continue
      }
    }

    return null
  }

  private fun deleteUserByEmailInRealm(
    email: String,
    realm: String,
  ) {
    try {
      var didDelete = false
      keycloakAdminClient.realm(realm).users().search(email).forEach { user ->
        keycloakAdminClient.realm(realm).users().delete(user.id)
        didDelete = true
        logger.info { "Successfully deleted user with ID ${user.id} in realm $realm" }
      }
      if (!didDelete) {
        logger.info { "User with email $email not found in realm $realm" }
      }
    } catch (e: NotFoundException) {
      logger.info { "User with email $email not found in realm $realm" }
    }
  }
}
