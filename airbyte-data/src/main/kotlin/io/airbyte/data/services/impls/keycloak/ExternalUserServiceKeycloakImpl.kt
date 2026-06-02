/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.keycloak

import io.airbyte.data.services.ExternalUserService
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Singleton
import jakarta.ws.rs.NotFoundException
import jakarta.ws.rs.core.Response
import org.keycloak.admin.client.Keycloak
import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.util.Locale

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

  override fun findUsersByEmailInRealm(
    email: String,
    realm: String,
  ): List<ExternalUserService.ExternalUser> =
    try {
      keycloakAdminClient
        .realm(realm)
        .users()
        .search(email)
        .filter { user ->
          user.email?.equals(email, ignoreCase = true) == true ||
            user.username?.equals(email, ignoreCase = true) == true
        }.map { user ->
          ExternalUserService.ExternalUser(
            authUserId = user.id,
            email = user.email,
            username = user.username,
            enabled = user.isEnabled,
          )
        }
    } catch (_: NotFoundException) {
      emptyList()
    }

  override fun deleteUsersByEmailInRealm(
    email: String,
    realm: String,
  ): Int = deleteUserByEmailInRealm(email, realm)

  override fun deleteUserByEmailOnOtherRealms(
    email: String,
    realmToKeep: String,
  ) {
    keycloakAdminClient.realms().findAll().forEach { realm ->
      if (realm.realm != realmToKeep) {
        deleteUserByEmailInRealm(email, realm.realm)
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
  ): Int {
    val users =
      try {
        findUsersByEmailInRealm(email, realm)
      } catch (e: NotFoundException) {
        logger.info { "User with emailHash ${emailLogRef(email)} not found in realm $realm" }
        return 0
      }

    var deleted = 0
    val failures = mutableListOf<String>()
    users.forEach { user ->
      runCatching {
        keycloakAdminClient.realm(realm).users().delete(user.authUserId).use { response ->
          if (response.statusInfo.family != Response.Status.Family.SUCCESSFUL) {
            throw IllegalStateException("Keycloak delete returned status ${response.status}")
          }
        }
      }.onSuccess {
        deleted++
        logger.info { "Successfully deleted user with ID ${user.authUserId} in realm $realm" }
      }.onFailure {
        if (it is NotFoundException) {
          logger.info { "User ${user.authUserId} with emailHash ${emailLogRef(email)} was already absent in realm $realm" }
        } else {
          logger.error(it) { "Failed to delete user ${user.authUserId} with emailHash ${emailLogRef(email)} in realm $realm" }
          failures.add("${user.authUserId}: ${it.message ?: it::class.simpleName}")
        }
      }
    }

    if (deleted == 0 && failures.isEmpty()) {
      logger.info { "User with emailHash ${emailLogRef(email)} not found in realm $realm" }
    }
    if (failures.isNotEmpty()) {
      throw IllegalStateException("Failed to delete ${failures.size} Keycloak user(s) in realm $realm: ${failures.joinToString(", ")}")
    }
    return deleted
  }

  private fun emailLogRef(email: String): String {
    val digest =
      MessageDigest
        .getInstance("SHA-256")
        .digest(email.trim().lowercase(Locale.US).toByteArray(StandardCharsets.UTF_8))
    return digest.joinToString("") { "%02x".format(it.toInt() and 0xff) }
  }
}
