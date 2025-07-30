/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.keycloak.setup

import io.airbyte.commons.auth.config.InitialUserConfig
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Singleton
import org.keycloak.admin.client.resource.RealmResource
import org.keycloak.representations.idm.CredentialRepresentation
import org.keycloak.representations.idm.UserRepresentation

/**
 * This class is responsible for user creation. It includes methods to create user credentials.
 */
@Singleton
class UserConfigurator(
  private val initialUserConfig: InitialUserConfig,
) {
  fun configureUser(keycloakRealm: RealmResource) {
    val userConfig = userRepresentationFromConfig

    val existingUser =
      keycloakRealm
        .users()
        .searchByEmail(userConfig.email, true)
        .stream()
        .findFirst()

    if (existingUser.isPresent) {
      userConfig.id = existingUser.get().id
      keycloakRealm.users()[existingUser.get().id].update(userConfig)
    } else {
      keycloakRealm.users().create(userConfig).use { response ->
        if (response.status == HTTP_STATUS_CREATED) {
          log.info(userConfig.username + " user created successfully. Status: " + response.statusInfo)
        } else {
          val errorMessage =
            String.format(
              "Failed to create %s user.\nReason: %s\nResponse: %s",
              userConfig.username,
              response.statusInfo.reasonPhrase,
              response.readEntity(String::class.java),
            )
          log.error(errorMessage)
          throw RuntimeException(errorMessage)
        }
      }
    }
  }

  val userRepresentationFromConfig: UserRepresentation
    get() {
      val user = UserRepresentation()
      user.username = initialUserConfig.email
      user.isEnabled = true
      user.email = initialUserConfig.email
      user.firstName = initialUserConfig.firstName
      user.lastName = initialUserConfig.lastName
      user.credentials = listOf(createCredentialRepresentation())
      return user
    }

  fun createCredentialRepresentation(): CredentialRepresentation {
    val password = CredentialRepresentation()
    password.isTemporary = false
    password.type = CredentialRepresentation.PASSWORD
    password.value = initialUserConfig.password
    return password
  }

  companion object {
    private val log = KotlinLogging.logger {}

    const val HTTP_STATUS_CREATED: Int = 201
  }
}
