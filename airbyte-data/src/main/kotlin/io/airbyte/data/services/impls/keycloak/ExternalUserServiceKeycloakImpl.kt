package io.airbyte.data.services.impls.keycloak

import io.airbyte.data.services.ExternalUserService
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Singleton
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
}
