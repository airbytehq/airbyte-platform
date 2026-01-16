/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.auth.keycloak

import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Singleton
import jakarta.ws.rs.core.Response
import org.keycloak.admin.client.resource.RealmResource
import org.keycloak.representations.idm.ClientScopeRepresentation
import org.keycloak.representations.idm.ProtocolMapperRepresentation

private val logger = KotlinLogging.logger {}

/**
 * This class is responsible for the OpenID Client Scope and Mappers. This so that the API Keys will
 * work with our JWTs.
 */
@Singleton
class ClientScopeConfigurator {
  companion object {
    const val HTTP_STATUS_CREATED: Int = 201
  }

  /**
   * This method creates the client scope and mappers for the Airbyte realm.
   *
   * @param keycloakRealm the realm to create the client scope in
   */
  fun configureClientScope(keycloakRealm: RealmResource) {
    val clientScopeRepresentation = createClientScopeRepresentation()
    val existingClientScope = keycloakRealm.clientScopes().findAll().find { it.name == clientScopeRepresentation.name }

    existingClientScope?.let {
      updateExistingClientScope(it, clientScopeRepresentation, keycloakRealm)
    } ?: createNewClientScope(clientScopeRepresentation, keycloakRealm)
  }

  private fun updateExistingClientScope(
    scope: ClientScopeRepresentation,
    clientScopeRepresentation: ClientScopeRepresentation,
    keycloakRealm: RealmResource,
  ) {
    clientScopeRepresentation.id = scope.id
    keycloakRealm.clientScopes()[scope.id].update(clientScopeRepresentation)
  }

  private fun createNewClientScope(
    clientScopeRepresentation: ClientScopeRepresentation,
    keycloakRealm: RealmResource,
  ) {
    keycloakRealm.clientScopes().create(clientScopeRepresentation).use { response ->
      when (response.status) {
        HTTP_STATUS_CREATED -> logger.info { "ClientScope ${clientScopeRepresentation.name} created successfully." }
        else -> handleCreationError(response)
      }
    }
  }

  private fun handleCreationError(response: Response) {
    val errorMessage = "Failed to create Client Scope.\nReason: ${response.statusInfo.reasonPhrase}\nResponse: ${
      response.readEntity(
        String::class.java,
      )
    }"
    logger.error { errorMessage }
    throw RuntimeException(errorMessage)
  }

  /**
   * This method creates the client scope representation.
   *
   * @return the client scope representation
   */
  private fun createClientScopeRepresentation(): ClientScopeRepresentation {
    val clientScopeRepresentation = ClientScopeRepresentation()
    clientScopeRepresentation.name = "openid"
    clientScopeRepresentation.protocol = "openid-connect"
    clientScopeRepresentation.protocolMappers = listOf(buildUserIdMapper(), buildSubMapper())
    return clientScopeRepresentation
  }

  /**
   * This method creates the user id mapper.
   *
   * @return the user id mapper
   */
  private fun buildUserIdMapper(): ProtocolMapperRepresentation {
    val userIdMapper = ProtocolMapperRepresentation()
    userIdMapper.name = "user_id"
    userIdMapper.protocol = "openid-connect"
    userIdMapper.protocolMapper = "oidc-usermodel-attribute-mapper"
    userIdMapper.config =
      mapOf(
        "user.attribute" to "user_id",
        "claim.name" to "user_id",
        "jsonType.label" to "",
        "id.token.claim" to "true",
        "access.token.claim" to "true",
        "userinfo.token.claim" to "true",
        "multivalued" to "false",
        "aggregate.attrs" to "false",
      )
    return userIdMapper
  }

  /**
   * This method creates the sub mapper.
   *
   * @return the sub mapper
   */
  private fun buildSubMapper(): ProtocolMapperRepresentation {
    val subMapper = ProtocolMapperRepresentation()
    subMapper.name = "sub"
    subMapper.protocol = "openid-connect"
    subMapper.protocolMapper = "oidc-usermodel-attribute-mapper"
    subMapper.config =
      mapOf(
        "user.attribute" to "user_id",
        "claim.name" to "sub",
        "jsonType.label" to "",
        "id.token.claim" to
          "true",
        "access.token.claim" to
          "true",
        "userinfo.token.claim" to
          "true",
        "multivalued" to
          "false",
        "aggregate.attrs" to
          "false",
      )
    return subMapper
  }
}
