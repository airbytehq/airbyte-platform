/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.keycloak.setup;

import jakarta.inject.Singleton;
import jakarta.ws.rs.core.Response;
import java.util.Arrays;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.keycloak.admin.client.resource.RealmResource;
import org.keycloak.representations.idm.ClientScopeRepresentation;
import org.keycloak.representations.idm.ProtocolMapperRepresentation;

/**
 * This class is responsible for the OpenID Client Scope and Mappers. This so that the API Keys will
 * work with our JWTs.
 */
@Singleton
@Slf4j
public class ClientScopeCreator {

  public static final int HTTP_STATUS_CREATED = 201;

  /**
   * This method creates the client scope and mappers for the Airbyte realm.
   *
   * @param keycloakRealm the realm to create the client scope in
   */
  public void createClientScope(final RealmResource keycloakRealm) {
    final ClientScopeRepresentation clientScopeRepresentation = createClientScopeRepresentation();
    final Response response = keycloakRealm.clientScopes().create(clientScopeRepresentation);

    if (response.getStatus() == HTTP_STATUS_CREATED) {
      log.info("ClientScope {} created successfully.", clientScopeRepresentation.getName());
    } else {
      log.info("Failed to create Client Scope. Status: " + response.getStatusInfo().getReasonPhrase());
    }
  }

  /**
   * This method creates the client scope representation.
   *
   * @return the client scope representation
   */
  private ClientScopeRepresentation createClientScopeRepresentation() {
    final ClientScopeRepresentation clientScopeRepresentation = new ClientScopeRepresentation();
    clientScopeRepresentation.setName("openid");
    clientScopeRepresentation.setProtocol("openid-connect");
    clientScopeRepresentation.setProtocolMappers(Arrays.asList(buildUserIdMapper(), buildSubMapper()));
    return clientScopeRepresentation;
  }

  /**
   * This method creates the user id mapper.
   *
   * @return the user id mapper
   */
  private ProtocolMapperRepresentation buildUserIdMapper() {
    final ProtocolMapperRepresentation userIdMapper = new ProtocolMapperRepresentation();
    userIdMapper.setName("user_id");
    userIdMapper.setProtocol("openid-connect");
    userIdMapper.setProtocolMapper("oidc-usermodel-attribute-mapper");
    userIdMapper.setConfig(
        Map.of(
            "user.attribute", "user_id",
            "claim.name", "user_id",
            "jsonType.label", "",
            "id.token.claim", "true",
            "access.token.claim", "true",
            "userinfo.token.claim", "true",
            "multivalued", "false",
            "aggregate.attrs", "false"));
    return userIdMapper;
  }

  /**
   * This method creates the sub mapper.
   *
   * @return the sub mapper
   */
  private ProtocolMapperRepresentation buildSubMapper() {
    final ProtocolMapperRepresentation subMapper = new ProtocolMapperRepresentation();
    subMapper.setName("sub");
    subMapper.setProtocol("openid-connect");
    subMapper.setProtocolMapper("oidc-usermodel-attribute-mapper");
    subMapper.setConfig(
        Map.of(
            "user.attribute", "user_id",
            "claim.name", "sub",
            "jsonType.label", "",
            "id.token.claim", "true",
            "access.token.claim", "true",
            "userinfo.token.claim", "true",
            "multivalued", "false",
            "aggregate.attrs", "false"));
    return subMapper;
  }

}
