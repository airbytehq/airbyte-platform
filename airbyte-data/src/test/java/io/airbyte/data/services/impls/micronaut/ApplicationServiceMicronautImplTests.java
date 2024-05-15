/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.micronaut;

import static io.airbyte.data.services.impls.micronaut.ApplicationServiceMicronautImpl.DEFAULT_AUTH_USER_ID;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import io.airbyte.commons.auth.AuthRole;
import io.airbyte.commons.auth.OrganizationAuthRole;
import io.airbyte.commons.auth.WorkspaceAuthRole;
import io.airbyte.commons.json.Jsons;
import io.airbyte.config.User;
import io.airbyte.data.config.InstanceAdminConfig;
import io.micronaut.context.annotation.Requires;
import io.micronaut.context.env.Environment;
import io.micronaut.security.token.jwt.generator.JwtTokenGenerator;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import jakarta.ws.rs.BadRequestException;
import java.util.Base64;
import java.util.HashSet;
import java.util.Set;
import org.apache.commons.lang3.NotImplementedException;
import org.junit.jupiter.api.Test;

@MicronautTest
@Requires(env = {Environment.TEST})
public class ApplicationServiceMicronautImplTests {

  @Inject
  private InstanceAdminConfig instanceAdminConfig;

  @Inject
  private JwtTokenGenerator tokenGenerator;

  @Test
  void testGetToken() {
    final var applicationServer = new ApplicationServiceMicronautImpl(
        instanceAdminConfig,
        tokenGenerator);

    var expectedRoles = new HashSet<>();
    expectedRoles.addAll(AuthRole.buildAuthRolesSet(AuthRole.ADMIN));
    expectedRoles.addAll(WorkspaceAuthRole.buildWorkspaceAuthRolesSet(WorkspaceAuthRole.WORKSPACE_ADMIN));
    expectedRoles.addAll(OrganizationAuthRole.buildOrganizationAuthRolesSet(OrganizationAuthRole.ORGANIZATION_ADMIN));
    var token = applicationServer.getToken("test-client-id", "test-client-secret");
    var claims = getTokenClaims(token);

    assertEquals(expectedRoles, getRolesFromNode((ArrayNode) claims.get("roles")));
    assertEquals("airbyte-server", claims.get("iss").asText());
    assertEquals(DEFAULT_AUTH_USER_ID.toString(), claims.get("sub").asText());
  }

  @Test
  void testGetTokenWithInvalidCredentials() {
    final var applicationServer = new ApplicationServiceMicronautImpl(
        instanceAdminConfig,
        tokenGenerator);

    assertThrows(BadRequestException.class, () -> applicationServer.getToken("test-client-id", "wrong-secret"));
  }

  @Test
  void testListingApplications() {
    final var applicationServer = new ApplicationServiceMicronautImpl(
        instanceAdminConfig,
        tokenGenerator);

    var applications = applicationServer.listApplicationsByUser(new User().withName("Test User"));
    assertEquals(1, applications.size());
  }

  @Test
  void testCreateApplication() {
    final var applicationServer = new ApplicationServiceMicronautImpl(
        instanceAdminConfig,
        tokenGenerator);

    assertThrows(NotImplementedException.class, () -> applicationServer.createApplication(new User(), "Test Application"));
  }

  @Test
  void testDeleteApplication() {
    final var applicationServer = new ApplicationServiceMicronautImpl(
        instanceAdminConfig,
        tokenGenerator);

    assertThrows(NotImplementedException.class, () -> applicationServer.deleteApplication(new User(), "Test Application"));
  }

  private JsonNode getTokenClaims(final String token) {
    final String decodedPayload = new String(Base64.getUrlDecoder().decode(token.split("\\.")[1]), java.nio.charset.StandardCharsets.UTF_8);
    return Jsons.deserialize(decodedPayload);
  }

  private Set<String> getRolesFromNode(final ArrayNode claimsNode) {
    final Set<String> roles = new HashSet<>();
    for (JsonNode role : claimsNode) {
      roles.add(role.asText());
    }
    return roles;
  }

}
