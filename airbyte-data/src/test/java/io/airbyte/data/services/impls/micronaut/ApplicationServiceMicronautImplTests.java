/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.micronaut;

import static io.airbyte.data.services.impls.data.ApplicationServiceMicronautImpl.DEFAULT_AUTH_USER_ID;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import io.airbyte.commons.auth.AuthRole;
import io.airbyte.commons.auth.config.TokenExpirationConfig;
import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.resources.MoreResources;
import io.airbyte.config.AuthenticatedUser;
import io.airbyte.data.config.InstanceAdminConfig;
import io.airbyte.data.services.impls.data.ApplicationServiceMicronautImpl;
import io.micronaut.security.token.jwt.generator.JwtTokenGenerator;
import jakarta.ws.rs.BadRequestException;
import java.io.IOException;
import java.util.Base64;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ApplicationServiceMicronautImplTests {

  private InstanceAdminConfig instanceAdminConfig;

  private TokenExpirationConfig tokenExpirationConfig;

  private JwtTokenGenerator tokenGenerator;

  private String token;

  private final String issuer = "https://example.com";

  @BeforeEach
  void setup() throws IOException {
    token = MoreResources.readResource("test.token");
    instanceAdminConfig = new InstanceAdminConfig();
    instanceAdminConfig.setUsername("test");
    instanceAdminConfig.setPassword("test-password");
    instanceAdminConfig.setClientId("test-client-id");
    instanceAdminConfig.setClientSecret("test-client-secret");
    tokenGenerator = mock(JwtTokenGenerator.class);
    tokenExpirationConfig = new TokenExpirationConfig();
    when(tokenGenerator.generateToken(any())).thenReturn(Optional.of(token));
  }

  @Test
  void testGetToken() {
    final var applicationServer = new ApplicationServiceMicronautImpl(
        instanceAdminConfig,
        tokenExpirationConfig,
        tokenGenerator,
        issuer);

    final var expectedRoles = new HashSet<>();
    expectedRoles.addAll(AuthRole.getInstanceAdminRoles());
    final var token = applicationServer.getToken("test-client-id", "test-client-secret");
    final var claims = getTokenClaims(token);

    assertEquals(expectedRoles, getRolesFromNode((ArrayNode) claims.get("roles")));
    assertEquals("airbyte-server", claims.get("iss").asText());
    assertEquals(DEFAULT_AUTH_USER_ID.toString(), claims.get("sub").asText());
  }

  @Test
  void testGetTokenWithInvalidCredentials() {
    final var applicationServer = new ApplicationServiceMicronautImpl(
        instanceAdminConfig,
        tokenExpirationConfig,
        tokenGenerator,
        issuer);

    assertThrows(BadRequestException.class, () -> applicationServer.getToken("test-client-id", "wrong-secret"));
  }

  @Test
  void testListingApplications() {
    final var applicationServer = new ApplicationServiceMicronautImpl(
        instanceAdminConfig,
        tokenExpirationConfig,
        tokenGenerator,
        issuer);

    final var applications = applicationServer.listApplicationsByUser(new AuthenticatedUser().withName("Test User"));
    assertEquals(1, applications.size());
  }

  @Test
  void testCreateApplication() {
    final var applicationServer = new ApplicationServiceMicronautImpl(
        instanceAdminConfig,
        tokenExpirationConfig,
        tokenGenerator,
        issuer);

    assertThrows(UnsupportedOperationException.class, () -> applicationServer.createApplication(new AuthenticatedUser(), "Test Application"));
  }

  @Test
  void testDeleteApplication() {
    final var applicationServer = new ApplicationServiceMicronautImpl(
        instanceAdminConfig,
        tokenExpirationConfig,
        tokenGenerator,
        issuer);

    assertThrows(UnsupportedOperationException.class, () -> applicationServer.deleteApplication(new AuthenticatedUser(), "Test Application"));
  }

  private JsonNode getTokenClaims(final String token) {
    final String decodedPayload = new String(Base64.getUrlDecoder().decode(token.split("\\.")[1]), java.nio.charset.StandardCharsets.UTF_8);
    return Jsons.deserialize(decodedPayload);
  }

  private Set<String> getRolesFromNode(final ArrayNode claimsNode) {
    final Set<String> roles = new HashSet<>();
    for (final JsonNode role : claimsNode) {
      roles.add(role.asText());
    }
    return roles;
  }

}
