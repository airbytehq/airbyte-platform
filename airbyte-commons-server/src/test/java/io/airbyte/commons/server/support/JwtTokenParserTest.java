/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.support;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.airbyte.commons.auth.support.JwtTokenParser;
import io.airbyte.config.AuthProvider;
import java.util.Map;
import org.junit.jupiter.api.Test;

class JwtTokenParserTest {

  @Test
  void testResolveUser_firebase() throws Exception {
    JsonNode jsonNode =
        new ObjectMapper().readTree(
            "{"
                + "\"name\": \"test-user-name\", "
                + "\"email\": \"test-user-email\", "
                + "\"email_verified\": true, "
                + "\"firebase\": \"someFirebaseField\", "
                + "\"authUserId\": \"test-user-auth-id\""
                + "}");
    final Map<String, Object> resolvedJwtMap = JwtTokenParser.convertJwtPayloadToUserAttributes(jsonNode);

    assertEquals(resolvedJwtMap.get(JwtTokenParser.JWT_USER_NAME), "test-user-name");
    assertEquals(resolvedJwtMap.get(JwtTokenParser.JWT_USER_EMAIL), "test-user-email");
    assertEquals(resolvedJwtMap.get(JwtTokenParser.JWT_USER_EMAIL_VERIFIED), true);
    assertEquals(resolvedJwtMap.get(JwtTokenParser.JWT_AUTH_PROVIDER), AuthProvider.GOOGLE_IDENTITY_PLATFORM);
    assertEquals(resolvedJwtMap.get(JwtTokenParser.JWT_AUTH_USER_ID), "test-user-auth-id");
  }

  @Test
  void testResolveUser_keycloak() throws Exception {
    JsonNode jsonNode =
        new ObjectMapper().readTree("{"
            + "\"name\": \"test-user-name\", "
            + "\"email\": \"test-user-email\", "
            + "\"email_verified\": false, "
            + "\"iss\": \"http://localhost:8000/auth/realms/airbyte\", "
            + "\"sub\": \"test-user-auth-id\""
            + "}");
    final Map<String, Object> resolvedJwtMap = JwtTokenParser.convertJwtPayloadToUserAttributes(jsonNode);

    assertEquals(resolvedJwtMap.get(JwtTokenParser.JWT_SSO_REALM), "airbyte");
    assertEquals(resolvedJwtMap.get(JwtTokenParser.JWT_USER_NAME), "test-user-name");
    assertEquals(resolvedJwtMap.get(JwtTokenParser.JWT_USER_EMAIL), "test-user-email");
    assertEquals(resolvedJwtMap.get(JwtTokenParser.JWT_USER_EMAIL_VERIFIED), false);
    assertEquals(resolvedJwtMap.get(JwtTokenParser.JWT_AUTH_PROVIDER), AuthProvider.KEYCLOAK);
    assertEquals(resolvedJwtMap.get(JwtTokenParser.JWT_AUTH_USER_ID), "test-user-auth-id");
  }

  @Test
  void testResolveSsoRealm_firebase() throws Exception {
    JsonNode jsonNode = new ObjectMapper().readTree("{\"iss\": \"https://securetoken.google.com/test-firebase\"}");

    final Map<String, Object> resolvedJwtMap = JwtTokenParser.convertJwtPayloadToUserAttributes(jsonNode);

    assertNull(resolvedJwtMap.get(JwtTokenParser.JWT_SSO_REALM));
  }

  @Test
  void testResolveSsoRealm_keycloak() throws Exception {

    JsonNode jsonNode = new ObjectMapper().readTree("{\"iss\": \"http://localhost:8000/auth/realms/airbyte\"}");

    final Map<String, Object> resolvedJwtMap = JwtTokenParser.convertJwtPayloadToUserAttributes(jsonNode);

    assertEquals(resolvedJwtMap.get(JwtTokenParser.JWT_SSO_REALM), "airbyte");
  }

}
