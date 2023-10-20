/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.support;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.airbyte.config.User.AuthProvider;
import java.util.HashMap;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;

/**
 * Helper function to parse out JWT token.
 */
@Slf4j
public class JwtTokenParser {

  public static final String JWT_SSO_REALM = "sso_realm";
  public static final String JWT_USER_NAME = "user_name";
  public static final String JWT_USER_EMAIL = "user_email";
  public static final String JWT_AUTH_PROVIDER = "auth_provider";
  public static final String JWT_AUTH_USER_ID = "auth_user_id";

  private static final String ISS_FIELD = "iss";
  private static final String AUTH_REALM_VALUE = "auth/realms/";

  private static final int EXPECTED_SPLIT_LENGTH = 2;

  /**
   * Parse out JWT token and extract fields backend cares about into a map.
   */
  public static String getJwtPayloadToken(final String jwtToken) {
    final String[] tokenParts = jwtToken.split("\\.");
    final String jwtPayload = tokenParts[1];

    return jwtPayload;
  }

  /**
   * Going through JWT payload part and extract fields backend cares about into a map.
   */
  public static Map<String, Object> convertJwtPayloadToUserAttributes(final JsonNode jwtPayloadNode) {
    ObjectMapper objectMapper = new ObjectMapper();
    Map<String, Object> jwtNode = objectMapper.convertValue(jwtPayloadNode, Map.class);
    log.debug("Authenticating with jwtmap {}: ", jwtNode);

    Map<String, Object> jwtMap = new HashMap<>();

    final String ssoRealm = resolveSsoRealm(jwtNode);
    if (ssoRealm != null) {
      jwtMap.put(JWT_SSO_REALM, ssoRealm);
    }
    final AuthProvider authProvider = resolveAuthProvider(jwtNode);
    if (authProvider != null) {
      jwtMap.put(JWT_AUTH_PROVIDER, authProvider);
    }
    if (jwtNode.containsKey("name")) {
      jwtMap.put(JWT_USER_NAME, jwtNode.get("name"));
    }
    if (jwtNode.containsKey("email")) {
      jwtMap.put(JWT_USER_EMAIL, jwtNode.get("email"));
    }
    if (AuthProvider.GOOGLE_IDENTITY_PLATFORM.equals(authProvider) && jwtNode.containsKey("authUserId")) {
      jwtMap.put(JWT_AUTH_USER_ID, jwtNode.get("authUserId"));
    }

    // For keycloak user, the authUserId is the sub field in the jwt token.
    if (AuthProvider.KEYCLOAK.equals(authProvider) && jwtNode.containsKey("sub")) {

      jwtMap.put(JWT_AUTH_USER_ID, jwtNode.get("sub"));
    }

    return jwtMap;
  }

  /**
   * Resolves JWT token to SsoRealm. If Sso realm does not exist, it will return null.
   */
  private static String resolveSsoRealm(final Map<String, Object> jwtNode) {
    if (!jwtNode.containsKey(ISS_FIELD)) {
      return null;
    }

    String issValue = (String) jwtNode.get(ISS_FIELD);

    // For Firebase, iss looks like `"iss": "https://securetoken.google.com/prod-ab-cloud-proj"`;
    // For SSO, iss will be in '"iss": "http://localhost:8000/auth/realms/airbyte"' format.
    // Thus, if issValue does not contain "auth/realms/" or we cannot parse out real realms from the
    // url,
    // for example in case it ends with "auth/realms/", then there is no ssoRealm attached.
    if (issValue == null || !issValue.contains(AUTH_REALM_VALUE) || issValue.endsWith(AUTH_REALM_VALUE)) {
      return null;
    }
    String[] issValueParts = issValue.split(AUTH_REALM_VALUE);
    if (issValueParts.length != EXPECTED_SPLIT_LENGTH) {
      log.error("unexpected iss value received. {}", issValue);
      return null;
    }
    return issValueParts[1];
  }

  private static AuthProvider resolveAuthProvider(final Map<String, Object> jwtNode) {
    if (jwtNode.containsKey("firebase")) {
      return AuthProvider.GOOGLE_IDENTITY_PLATFORM;
    }
    if (resolveSsoRealm(jwtNode) != null) {
      return AuthProvider.KEYCLOAK;
    }

    return null;
  }

}
