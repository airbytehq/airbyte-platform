/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.support

import com.fasterxml.jackson.databind.ObjectMapper
import io.airbyte.commons.auth.support.JwtTokenParser
import io.airbyte.commons.auth.support.JwtTokenParser.convertJwtPayloadToUserAttributes
import io.airbyte.config.AuthProvider
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test

internal class JwtTokenParserTest {
  @Test
  @Throws(Exception::class)
  fun testResolveUser_firebase() {
    val jsonNode =
      ObjectMapper().readTree(
        (
          "{" +
            "\"name\": \"test-user-name\", " +
            "\"email\": \"test-user-email\", " +
            "\"email_verified\": true, " +
            "\"firebase\": \"someFirebaseField\", " +
            "\"authUserId\": \"test-user-auth-id\"" +
            "}"
        ),
      )
    val resolvedJwtMap: Map<String, Any> = convertJwtPayloadToUserAttributes(jsonNode)

    Assertions.assertEquals(resolvedJwtMap[JwtTokenParser.JWT_USER_NAME], "test-user-name")
    Assertions.assertEquals(resolvedJwtMap[JwtTokenParser.JWT_USER_EMAIL], "test-user-email")
    Assertions.assertEquals(resolvedJwtMap[JwtTokenParser.JWT_USER_EMAIL_VERIFIED], true)
    Assertions.assertEquals(resolvedJwtMap[JwtTokenParser.JWT_AUTH_PROVIDER], AuthProvider.GOOGLE_IDENTITY_PLATFORM)
    Assertions.assertEquals(resolvedJwtMap[JwtTokenParser.JWT_AUTH_USER_ID], "test-user-auth-id")
  }

  @Test
  @Throws(Exception::class)
  fun testResolveUser_keycloak() {
    val jsonNode =
      ObjectMapper().readTree(
        (
          "{" +
            "\"name\": \"test-user-name\", " +
            "\"email\": \"test-user-email\", " +
            "\"email_verified\": false, " +
            "\"iss\": \"http://localhost:8000/auth/realms/airbyte\", " +
            "\"sub\": \"test-user-auth-id\"" +
            "}"
        ),
      )
    val resolvedJwtMap: Map<String, Any> = convertJwtPayloadToUserAttributes(jsonNode)

    Assertions.assertEquals(resolvedJwtMap[JwtTokenParser.JWT_SSO_REALM], "airbyte")
    Assertions.assertEquals(resolvedJwtMap[JwtTokenParser.JWT_USER_NAME], "test-user-name")
    Assertions.assertEquals(resolvedJwtMap[JwtTokenParser.JWT_USER_EMAIL], "test-user-email")
    Assertions.assertEquals(resolvedJwtMap[JwtTokenParser.JWT_USER_EMAIL_VERIFIED], false)
    Assertions.assertEquals(resolvedJwtMap[JwtTokenParser.JWT_AUTH_PROVIDER], AuthProvider.KEYCLOAK)
    Assertions.assertEquals(resolvedJwtMap[JwtTokenParser.JWT_AUTH_USER_ID], "test-user-auth-id")
  }

  @Test
  @Throws(Exception::class)
  fun testResolveSsoRealm_firebase() {
    val issuer = "https://securetoken.google.com/test-firebase"
    val jsonNode = ObjectMapper().readTree("{\"iss\": \"" + issuer + "\"}")

    val resolvedJwtMap: Map<String, Any> = convertJwtPayloadToUserAttributes(jsonNode)

    Assertions.assertEquals(resolvedJwtMap[JwtTokenParser.JWT_SSO_REALM], issuer)
  }

  @Test
  @Throws(Exception::class)
  fun testResolveSsoRealm_keycloak() {
    val jsonNode = ObjectMapper().readTree("{\"iss\": \"http://localhost:8000/auth/realms/airbyte\"}")

    val resolvedJwtMap: Map<String, Any> = convertJwtPayloadToUserAttributes(jsonNode)

    Assertions.assertEquals(resolvedJwtMap[JwtTokenParser.JWT_SSO_REALM], "airbyte")
  }
}
