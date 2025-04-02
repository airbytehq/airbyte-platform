/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.auth.support

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.commons.json.Jsons
import io.airbyte.config.AuthProvider
import io.github.oshai.kotlinlogging.KotlinLogging
import java.nio.charset.StandardCharsets
import java.util.Base64

private val log = KotlinLogging.logger {}

/**
 * Helper function to parse out JWT token.
 */
object JwtTokenParser {
  const val JWT_SSO_REALM = "sso_realm"
  const val JWT_USER_NAME = "user_name"
  const val JWT_USER_EMAIL = "user_email"
  const val JWT_AUTH_PROVIDER = "auth_provider"
  const val JWT_AUTH_USER_ID = "auth_user_id"
  const val JWT_FIREBASE = "firebase"
  const val JWT_USER_EMAIL_VERIFIED = "email_verified"

  private const val ISS_FIELD = "iss"
  private const val AUTH_REALM_VALUE = "auth/realms/"
  private const val EXPECTED_SPLIT_LENGTH: Int = 2

  /**
   * Get the payload part of the given JWT.
   */
  @JvmStatic
  fun getJwtPayloadToken(jwtToken: String): String = jwtToken.split("\\.".toRegex())[1]

  @JvmStatic
  fun tokenToAttributes(jwtToken: String): Map<String, Any> {
    val rawJwtPayload = getJwtPayloadToken(jwtToken)
    val jwtPayloadDecoded = Base64.getUrlDecoder().decode(rawJwtPayload).toString(StandardCharsets.UTF_8)
    val jwtPayloadNode = Jsons.deserialize(jwtPayloadDecoded)
    return convertJwtPayloadToUserAttributes(jwtPayloadNode)
  }

  /**
   * Going through JWT payload part and extract fields backend cares about into a map.
   */
  @JvmStatic
  fun convertJwtPayloadToUserAttributes(jwtPayloadNode: JsonNode?): Map<String, Any> {
    @Suppress("UNCHECKED_CAST")
    val jwtNode: Map<String, Any> = Jsons.convertValue(jwtPayloadNode, Map::class.java) as Map<String, Any>
    log.debug { "Authenticating with jwtmap: $jwtNode" }

    val jwtMap = mutableMapOf<String, Any>()
    resolveSsoRealm(jwtNode)?.let { jwtMap[JWT_SSO_REALM] = it }
    val authProvider = resolveAuthProvider(jwtNode)?.also { jwtMap[JWT_AUTH_PROVIDER] = it }
    jwtNode["name"]?.let { jwtMap[JWT_USER_NAME] = it }
    jwtNode["email"]?.let { jwtMap[JWT_USER_EMAIL] = it }
    jwtNode["email_verified"]?.let { jwtMap[JWT_USER_EMAIL_VERIFIED] = it }
    if (AuthProvider.GOOGLE_IDENTITY_PLATFORM == authProvider) {
      jwtNode["authUserId"]?.let { jwtMap[JWT_AUTH_USER_ID] = it } ?: run {
        // speakeasy generated jwt tokens contain the auth user id under the userId field
        jwtNode["user_id"]?.let { jwtMap[JWT_AUTH_USER_ID] = it }
      }
      jwtNode[JWT_FIREBASE]?.let { jwtMap[JWT_FIREBASE] = it }
    }

    // For keycloak user, the authUserId is the sub field in the jwt token.
    if (AuthProvider.KEYCLOAK == authProvider) {
      jwtNode["sub"]?.let { jwtMap[JWT_AUTH_USER_ID] = it }
    }

    return jwtMap
  }

  /**
   * Resolves JWT token to SsoRealm. If Sso realm does not exist, it will return null.
   */
  private fun resolveSsoRealm(jwtNode: Map<String, Any>): String? {
    if (!jwtNode.containsKey(ISS_FIELD)) {
      return null
    }

    val issValue = jwtNode[ISS_FIELD] as String?

    // For Firebase, iss looks like `"iss": "https://securetoken.google.com/prod-ab-cloud-proj"`;
    // For SSO, iss will be in '"iss": "http://localhost:8000/auth/realms/airbyte"' format.
    // Thus, if issValue does not contain "auth/realms/" or we cannot parse out real realms from the
    // url,
    // for example in case it ends with "auth/realms/", then there is no ssoRealm attached.
    if (issValue == null || !issValue.contains(AUTH_REALM_VALUE) || issValue.endsWith(AUTH_REALM_VALUE)) {
      return issValue
    }

    val issValueParts = issValue.split(AUTH_REALM_VALUE.toRegex())
    if (issValueParts.size != EXPECTED_SPLIT_LENGTH) {
      log.error { "unexpected iss value received. $issValue" }
      return null
    }

    return issValueParts[1]
  }

  private fun resolveAuthProvider(jwtNode: Map<String, Any>): AuthProvider? =
    when {
      jwtNode.containsKey(JWT_FIREBASE) -> AuthProvider.GOOGLE_IDENTITY_PLATFORM
      resolveSsoRealm(jwtNode) != null -> AuthProvider.KEYCLOAK
      else -> null
    }
}
