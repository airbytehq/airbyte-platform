/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data

/**
 * TokenType describes the various types of auth (JWT) tokens we use in Airbyte.
 *
 * TokenTypes sometimes map to hard-coded roles. See the [RoleResolver].
 */
enum class TokenType {
  // Workload API tokens are a deprecated form of auth,
  // used for internal communication with the workload-api-server.
  @Deprecated("workload API tokens should be replaced by service accounts")
  WORKLOAD_API,

  // Dataplane V1 tokens grant dataplanes access to workload-related APIs.
  // These are deprecated; they will be replaced by service accounts.
  @Deprecated("dataplane tokens should be replaced by service accounts")
  DATAPLANE_V1,

  // Embedded API tokens are used to grant external users temporary, limited access.
  EMBEDDED_V1,

  // Service account tokens represent service accounts.
  // These are NOT legacy, keycloak-based, internal service accounts.
  SERVICE_ACCOUNT,

  // User tokens represent users.
  // This is the default token type if a token cannot be identified by as one of the types above.
  USER,

  ;

  fun toClaim() = "typ" to "io.airbyte.auth.${this.name.lowercase()}"

  companion object {
    fun fromClaims(claims: Map<String, Any>): TokenType {
      val claimedType = claims["typ"] as? String
      if (claimedType != null) {
        for (it in TokenType.entries) {
          if (claimedType == it.toClaim().second) {
            return it
          }
        }
      }
      return USER // fallback to assuming the token represents a user.
    }
  }
}
