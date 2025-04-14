/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.auth

/**
 * TokenScopeClaim is a custom JWT claim that describes the scope a token has access to.
 *
 * See [ScopedTokenSecurityRule] for details.
 */
data class TokenScopeClaim(
  val workspaceId: String,
) {
  companion object {
    const val CLAIM_ID = "io.airbyte.auth.workspace_scope"
  }
}
