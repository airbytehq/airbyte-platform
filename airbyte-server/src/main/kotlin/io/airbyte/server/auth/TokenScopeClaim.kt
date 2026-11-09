/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.auth

import io.airbyte.commons.json.Jsons
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.security.authentication.Authentication

private val logger = KotlinLogging.logger {}

/**
 * TokenScopeClaim is a custom JWT claim that describes the scope a token has access to.
 */
data class TokenScopeClaim(
  val workspaceId: String,
) {
  companion object {
    const val CLAIM_ID = "io.airbyte.auth.workspace_scope"
  }
}

internal fun getTokenScopeClaim(authentication: Authentication): TokenScopeClaim? {
  val claim = authentication.attributes[TokenScopeClaim.CLAIM_ID] ?: return null
  return try {
    Jsons.convertValue(claim, TokenScopeClaim::class.java)
  } catch (e: Exception) {
    logger.error(e) { "could not parse token scope claim" }
    null
  }
}
