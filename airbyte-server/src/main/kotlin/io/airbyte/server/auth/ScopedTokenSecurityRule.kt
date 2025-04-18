/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.auth

import io.airbyte.commons.auth.permissions.IntentSecurityRule
import io.airbyte.commons.json.Jsons
import io.airbyte.commons.server.support.AuthenticationHeaderResolver
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.http.HttpRequest
import io.micronaut.security.authentication.Authentication
import io.micronaut.security.rules.SecurityRule
import io.micronaut.security.rules.SecurityRuleResult
import jakarta.inject.Singleton
import org.reactivestreams.Publisher
import reactor.core.publisher.Flux

private val logger = KotlinLogging.logger {}

/**
 * ScopedTokenSecurityRule authorizes "scoped tokens".
 *
 * A scoped token is a JWT token that contains a [TokenScopeClaim],
 * which grants a request access to a single workspace.
 *
 * The [TokenScopeClaim.workspaceId] must match the workspace of the resource being requested.
 * Workspace IDs are not easily available in our API endpoints and request bodies,
 * so [AuthenticationHeaderResolver] is used to resolve requests to workspace IDs.
 *
 * Scoped tokens are used when there isn't a user account attached to the request.
 * The RBAC permission system was built around user accounts, so when there's no user account,
 * there's no way to determine whether a request has access to a given resource in a
 * workspace or organization. Scoped tokens solve this by encoding the scope of access
 * directly into the auth token (JWT).
 *
 * For example, Airbyte Embedded uses this to grant temporary access to an external user,
 * granting them access to only a specific set of API endpoints related to the Embedded
 * widget use case, and only to a specific workspace. Airbyte data planes use these tokens
 * to grant data planes access to a data-plane-related endpoints in a specific organization.
 */
@Singleton
class ScopedTokenSecurityRule(
  private val authenticationHeaderResolver: AuthenticationHeaderResolver,
) : SecurityRule<HttpRequest<*>> {
  override fun getOrder(): Int {
    // Process this rule before IntentSecurityRule.
    return IntentSecurityRule.ORDER - 100
  }

  override fun check(
    request: HttpRequest<*>,
    authentication: Authentication?,
  ): Publisher<SecurityRuleResult> {
    // The health endpoint doesn't support auth.
    // This just makes debugging easier, it's not strictly necessary.
    if (request.path == "/api/v1/health") {
      return Flux.just(SecurityRuleResult.UNKNOWN)
    }

    if (authentication == null) {
      return Flux.just(SecurityRuleResult.UNKNOWN)
    }

    // Get the TokenScopeClaim from the JWT.
    val tokenScopeClaim = getTokenScopeClaim(authentication)
    if (tokenScopeClaim == null) {
      // If there's no TokenScopeClaim, then there's nothing to verify.
      return Flux.just(SecurityRuleResult.UNKNOWN)
    }

    // Scoped token requests must match a single workspace ID.
    // If we can't find a workspace ID in the request, the request is rejected.
    val requestedWorkspaceId = resolveWorkspaceId(request)
    if (requestedWorkspaceId == null) {
      return Flux.just(SecurityRuleResult.REJECTED)
    }

    // If the requested workspace doesn't match the token scope claim,
    // the request is rejected.
    if (tokenScopeClaim.workspaceId != requestedWorkspaceId) {
      return Flux.just(SecurityRuleResult.REJECTED)
    }

    // The request continues on to normal role-based auth (that's why this isn't ALLOWED).
    // This will continue on to the [IntentSecurityRule], which verifies role-based access.
    return Flux.just(SecurityRuleResult.UNKNOWN)
  }

  // Use the AuthenticationHeaderResolver to resolve the request fields to a single workspace ID.
  private fun resolveWorkspaceId(request: HttpRequest<*>): String? {
    val headerMap = request.headers.asMap(String::class.java, String::class.java)
    val requestedWorkspaceIds = authenticationHeaderResolver.resolveWorkspace(headerMap)
    if (requestedWorkspaceIds.size != 1) {
      return null
    }
    return requestedWorkspaceIds.first().toString()
  }
}

// Get the TokenScopeClaim from the Authentication attributes (which come from the JWT).
private fun getTokenScopeClaim(authentication: Authentication): TokenScopeClaim? {
  val obj = authentication.attributes[TokenScopeClaim.CLAIM_ID]
  if (obj == null) {
    return null
  }

  try {
    return Jsons.convertValue(obj, TokenScopeClaim::class.java)
  } catch (e: Exception) {
    logger.error(e) { "could not parse token scope claim" }
    return null
  }
}
