/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.scim

import io.micronaut.core.async.publisher.Publishers
import io.micronaut.http.HttpRequest
import io.micronaut.security.authentication.Authentication
import io.micronaut.security.rules.SecuredAnnotationRule
import io.micronaut.security.rules.SecurityRule
import io.micronaut.security.rules.SecurityRuleResult
import jakarta.inject.Singleton
import org.reactivestreams.Publisher

@Singleton
class ScimSecurityRule : SecurityRule<HttpRequest<*>> {
  override fun getOrder(): Int = SecuredAnnotationRule.ORDER - 1

  override fun check(
    request: HttpRequest<*>,
    authentication: Authentication?,
  ): Publisher<SecurityRuleResult> =
    Publishers.just(
      when {
        !request.isScimRequest() -> SecurityRuleResult.UNKNOWN
        request.hasScimAuthenticationContext() -> SecurityRuleResult.ALLOWED
        else -> SecurityRuleResult.REJECTED
      },
    )

  private fun HttpRequest<*>.isScimRequest(): Boolean = path == SCIM_BASE_PATH || path.startsWith("$SCIM_BASE_PATH/")

  private fun HttpRequest<*>.hasScimAuthenticationContext(): Boolean = getAttribute(SCIM_AUTHENTICATION_ATTRIBUTE).isPresent

  private companion object {
    const val SCIM_BASE_PATH = "/scim/v2"
  }
}
