package io.airbyte.commons.auth.permissions

import io.airbyte.commons.auth.generated.Intent
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.core.annotation.Nullable
import io.micronaut.http.HttpAttributes
import io.micronaut.http.HttpRequest
import io.micronaut.security.authentication.Authentication
import io.micronaut.security.rules.SecuredAnnotationRule
import io.micronaut.security.rules.SecurityRule
import io.micronaut.security.rules.SecurityRuleResult
import io.micronaut.web.router.RouteMatch
import jakarta.inject.Singleton
import org.reactivestreams.Publisher
import reactor.core.publisher.Flux

val logger = KotlinLogging.logger {}

@Singleton
class IntentSecurityRule : SecurityRule<HttpRequest<*>> {
  override fun getOrder(): Int {
    // We want this rule to run before the @Secured annotation
    return SecuredAnnotationRule.ORDER - 100
  }

  override fun check(
    request: HttpRequest<*>,
    authentication: @Nullable Authentication?,
  ): Publisher<SecurityRuleResult> {
    val routeMatch = request.getAttribute(HttpAttributes.ROUTE_MATCH, RouteMatch::class.java).orElse(null)
    if (!routeMatch.isAnnotationPresent(RequiresIntent::class.java)) {
      logger.debug { "RequiresIntent annotation not found on request, returning UNKNOWN" }
      return Flux.just(SecurityRuleResult.UNKNOWN)
    }
    val userRoles = authentication?.roles ?: return Flux.just(SecurityRuleResult.UNKNOWN)
    val intentName = routeMatch.getAnnotation(RequiresIntent::class.java).stringValue().orElseThrow()
    val intent = Intent.valueOf(intentName)

    if (intent.roles.isEmpty()) {
      logger.debug { "No roles found for intent $intent, rejecting request" }
      return Flux.just(SecurityRuleResult.REJECTED)
    }

    if (userRoles.any { it: String? -> intent.roles.contains(it) }) {
      return Flux.just(SecurityRuleResult.ALLOWED)
    }

    return Flux.just(SecurityRuleResult.REJECTED)
  }
}
