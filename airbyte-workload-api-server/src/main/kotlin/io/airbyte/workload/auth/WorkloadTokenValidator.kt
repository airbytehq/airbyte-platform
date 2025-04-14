/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.auth

import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.Value
import io.micronaut.core.order.Ordered
import io.micronaut.http.HttpRequest
import io.micronaut.security.authentication.Authentication
import io.micronaut.security.token.validator.TokenValidator
import jakarta.inject.Singleton
import org.reactivestreams.Publisher
import reactor.core.publisher.Mono
import java.util.Base64

private val LOGGER = KotlinLogging.logger {}

@Singleton
class WorkloadTokenValidator(
  @Value("\${airbyte.workload-api.bearer-token.secret}") val bearerSecret: String,
) : TokenValidator<HttpRequest<*>> {
  override fun validateToken(
    token: String,
    request: HttpRequest<*>?,
  ): Publisher<Authentication> =
    try {
      if (authenticateRequest(token)) {
        LOGGER.debug { "Request authorized." }
        Mono.just(Authentication.build(WORKLOAD_API_USER))
      } else {
        LOGGER.debug { "Request denied." }
        Mono.empty()
      }
    } catch (exception: Exception) {
      LOGGER.debug(exception) { "Error validating token" }
      Mono.empty()
    }

  // This validator should run last.
  // This hopefully allows the default NimbusReactiveJsonWebTokenValidator to run and use micronaut security to validate JWTs.
  override fun getOrder(): Int = Ordered.LOWEST_PRECEDENCE

  private fun authenticateRequest(token: String): Boolean =
    Base64
      .getUrlDecoder()
      .decode(token)
      .decodeToString()
      .trim() == bearerSecret

  companion object {
    const val WORKLOAD_API_USER: String = "WORKLOAD_API_USER"
  }
}
