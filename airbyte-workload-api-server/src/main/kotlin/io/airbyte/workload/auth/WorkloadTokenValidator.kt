/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.auth

import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.Value
import io.micronaut.http.HttpRequest
import io.micronaut.security.authentication.Authentication
import io.micronaut.security.authentication.AuthenticationFailureReason
import io.micronaut.security.authentication.AuthenticationResponse
import io.micronaut.security.token.validator.TokenValidator
import jakarta.inject.Singleton
import org.reactivestreams.Publisher
import reactor.core.publisher.Flux
import reactor.core.publisher.FluxSink
import java.util.Base64

private val LOGGER = KotlinLogging.logger {}

/**
 * Validates that the token extracted from the HTTP request has access to the workload API.
 */
@Singleton
class WorkloadTokenValidator(
  @Value("\${micronaut.security.token.jwt.bearer.secret}") val bearerSecret: String,
) : TokenValidator<HttpRequest<*>> {
  override fun validateToken(
    token: String,
    request: HttpRequest<*>?,
  ): Publisher<Authentication> {
    return Flux.create<Authentication> { emitter: FluxSink<Authentication?> ->
      if (authenticateRequest(token)) {
        LOGGER.debug { "Request authorized." }
        emitter.next(Authentication.build(WORKLOAD_API_USER))
        emitter.complete()
      } else {
        LOGGER.debug { "Request denied." }
        emitter.error(AuthenticationResponse.exception(AuthenticationFailureReason.CREDENTIALS_DO_NOT_MATCH))
      }
    }
  }

  private fun authenticateRequest(token: String): Boolean {
    return Base64.getUrlDecoder().decode(token).decodeToString().trim() == bearerSecret
  }

  companion object {
    const val WORKLOAD_API_USER: String = "WORKLOAD_API_USER"
  }
}
