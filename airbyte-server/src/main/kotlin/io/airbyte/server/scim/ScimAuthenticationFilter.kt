/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.scim

import io.airbyte.api.scim.generated.models.ScimError
import io.airbyte.domain.models.scim.ScimAccessDeniedException
import io.airbyte.domain.models.scim.ScimAuthenticationException
import io.airbyte.domain.services.scim.ScimAuthenticationService
import io.micronaut.core.async.publisher.Publishers
import io.micronaut.core.order.Ordered
import io.micronaut.http.HttpHeaders
import io.micronaut.http.HttpRequest
import io.micronaut.http.HttpResponse
import io.micronaut.http.HttpStatus
import io.micronaut.http.MutableHttpHeaders
import io.micronaut.http.MutableHttpResponse
import io.micronaut.http.annotation.RequestFilter
import io.micronaut.http.annotation.ServerFilter
import io.micronaut.http.filter.FilterContinuation
import io.micronaut.http.filter.ServerFilterPhase
import io.micronaut.scheduling.TaskExecutors
import io.micronaut.scheduling.annotation.ExecuteOn
import org.reactivestreams.Publisher
import reactor.core.publisher.Flux

@ServerFilter("/scim/v2/**")
@ExecuteOn(TaskExecutors.BLOCKING)
class ScimAuthenticationFilter(
  private val authenticationService: ScimAuthenticationService,
) : Ordered {
  override fun getOrder(): Int = ServerFilterPhase.SECURITY.before()

  @RequestFilter
  fun filter(
    request: HttpRequest<*>,
    continuation: FilterContinuation<Publisher<HttpResponse<*>>>,
  ): Publisher<HttpResponse<*>> {
    val rawToken = parseBearerToken(request) ?: return Publishers.just(unauthorized())

    val context =
      try {
        authenticationService.authenticate(rawToken)
      } catch (_: ScimAuthenticationException) {
        return Publishers.just(unauthorized())
      } catch (_: ScimAccessDeniedException) {
        return Publishers.just(forbidden())
      }

    request.setAttribute(SCIM_AUTHENTICATION_ATTRIBUTE, context)
    (request.headers as MutableHttpHeaders).remove(HttpHeaders.AUTHORIZATION)

    return Flux
      .from(continuation.proceed())
      .onErrorResume(ScimAuthenticationException::class.java) { Flux.just(unauthorized()) }
      .onErrorResume(ScimAccessDeniedException::class.java) { Flux.just(forbidden()) }
  }

  private fun parseBearerToken(request: HttpRequest<*>): String? {
    val authorizationHeaders = request.headers.getAll(HttpHeaders.AUTHORIZATION)
    if (authorizationHeaders.size != 1) {
      return null
    }

    val value = authorizationHeaders.single()
    if (value.length <= BEARER_SCHEME.length || !value.regionMatches(0, BEARER_SCHEME, 0, BEARER_SCHEME.length, ignoreCase = true)) {
      return null
    }
    if (value[BEARER_SCHEME.length] != ' ') {
      return null
    }

    var tokenStart = BEARER_SCHEME.length
    while (tokenStart < value.length && value[tokenStart] == ' ') {
      tokenStart++
    }
    return value.substring(tokenStart).takeIf(String::isNotEmpty)
  }

  private fun unauthorized(): MutableHttpResponse<ScimError> =
    HttpResponse
      .status<ScimError>(HttpStatus.UNAUTHORIZED)
      .contentType(SCIM_MEDIA_TYPE)
      .header(HttpHeaders.WWW_AUTHENTICATE, BEARER_SCHEME)
      .body(
        scimError(
          status = HttpStatus.UNAUTHORIZED,
          detail = "Invalid bearer token",
        ),
      )

  private fun forbidden(): MutableHttpResponse<ScimError> =
    HttpResponse
      .status<ScimError>(HttpStatus.FORBIDDEN)
      .contentType(SCIM_MEDIA_TYPE)
      .body(
        scimError(
          status = HttpStatus.FORBIDDEN,
          detail = "SCIM access is denied",
        ),
      )

  private companion object {
    const val BEARER_SCHEME = "Bearer"
  }
}
