/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.scim

import io.airbyte.api.scim.generated.models.ScimError
import io.airbyte.commons.json.Jsons
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.http.HttpHeaders
import io.micronaut.http.HttpRequest
import io.micronaut.http.HttpResponse
import io.micronaut.http.HttpStatus
import io.micronaut.http.MutableHttpResponse
import io.micronaut.http.annotation.Filter
import io.micronaut.http.filter.HttpServerFilter
import io.micronaut.http.filter.ServerFilterChain
import io.micronaut.http.filter.ServerFilterPhase
import org.reactivestreams.Publisher
import reactor.core.publisher.Flux
import java.util.UUID

private val log = KotlinLogging.logger {}

@Filter("/scim/v2/**")
class ScimErrorResponseFilter : HttpServerFilter {
  override fun getOrder(): Int = ServerFilterPhase.SECURITY.before() - 1

  override fun doFilter(
    request: HttpRequest<*>,
    chain: ServerFilterChain,
  ): Publisher<MutableHttpResponse<*>> =
    Flux
      .defer { Flux.from(chain.proceed(request)) }
      .map(::normalizeErrorResponse)
      .onErrorResume { throwable ->
        val errorId = UUID.randomUUID()
        log.error {
          "Unhandled exception while processing SCIM request [errorId: $errorId, exceptionType: ${throwable.javaClass.name}]"
        }
        Flux.just(scimErrorResponse(HttpStatus.INTERNAL_SERVER_ERROR, "Internal server error. Reference: $errorId"))
      }

  private fun normalizeErrorResponse(response: MutableHttpResponse<*>): MutableHttpResponse<*> {
    if (response.status == HttpStatus.UNAUTHORIZED) {
      response.headers.set(HttpHeaders.WWW_AUTHENTICATE, "Bearer")
    }

    if (response.code() < HttpStatus.BAD_REQUEST.code || response.body() is ScimError) {
      return response
    }

    val malformedBody =
      response.status == HttpStatus.BAD_REQUEST ||
        response.status == HttpStatus.UNPROCESSABLE_ENTITY
    val status = if (malformedBody) HttpStatus.BAD_REQUEST else response.status
    val scimType = if (malformedBody) "invalidSyntax" else null
    val detail =
      if (status == HttpStatus.INTERNAL_SERVER_ERROR) {
        val serializedErrorId =
          (response.body() as? String)
            ?.let(Jsons::tryDeserialize)
            ?.orElse(null)
            ?.get("errorId")
            ?.takeIf { it.isTextual }
            ?.asText()
        val preservedErrorId =
          try {
            serializedErrorId?.let(UUID::fromString)
          } catch (_: IllegalArgumentException) {
            null
          }
        val errorId =
          preservedErrorId ?: UUID.randomUUID().also {
            log.error { "Normalized SCIM internal server error [errorId: $it]" }
          }
        "Internal server error. Reference: $errorId"
      } else {
        errorDetail(status, malformedBody)
      }

    return response
      .status(status)
      .contentType(SCIM_MEDIA_TYPE)
      .body(
        scimError(
          status = status,
          detail = detail,
          scimType = scimType,
        ),
      )
  }

  private fun errorDetail(
    status: HttpStatus,
    malformedBody: Boolean,
  ): String =
    when {
      malformedBody -> "Invalid request body"
      status == HttpStatus.UNAUTHORIZED -> "Invalid bearer token"
      status == HttpStatus.FORBIDDEN -> "SCIM access is denied"
      status == HttpStatus.NOT_FOUND -> "Resource not found"
      status == HttpStatus.METHOD_NOT_ALLOWED -> "Method not allowed"
      else -> status.reason
    }

  private fun scimErrorResponse(
    status: HttpStatus,
    detail: String,
  ): MutableHttpResponse<ScimError> =
    HttpResponse
      .status<ScimError>(status)
      .contentType(SCIM_MEDIA_TYPE)
      .body(scimError(status = status, detail = detail))
}
