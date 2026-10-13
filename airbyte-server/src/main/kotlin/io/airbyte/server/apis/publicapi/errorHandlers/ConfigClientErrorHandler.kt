/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

@file:Suppress("PackageName")

package io.airbyte.server.apis.publicapi.errorHandlers

import io.airbyte.api.problems.AbstractThrowableProblem
import io.airbyte.api.problems.model.generated.ProblemResourceData
import io.airbyte.api.problems.throwable.generated.BadRequestProblem
import io.airbyte.api.problems.throwable.generated.InvalidApiKeyProblem
import io.airbyte.api.problems.throwable.generated.OAuthCallbackFailureProblem
import io.airbyte.api.problems.throwable.generated.ResourceNotFoundProblem
import io.airbyte.api.problems.throwable.generated.StateConflictProblem
import io.airbyte.api.problems.throwable.generated.TryAgainLaterConflictProblem
import io.airbyte.api.problems.throwable.generated.UnexpectedProblem
import io.airbyte.api.problems.throwable.generated.UnprocessableEntityProblem
import io.airbyte.commons.server.errors.ConflictException
import io.airbyte.commons.server.errors.ValueConflictKnownException
import io.airbyte.config.persistence.ConfigNotFoundException
import io.airbyte.publicApi.server.generated.models.ConnectionCreateRequest
import io.airbyte.server.apis.publicapi.constants.MESSAGE
import io.airbyte.server.apis.publicapi.exceptions.OAuthCallbackException
import io.airbyte.validation.json.JsonValidationException
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.http.HttpResponse
import io.micronaut.http.HttpStatus
import java.io.IOException

private val log = KotlinLogging.logger {}

const val JOB_NOT_RUNNING_DETAIL = "The job is not running or could not be found."

private const val JSON_SCHEMA_VALIDATION_PREFIX = "json schema validation failed when comparing the data to the json schema."

/** Formats platform-authored validation text, omitting the schema dump and preserving the default for empty messages. */
internal fun toDetail(message: String?): String? {
  val text = message?.substringBefore("\nSchema")?.trim()?.takeIf { it.isNotEmpty() } ?: return null
  val errors =
    text
      .removePrefix(JSON_SCHEMA_VALIDATION_PREFIX)
      .trim()
      .removePrefix("Errors:")
      .trim()
  if (errors.isEmpty()) return null
  val sentence = if (errors != text) "The request body failed validation: $errors" else text
  return if (sentence.endsWith(".")) sentence else "$sentence."
}

/**
 * Maps config API client response statuses to problems.
 */
object ConfigClientErrorHandler {
  /**
   * Maps config API client response statuses to problems.
   *
   * @param response response from ConfigApiClient
   * @param resourceId resource ID passed in with the request
   */
  fun handleError(
    response: HttpResponse<*>,
    resourceId: String?,
  ) {
    when (response.status) {
      HttpStatus.NOT_FOUND -> throw ResourceNotFoundProblem(ProblemResourceData().resourceId(resourceId))
      HttpStatus.CONFLICT -> throw TryAgainLaterConflictProblem(toDetail(upstreamMessage(response)), null)
      HttpStatus.UNAUTHORIZED -> throw InvalidApiKeyProblem()
      HttpStatus.UNPROCESSABLE_ENTITY -> throw UnprocessableEntityProblem(toDetail(upstreamMessage(response)), null)

      else -> passThroughBadStatusCode(response)
    }
  }

  /**
   * Maps handler exceptions to problems.
   *
   * @param throwable throwable from handler
   */
  fun handleError(throwable: Throwable) {
    when (throwable) {
      is ConfigNotFoundException -> throw ResourceNotFoundProblem(ProblemResourceData().resourceType(throwable.type).resourceId(throwable.configId))
      is io.airbyte.data.ConfigNotFoundException -> throw ResourceNotFoundProblem(
        ProblemResourceData().resourceType(throwable.type).resourceId(throwable.configId),
      )

      is ValueConflictKnownException -> throw TryAgainLaterConflictProblem(toDetail(throwable.message), null)
      is ConflictException -> throw StateConflictProblem(toDetail(throwable.message), null)

      is IllegalStateException -> {
        if (throwable.message?.contains("Failed to cancel") == true) {
          throw StateConflictProblem(JOB_NOT_RUNNING_DETAIL, null)
        }
        throw StateConflictProblem()
      }

      is JsonValidationException -> throw UnprocessableEntityProblem(toDetail(throwable.message), null)

      is IOException -> {
        log.error(throwable) { "I/O failure while handling a Public API request" }
        throw UnprocessableEntityProblem()
      }

      is OAuthCallbackException -> {
        log.error(throwable) { "OAuth callback failed" }
        throw OAuthCallbackFailureProblem()
      }

      is AbstractThrowableProblem -> throw throwable

      else -> {
        if (throwable.message?.contains("Could not find job with id") == true) {
          throw StateConflictProblem(JOB_NOT_RUNNING_DETAIL, null)
        }
        log.error(throwable) { "Unexpected failure while handling a Public API request" }
        throw UnexpectedProblem()
      }
    }
  }

  /**
   * Maps connection Create exception to problems
   *
   * @param throwable  failure
   * @param connectionCreate connection create inputs passed in with the request
   */
  fun handleCreateConnectionError(
    throwable: Throwable,
    connectionCreate: ConnectionCreateRequest,
  ): AbstractThrowableProblem =
    when (throwable) {
      is JsonValidationException -> {
        log.error(throwable) { "Failed to create connection" }
        UnexpectedProblem(toDetail(throwable.message), null)
      }
      is IOException -> {
        log.error(throwable) { "Failed to create connection" }
        UnexpectedProblem()
      }
      is ConfigNotFoundException ->
        if (throwable.toString().contains(connectionCreate.sourceId.toString())) {
          ResourceNotFoundProblem(ProblemResourceData().resourceType("source").resourceId(connectionCreate.sourceId.toString()))
        } else if (throwable.toString().contains(connectionCreate.destinationId.toString())) {
          ResourceNotFoundProblem(ProblemResourceData().resourceType("destination").resourceId(connectionCreate.destinationId.toString()))
        } else {
          UnexpectedProblem()
        }
      is AbstractThrowableProblem -> {
        // No need to do anything to these, they're ready to throw
        throwable
      }
      else -> UnexpectedProblem()
    }

  /**
   * Throws an UnexpectedProblem if the response contains an error code 400 or above.
   *
   * @param response HttpResponse, most likely from the config api
   */
  private fun passThroughBadStatusCode(response: HttpResponse<*>) {
    if (response.status.code >= HttpStatus.INTERNAL_SERVER_ERROR.code) {
      log.error { "Upstream call failed with status ${response.status.code}: ${response.body}" }
      throw UnexpectedProblem()
    } else if (response.status.code >= HttpStatus.BAD_REQUEST.code) {
      log.warn { "Upstream call rejected with status ${response.status.code}: ${response.body}" }
      throw BadRequestProblem()
    }
  }

  private fun upstreamMessage(response: HttpResponse<*>): String? =
    response
      .getBody(MutableMap::class.java)
      .orElse(null)
      ?.get(MESSAGE)
      ?.toString()
}
