/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

@file:Suppress("PackageName")

package io.airbyte.server.apis.publicapi.errorHandlers

import io.airbyte.commons.json.Jsons
import io.airbyte.commons.server.errors.ValueConflictKnownException
import io.airbyte.commons.server.errors.problems.AbstractThrowableProblem
import io.airbyte.commons.server.errors.problems.BadRequestProblem
import io.airbyte.commons.server.errors.problems.ConflictProblem
import io.airbyte.commons.server.errors.problems.InvalidApiKeyProblem
import io.airbyte.commons.server.errors.problems.OAuthCallbackFailureProblem
import io.airbyte.commons.server.errors.problems.ResourceNotFoundProblem
import io.airbyte.commons.server.errors.problems.SyncConflictProblem
import io.airbyte.commons.server.errors.problems.UnexpectedProblem
import io.airbyte.commons.server.errors.problems.UnprocessableEntityProblem
import io.airbyte.config.persistence.ConfigNotFoundException
import io.airbyte.public_api.model.generated.ConnectionCreateRequest
import io.airbyte.server.apis.publicapi.constants.MESSAGE
import io.airbyte.server.apis.publicapi.exceptions.OAuthCallbackException
import io.airbyte.validation.json.JsonValidationException
import io.micronaut.http.HttpResponse
import io.micronaut.http.HttpStatus
import java.io.IOException

const val DEFAULT_CONFLICT_MESSAGE = "Could not fulfill request"
const val DEFAULT_INTERNAL_SERVER_ERROR_MESSAGE =
  "An unexpected problem has occurred. If this is an error that needs to be addressed, please submit a pull request or github issue."
const val DEFAULT_UNPROCESSABLE_ENTITY_MESSAGE = "The body of the request was not understood"
const val JOB_NOT_RUNNING_MESSAGE = "Job is not currently running"

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
      HttpStatus.NOT_FOUND -> throw ResourceNotFoundProblem(resourceId)
      HttpStatus.CONFLICT -> {
        val message: String =
          response.getBody(MutableMap::class.java)
            .orElseGet { mutableMapOf(Pair(MESSAGE, DEFAULT_CONFLICT_MESSAGE)) }
            .getOrDefault(MESSAGE, DEFAULT_CONFLICT_MESSAGE).toString()
        throw SyncConflictProblem(message)
      }

      HttpStatus.UNAUTHORIZED -> throw InvalidApiKeyProblem()
      HttpStatus.UNPROCESSABLE_ENTITY -> {
        val message: String =
          response.getBody(MutableMap::class.java)
            .orElseGet { mutableMapOf(Pair(MESSAGE, DEFAULT_UNPROCESSABLE_ENTITY_MESSAGE)) }
            .getOrDefault(MESSAGE, DEFAULT_UNPROCESSABLE_ENTITY_MESSAGE).toString()
        // Exclude the part of a schema validation message that's ugly if it's there
        throw UnprocessableEntityProblem(message.split("\nSchema".toRegex()).dropLastWhile { it.isEmpty() }.toTypedArray()[0])
      }

      else -> passThroughBadStatusCode(response)
    }
  }

  /**
   * Maps handler exceptions to problems.
   *
   * @param throwable throwable from handler
   * @param resourceId resource ID passed in with the request
   */
  fun handleError(
    throwable: Throwable,
    resourceId: String?,
  ) {
    when (throwable) {
      is ConfigNotFoundException -> throw ResourceNotFoundProblem(resourceId)
      is ValueConflictKnownException -> {
        val message = Jsons.serialize(mapOf(MESSAGE to (throwable.message ?: DEFAULT_CONFLICT_MESSAGE)))
        throw SyncConflictProblem(message)
      }

      is IllegalStateException -> {
        // Many of the job failures share this exception type.
        // If a job has already been canceled it throws this exception with a cryptic message.
        val isFailedCancellation = throwable.message?.contains("Failed to cancel")
        val message =
          if (isFailedCancellation == true) {
            Jsons.serialize(mapOf(MESSAGE to JOB_NOT_RUNNING_MESSAGE))
          } else {
            Jsons.serialize(mapOf(MESSAGE to (throwable.message ?: DEFAULT_CONFLICT_MESSAGE)))
          }
        throw ConflictProblem(message)
      }

      is JsonValidationException -> {
        val error = throwable.message?.substringBefore("\nSchema:") ?: DEFAULT_UNPROCESSABLE_ENTITY_MESSAGE
        val message = Jsons.serialize(mapOf(MESSAGE to error))
        throw UnprocessableEntityProblem(message)
      }

      is IOException -> {
        val message = Jsons.serialize(mapOf(MESSAGE to (throwable.message ?: DEFAULT_UNPROCESSABLE_ENTITY_MESSAGE)))
        throw UnprocessableEntityProblem(message)
      }

      is OAuthCallbackException -> {
        throw OAuthCallbackFailureProblem(throwable.message)
      }

      else -> {
        val message = throwable.message ?: DEFAULT_INTERNAL_SERVER_ERROR_MESSAGE
        if (message.contains("Could not find job with id")) {
          throw ConflictProblem(JOB_NOT_RUNNING_MESSAGE)
        } else {
          throw UnexpectedProblem(HttpStatus.INTERNAL_SERVER_ERROR, message)
        }
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
  ): AbstractThrowableProblem {
    return when (throwable) {
      is JsonValidationException -> UnprocessableEntityProblem(throwable.message)
      is IOException -> UnexpectedProblem(HttpStatus.INTERNAL_SERVER_ERROR, throwable.message)
      is ConfigNotFoundException ->
        if (throwable.toString().contains(connectionCreate.sourceId.toString())) {
          ResourceNotFoundProblem(connectionCreate.sourceId.toString())
        } else if (throwable.toString().contains(connectionCreate.destinationId.toString())) {
          ResourceNotFoundProblem(connectionCreate.destinationId.toString())
        } else {
          UnexpectedProblem(HttpStatus.INTERNAL_SERVER_ERROR)
        }
      else -> UnexpectedProblem(HttpStatus.INTERNAL_SERVER_ERROR)
    }
  }

  /**
   * Throws an UnexpectedProblem if the response contains an error code 400 or above.
   *
   * @param response HttpResponse, most likely from the config api
   */
  private fun passThroughBadStatusCode(response: HttpResponse<*>) {
    if (response.status.code >= HttpStatus.INTERNAL_SERVER_ERROR.code) {
      throw UnexpectedProblem(response.status, response.body()?.toString())
    } else if (response.status.code >= HttpStatus.BAD_REQUEST.code) {
      throw BadRequestProblem("${response.status.reason}: ${response.body}")
    }
  }
}
