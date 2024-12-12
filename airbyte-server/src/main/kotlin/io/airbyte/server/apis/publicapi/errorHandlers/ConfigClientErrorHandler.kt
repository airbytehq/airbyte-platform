/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

@file:Suppress("PackageName")

package io.airbyte.server.apis.publicapi.errorHandlers

import io.airbyte.api.problems.AbstractThrowableProblem
import io.airbyte.api.problems.model.generated.ProblemMessageData
import io.airbyte.api.problems.model.generated.ProblemResourceData
import io.airbyte.api.problems.throwable.generated.BadRequestProblem
import io.airbyte.api.problems.throwable.generated.InvalidApiKeyProblem
import io.airbyte.api.problems.throwable.generated.OAuthCallbackFailureProblem
import io.airbyte.api.problems.throwable.generated.ResourceNotFoundProblem
import io.airbyte.api.problems.throwable.generated.StateConflictProblem
import io.airbyte.api.problems.throwable.generated.TryAgainLaterConflictProblem
import io.airbyte.api.problems.throwable.generated.UnexpectedProblem
import io.airbyte.api.problems.throwable.generated.UnprocessableEntityProblem
import io.airbyte.commons.server.errors.ValueConflictKnownException
import io.airbyte.config.persistence.ConfigNotFoundException
import io.airbyte.publicApi.server.generated.models.ConnectionCreateRequest
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
      HttpStatus.NOT_FOUND -> throw ResourceNotFoundProblem(ProblemResourceData().resourceId(resourceId))
      HttpStatus.CONFLICT -> {
        val message: String =
          response.getBody(MutableMap::class.java)
            .orElseGet { mutableMapOf(Pair(MESSAGE, DEFAULT_CONFLICT_MESSAGE)) }
            .getOrDefault(MESSAGE, DEFAULT_CONFLICT_MESSAGE).toString()
        throw TryAgainLaterConflictProblem(ProblemMessageData().message(message))
      }

      HttpStatus.UNAUTHORIZED -> throw InvalidApiKeyProblem()
      HttpStatus.UNPROCESSABLE_ENTITY -> {
        val message: String =
          response.getBody(MutableMap::class.java)
            .orElseGet { mutableMapOf(Pair(MESSAGE, DEFAULT_UNPROCESSABLE_ENTITY_MESSAGE)) }
            .getOrDefault(MESSAGE, DEFAULT_UNPROCESSABLE_ENTITY_MESSAGE).toString()
        // Exclude the part of a schema validation message that's ugly if it's there
        throw UnprocessableEntityProblem(
          ProblemMessageData()
            .message(message.split("\nSchema".toRegex()).dropLastWhile { it.isEmpty() }.toTypedArray()[0]),
        )
      }

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
      is io.airbyte.data.exceptions.ConfigNotFoundException -> throw ResourceNotFoundProblem(
        ProblemResourceData().resourceType(throwable.type).resourceId(throwable.configId),
      )

      is ValueConflictKnownException -> {
        val message = throwable.message ?: DEFAULT_CONFLICT_MESSAGE
        throw TryAgainLaterConflictProblem(ProblemMessageData().message(message))
      }

      is IllegalStateException -> {
        // Many of the job failures share this exception type.
        // If a job has already been canceled it throws this exception with a cryptic message.
        val isFailedCancellation = throwable.message?.contains("Failed to cancel")
        val message =
          if (isFailedCancellation == true) {
            JOB_NOT_RUNNING_MESSAGE
          } else {
            DEFAULT_CONFLICT_MESSAGE
          }
        throw StateConflictProblem(ProblemMessageData().message(message))
      }

      is JsonValidationException -> {
        val error = throwable.message?.substringBefore("\nSchema:") ?: DEFAULT_UNPROCESSABLE_ENTITY_MESSAGE
        throw UnprocessableEntityProblem(ProblemMessageData().message(error))
      }

      is IOException -> {
        val message = throwable.message ?: DEFAULT_UNPROCESSABLE_ENTITY_MESSAGE
        throw UnprocessableEntityProblem(ProblemMessageData().message(message))
      }

      is OAuthCallbackException -> {
        throw OAuthCallbackFailureProblem(ProblemMessageData().message(throwable.message))
      }

      is AbstractThrowableProblem -> throw throwable

      else -> {
        val message = throwable.message ?: DEFAULT_INTERNAL_SERVER_ERROR_MESSAGE
        if (message.contains("Could not find job with id")) {
          throw StateConflictProblem(ProblemMessageData().message(JOB_NOT_RUNNING_MESSAGE))
        } else {
          throw UnexpectedProblem(ProblemMessageData().message(message))
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
      is JsonValidationException -> UnexpectedProblem(ProblemMessageData().message(throwable.message))
      is IOException -> UnexpectedProblem(ProblemMessageData().message(throwable.message))
      is ConfigNotFoundException ->
        if (throwable.toString().contains(connectionCreate.sourceId.toString())) {
          ResourceNotFoundProblem(ProblemResourceData().resourceType("source").resourceId(connectionCreate.sourceId.toString()))
        } else if (throwable.toString().contains(connectionCreate.destinationId.toString())) {
          ResourceNotFoundProblem(ProblemResourceData().resourceType("destination").resourceId(connectionCreate.destinationId.toString()))
        } else {
          UnexpectedProblem()
        }
      else -> UnexpectedProblem()
    }
  }

  /**
   * Throws an UnexpectedProblem if the response contains an error code 400 or above.
   *
   * @param response HttpResponse, most likely from the config api
   */
  private fun passThroughBadStatusCode(response: HttpResponse<*>) {
    if (response.status.code >= HttpStatus.INTERNAL_SERVER_ERROR.code) {
      // TODO pass status
      throw UnexpectedProblem(ProblemMessageData().message(response.body.toString()))
//      throw UnexpectedProblem(response.status, response.body()?.toString())
    } else if (response.status.code >= HttpStatus.BAD_REQUEST.code) {
      throw BadRequestProblem(ProblemMessageData().message("${response.status.reason}: ${response.body}"))
    }
  }
}
