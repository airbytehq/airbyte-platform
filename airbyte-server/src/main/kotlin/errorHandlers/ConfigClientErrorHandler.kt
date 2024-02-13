/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

@file:Suppress("PackageName")

package io.airbyte.api.server.errorHandlers

import io.airbyte.airbyte_api.model.generated.ConnectionCreateRequest
import io.airbyte.api.server.constants.MESSAGE
import io.airbyte.api.server.problems.InvalidApiKeyProblem
import io.airbyte.api.server.problems.ResourceNotFoundProblem
import io.airbyte.api.server.problems.SyncConflictProblem
import io.airbyte.api.server.problems.UnexpectedProblem
import io.airbyte.api.server.problems.UnprocessableEntityProblem
import io.airbyte.commons.json.Jsons
import io.airbyte.commons.server.errors.problems.AbstractThrowableProblem
import io.airbyte.config.persistence.ConfigNotFoundException
import io.airbyte.validation.json.JsonValidationException
import io.micronaut.http.HttpResponse
import io.micronaut.http.HttpStatus
import java.io.IOException

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
        val couldNotFulfillRequest = "Could not fulfill request"
        val message: String =
          response.getBody(MutableMap::class.java)
            .orElseGet { mutableMapOf(Pair(MESSAGE, couldNotFulfillRequest)) }
            .getOrDefault(MESSAGE, couldNotFulfillRequest).toString()
        throw SyncConflictProblem(message)
      }

      HttpStatus.UNAUTHORIZED -> throw InvalidApiKeyProblem()
      HttpStatus.UNPROCESSABLE_ENTITY -> {
        val defaultErrorMessage = "The body of the request was not understood"
        val message: String =
          response.getBody(MutableMap::class.java)
            .orElseGet { mutableMapOf(Pair(MESSAGE, defaultErrorMessage)) }
            .getOrDefault(MESSAGE, defaultErrorMessage).toString()
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
      is SyncConflictProblem -> {
        val couldNotFulfillRequest = "Could not fulfill request"
        val message =
          Jsons.deserialize(throwable.message, MutableMap::class.java).orEmpty()
            .getOrDefault(MESSAGE, couldNotFulfillRequest).toString()
        throw SyncConflictProblem(message)
      }
      is JsonValidationException, is IOException -> {
        val defaultErrorMessage = "The body of the request was not understood"
        val message: String =
          Jsons.deserialize(throwable.message, MutableMap::class.java).orEmpty()
            .getOrDefault(MESSAGE, defaultErrorMessage).toString()
        // Exclude the part of a schema validation message that's ugly if it's there
        throw UnprocessableEntityProblem(message.split("\nSchema".toRegex()).dropLastWhile { it.isEmpty() }.toTypedArray()[0])
      }
      else -> throw UnexpectedProblem(HttpStatus.INTERNAL_SERVER_ERROR)
    }
  }

  /**
   * Maps sync exceptions to problems.
   *
   * @param throwable throwable
   * @param resourceId resource ID passed in with the request
   */
  fun handleSyncError(
    throwable: Throwable,
    resourceId: String?,
  ) {
    when (throwable) {
      is ConfigNotFoundException -> throw ResourceNotFoundProblem(resourceId)
      is SyncConflictProblem -> {
        val couldNotFulfillRequest = "Could not fulfill request"
        val message =
          Jsons.deserialize(throwable.message, MutableMap::class.java).orEmpty()
            .getOrDefault(MESSAGE, couldNotFulfillRequest).toString()
        throw SyncConflictProblem(message)
      }
      is JsonValidationException, is IOException -> {
        val defaultErrorMessage = "The body of the request was not understood"
        val message: String =
          Jsons.deserialize(throwable.message, MutableMap::class.java).orEmpty()
            .getOrDefault(MESSAGE, defaultErrorMessage).toString()
        // Exclude the part of a schema validation message that's ugly if it's there
        throw UnprocessableEntityProblem(message.split("\nSchema".toRegex()).dropLastWhile { it.isEmpty() }.toTypedArray()[0])
      }
      else -> throw UnexpectedProblem(HttpStatus.INTERNAL_SERVER_ERROR)
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
    if (response.status.code >= HttpStatus.BAD_REQUEST.code) {
      throw UnexpectedProblem(response.status)
    }
  }
}
