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
import io.micronaut.http.HttpResponse
import io.micronaut.http.HttpStatus

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
   * Maps config API client response statuses to problems during connection creation.
   *
   * @param response response from ConfigApiClient
   * @param connectionCreate connection create inputs passed in with the request
   */
  fun handleCreateConnectionError(
    response: HttpResponse<*>,
    connectionCreate: ConnectionCreateRequest,
  ) {
    when (response.status) {
      HttpStatus.NOT_FOUND -> {
        if (response.body.toString().contains(connectionCreate.getSourceId().toString())) {
          throw ResourceNotFoundProblem(connectionCreate.getSourceId().toString())
        } else if (response.body.toString().contains(connectionCreate.getDestinationId().toString())) {
          throw ResourceNotFoundProblem(connectionCreate.getDestinationId().toString())
        }
        throw UnprocessableEntityProblem()
      }

      HttpStatus.UNAUTHORIZED -> throw InvalidApiKeyProblem()
      HttpStatus.UNPROCESSABLE_ENTITY -> throw UnprocessableEntityProblem()
      else -> passThroughBadStatusCode(response)
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
