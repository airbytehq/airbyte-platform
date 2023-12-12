/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.api.server.apiTracking

import io.airbyte.analytics.TrackingClient
import io.micronaut.http.HttpStatus
import jakarta.inject.Singleton
import org.zalando.problem.AbstractThrowableProblem
import java.util.Optional
import java.util.UUID
import java.util.concurrent.Callable
import javax.ws.rs.core.Response

/**
 * Helper for segment tracking used by the public-api server.
 */
@Singleton
class TrackingHelper(private val trackingClient: TrackingClient) {
  private fun trackSuccess(
    endpointPath: String,
    httpOperation: String,
    userId: UUID,
    workspaceId: Optional<UUID>,
  ) {
    val statusCode = Response.Status.OK.statusCode
    track(
      userId,
      endpointPath,
      httpOperation,
      statusCode,
      workspaceId,
    )
  }

  /**
   * Track success calls.
   */
  fun trackSuccess(
    endpointPath: String?,
    httpOperation: String?,
    userId: UUID?,
  ) {
    trackSuccess(endpointPath!!, httpOperation!!, userId!!, Optional.empty())
  }

  /**
   * Track success calls with workspace id.
   */
  fun trackSuccess(
    endpointPath: String?,
    httpOperation: String?,
    userId: UUID?,
    workspaceId: UUID,
  ) {
    trackSuccess(endpointPath!!, httpOperation!!, userId!!, Optional.of(workspaceId))
  }

  /**
   * Gets the status code from the problem if there was one thrown.
   */
  fun trackFailuresIfAny(
    endpointPath: String?,
    httpOperation: String?,
    userId: UUID,
    e: Exception?,
  ) {
    var statusCode = 0
    if (e is AbstractThrowableProblem) {
      statusCode = (e as AbstractThrowableProblem?)?.status?.statusCode ?: 500
    } else if (e != null) {
      // also contains InvalidConsentUrlProblem
      statusCode = HttpStatus.INTERNAL_SERVER_ERROR.code
    }

    // only track if there was a failure
    if (statusCode != 0) {
      track(
        userId,
        endpointPath,
        httpOperation,
        statusCode,
        Optional.empty(),
      )
    }
  }

  /**
   * Tracks the problems thrown from the function being called. The function is usually a call to the
   * config or cloud api servers. This tracker DOES NOT track successes.
   *
   * @param function usually a call to the config or cloud api
   * @param endpoint the endpoint being called to be tracked into segment
   * @param httpOperation the http operation to be tracked into segment e.g. GET, POST, DELETE
   * @param userId the id of the user we want to track
   * @return the output of the function. Will send segment tracking event for any exceptions caught
   * from the function.
   */
  fun <T> callWithTracker(
    function: Callable<T>,
    endpoint: String?,
    httpOperation: String?,
    userId: UUID,
  ): T {
    return try {
      function.call()
    } catch (e: Exception) {
      trackFailuresIfAny(endpoint, httpOperation, userId, e)
      throw e
    }
  }

  fun track(
    userId: UUID,
    endpointPath: String?,
    httpOperation: String?,
    httpStatusCode: Int,
    workspaceId: Optional<UUID>,
  ) {
    val payload =
      mutableMapOf(
        Pair(USER_ID, userId),
        Pair(ENDPOINT, endpointPath),
        Pair(OPERATION, httpOperation),
        Pair(STATUS_CODE, httpStatusCode),
      )
    if (workspaceId.isPresent) {
      payload[WORKSPACE] = workspaceId.get().toString()
    }
    trackingClient.track(
      userId,
      AIRBYTE_API_CALL,
      payload as Map<String?, Any?>?,
    )
  }

  companion object {
    // Operation names
    const val AIRBYTE_API_CALL = "Airbyte_API_Call"

    const val USER_ID = "user_id"
    const val ENDPOINT = "endpoint"
    const val OPERATION = "operation"
    const val STATUS_CODE = "status_code"
    const val WORKSPACE = "workspace"
  }
}
