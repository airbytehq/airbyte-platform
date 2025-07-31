/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.helpers

import io.airbyte.api.client.AirbyteApiClient
import io.airbyte.api.client.model.generated.AttemptStats
import io.airbyte.api.client.model.generated.GetAttemptStatsRequestBody
import io.airbyte.commons.temporal.exception.RetryableException
import io.micronaut.http.HttpStatus
import jakarta.inject.Singleton
import org.openapitools.client.infrastructure.ClientException
import java.io.IOException

/**
 * Composes all the business and request logic for checking progress of a run.
 */
@Singleton
class ProgressChecker(
  private val airbyteApiClient: AirbyteApiClient,
  private val predicate: ProgressCheckerPredicates,
) {
  /**
   * Fetches an attempt stats and evaluates it against a given progress predicate.
   *
   * @param jobId Job id for run in question
   * @param attemptNo Attempt number for run in question — 0-based
   * @return whether we made progress. Returns false if we failed to check.
   * @throws IOException Rethrows the OkHttp execute method exception
   */
  @Throws(IOException::class)
  fun check(
    jobId: Long,
    attemptNo: Int,
  ): Boolean {
    val resp = fetchAttemptStats(jobId, attemptNo)

    return resp
      ?.let { stats: AttemptStats -> predicate.test(stats) }
      ?: false
  }

  @Throws(RetryableException::class)
  private fun fetchAttemptStats(
    jobId: Long,
    attemptNo: Int,
  ): AttemptStats? {
    val req = GetAttemptStatsRequestBody(jobId, attemptNo)

    var resp: AttemptStats?

    try {
      resp = airbyteApiClient.attemptApi.getAttemptCombinedStats(req)
    } catch (e: ClientException) {
      // Retry unexpected 4xx/5xx status codes.
      // 404 is an expected status code and should not be retried.
      if (e.statusCode != HttpStatus.NOT_FOUND.code) {
        throw RetryableException(e)
      }
      resp = null
    } catch (e: IOException) {
      throw RetryableException(e)
    }

    return resp
  }
}
