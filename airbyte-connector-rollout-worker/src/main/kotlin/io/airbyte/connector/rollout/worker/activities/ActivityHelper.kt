/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.connector.rollout.worker.activities

import io.airbyte.api.client.model.generated.ConnectorRolloutStrategy
import io.airbyte.config.ConnectorEnumRolloutStrategy
import io.airbyte.connector.rollout.shared.Constants
import io.github.oshai.kotlinlogging.KotlinLogging
import io.temporal.failure.ApplicationFailure
import org.openapitools.client.infrastructure.ClientError
import org.openapitools.client.infrastructure.ClientException
import org.openapitools.client.infrastructure.ServerError

private val logger = KotlinLogging.logger {}

fun handleAirbyteApiClientException(e: ClientException): Nothing {
  val body =
    when (val response = e.response) {
      is ClientError<*> -> {
        logger.error { "ClientException: e.response.body = ${response.body}, message = ${response.message}" }
        response.body.toString()
      }
      is ServerError<*> -> {
        logger.error { "ServerException: e.response.body = ${response.body}, message = ${response.message}" }
        response.body.toString()
      }
      else -> {
        logger.error { "ClientException: e.response = $response" }
        response.toString()
      }
    }

  throw ApplicationFailure.newFailure(body, Constants.AIRBYTE_API_CLIENT_EXCEPTION)
}

fun getRolloutStrategyFromInput(rolloutStrategy: ConnectorEnumRolloutStrategy?): ConnectorRolloutStrategy =
  if (rolloutStrategy == null) ConnectorRolloutStrategy.MANUAL else ConnectorRolloutStrategy.valueOf(rolloutStrategy.toString().uppercase())
