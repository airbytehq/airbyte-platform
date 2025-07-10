/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

@file:JvmName("ApiHelper")

package io.airbyte.server.apis

import datadog.trace.api.Trace
import io.airbyte.commons.server.errors.BadObjectSchemaKnownException
import io.airbyte.commons.server.errors.ConflictException
import io.airbyte.commons.server.errors.IdNotFoundKnownException
import io.airbyte.commons.server.errors.OperationNotAllowedException
import io.airbyte.config.persistence.ConfigNotFoundException
import io.airbyte.metrics.lib.ApmTraceConstants
import io.airbyte.metrics.lib.ApmTraceUtils
import io.airbyte.validation.json.JsonValidationException
import io.github.oshai.kotlinlogging.KotlinLogging
import org.jooq.exception.DataAccessException
import java.io.IOException
import java.util.concurrent.Callable
import io.airbyte.data.ConfigNotFoundException as DataConfigNotFoundException

val logger = KotlinLogging.logger {}

/**
 * Helpers for executing api code to and handling exceptions it might throw.
 */
@Trace(operationName = ApmTraceConstants.ENDPOINT_EXECUTION_OPERATION_NAME)
internal fun <T> execute(call: Callable<T>): T? {
  try {
    return call.call()
  } catch (e: Exception) {
    ApmTraceUtils.recordErrorOnRootSpan(e)

    when (e) {
      is ConfigNotFoundException -> throw IdNotFoundKnownException("Could not find configuration for ${e.type}: ${e.configId}.", e.configId, e)
      is DataConfigNotFoundException -> throw IdNotFoundKnownException("Could not find configuration for ${e.type}: ${e.configId}.", e.configId, e)
      is JsonValidationException -> throw BadObjectSchemaKnownException(
        "The provided configuration does not fulfill the specification. Errors: ${e.message}",
        e,
      )
      is OperationNotAllowedException -> throw e
      is IOException -> throw RuntimeException(e)
      is DataAccessException -> throw ConflictException("Failed to access database. Check the server logs for more information", e)
      else -> {
        logger.error(e) { "Unexpected Exception" }
        throw e
      }
    }
  }
}
