/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.connectorbuilder.handlers

import io.airbyte.commons.server.builder.exceptions.ConnectorBuilderException
import io.airbyte.connectorbuilder.TracingHelper.addWorkspaceAndProjectIdsToTrace
import io.airbyte.connectorbuilder.api.model.generated.StreamRead
import io.airbyte.connectorbuilder.api.model.generated.StreamReadRequestBody
import io.airbyte.connectorbuilder.exceptions.AirbyteCdkInvalidInputException
import io.airbyte.connectorbuilder.requester.AirbyteCdkRequester
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Inject
import jakarta.inject.Singleton
import java.io.IOException

/**
 * Handle /stream requests.
 */
@Singleton
open class StreamHandler
  @Inject
  constructor(
    private val requester: AirbyteCdkRequester,
  ) {
    /**
     * Use the requester to send the test_read request to the CDK.
     */
    @Throws(AirbyteCdkInvalidInputException::class, ConnectorBuilderException::class)
    fun readStream(streamReadRequestBody: StreamReadRequestBody): StreamRead {
      try {
        addWorkspaceAndProjectIdsToTrace(streamReadRequestBody.workspaceId, streamReadRequestBody.projectId)
        log.info(
          "Handling test_read request for workspace '{}' with project ID = '{}'",
          streamReadRequestBody.workspaceId,
          streamReadRequestBody.projectId,
        )
        return requester.readStream(
          streamReadRequestBody.manifest,
          streamReadRequestBody.customComponentsCode,
          streamReadRequestBody.config,
          streamReadRequestBody.state,
          streamReadRequestBody.stream,
          streamReadRequestBody.recordLimit,
          streamReadRequestBody.pageLimit,
          streamReadRequestBody.sliceLimit,
        )
      } catch (exc: IOException) {
        log.error(exc) { "Error handling test_read request." }
        throw ConnectorBuilderException("Error handling test_read request.", exc)
      }
    }

    companion object {
      private val log = KotlinLogging.logger {}
    }
  }
