/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.connectorbuilder.handlers

import io.airbyte.connectorbuilder.TracingHelper.addWorkspaceAndProjectIdsToTrace
import io.airbyte.connectorbuilder.api.model.generated.StreamRead
import io.airbyte.connectorbuilder.api.model.generated.StreamReadRequestBody
import io.airbyte.connectorbuilder.exceptions.AirbyteCdkInvalidInputException
import io.airbyte.connectorbuilder.exceptions.ConnectorBuilderException
import io.airbyte.connectorbuilder.requester.AirbyteCdkRequester
import jakarta.inject.Inject
import jakarta.inject.Singleton
import org.slf4j.Logger
import org.slf4j.LoggerFactory
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
        LOGGER.info(
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
        LOGGER.error("Error handling test_read request.", exc)
        throw ConnectorBuilderException("Error handling test_read request.", exc)
      }
    }

    companion object {
      private val LOGGER: Logger = LoggerFactory.getLogger(StreamHandler::class.java)
    }
  }
