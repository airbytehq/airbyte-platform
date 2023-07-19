/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.connector_builder.handlers;

import io.airbyte.connector_builder.TracingHelper;
import io.airbyte.connector_builder.api.model.generated.StreamRead;
import io.airbyte.connector_builder.api.model.generated.StreamReadRequestBody;
import io.airbyte.connector_builder.exceptions.AirbyteCdkInvalidInputException;
import io.airbyte.connector_builder.exceptions.ConnectorBuilderException;
import io.airbyte.connector_builder.requester.AirbyteCdkRequester;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handle /stream requests.
 */
@Singleton
public class StreamHandler {

  private static final Logger LOGGER = LoggerFactory.getLogger(StreamHandler.class);

  private final AirbyteCdkRequester requester;

  @Inject
  public StreamHandler(
                       final AirbyteCdkRequester requester) {
    this.requester = requester;
  }

  /**
   * Use the requester to send the test_read request to the CDK.
   */
  public StreamRead readStream(
                               final StreamReadRequestBody streamReadRequestBody)
      throws AirbyteCdkInvalidInputException, ConnectorBuilderException {
    try {
      TracingHelper.addWorkspaceAndProjectIdsToTrace(streamReadRequestBody.getWorkspaceId(), streamReadRequestBody.getProjectId());
      LOGGER.info("Handling test_read request for workspace '{}' with project ID = '{}'",
          streamReadRequestBody.getWorkspaceId(), streamReadRequestBody.getProjectId());
      return this.requester.readStream(
          streamReadRequestBody.getManifest(),
          streamReadRequestBody.getConfig(),
          streamReadRequestBody.getStream(),
          streamReadRequestBody.getRecordLimit());
    } catch (final IOException exc) {
      LOGGER.error("Error handling test_read request.", exc);
      throw new ConnectorBuilderException("Error handling test_read request.", exc);
    }
  }

}
