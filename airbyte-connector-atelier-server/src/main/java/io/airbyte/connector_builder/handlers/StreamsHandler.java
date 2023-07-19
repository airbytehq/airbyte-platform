/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.connector_builder.handlers;

import io.airbyte.connector_builder.TracingHelper;
import io.airbyte.connector_builder.api.model.generated.StreamsListRead;
import io.airbyte.connector_builder.api.model.generated.StreamsListRequestBody;
import io.airbyte.connector_builder.exceptions.AirbyteCdkInvalidInputException;
import io.airbyte.connector_builder.exceptions.ConnectorBuilderException;
import io.airbyte.connector_builder.requester.AirbyteCdkRequester;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handle /streams requests.
 */
@Singleton
public class StreamsHandler {

  private static final Logger LOGGER = LoggerFactory.getLogger(ResolveManifestHandler.class);

  private final AirbyteCdkRequester requester;

  @Inject
  public StreamsHandler(final AirbyteCdkRequester requester) {
    this.requester = requester;
  }

  /**
   * Handle list_streams.
   */
  public StreamsListRead listStreams(final StreamsListRequestBody streamsListRequestBody)
      throws AirbyteCdkInvalidInputException, ConnectorBuilderException {
    try {
      TracingHelper.addWorkspaceAndProjectIdsToTrace(streamsListRequestBody.getWorkspaceId(), streamsListRequestBody.getProjectId());
      LOGGER.info("Handling list_streams request for workspace '{}' with project ID = '{}'",
          streamsListRequestBody.getWorkspaceId(), streamsListRequestBody.getProjectId());
      return this.requester.listStreams(streamsListRequestBody.getManifest(), streamsListRequestBody.getConfig());
    } catch (final IOException exc) {
      LOGGER.error("Error handling list_streams request.", exc);
      throw new ConnectorBuilderException("Error handling list_streams request.", exc);
    }
  }

}
