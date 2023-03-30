/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.connector_builder.requester;

import com.fasterxml.jackson.databind.JsonNode;
import io.airbyte.connector_builder.api.model.generated.ResolveManifest;
import io.airbyte.connector_builder.api.model.generated.StreamsListRead;
import io.airbyte.connector_builder.exceptions.AirbyteCdkInvalidInputException;
import io.airbyte.connector_builder.exceptions.CdkProcessException;
import io.airbyte.connector_builder.exceptions.ConnectorBuilderException;
import java.io.IOException;

/**
 * Exposes a way of handling synchronous Connector Builder requests. Blocks until the job completes.
 */
public interface AirbyteCdkRequester {

  ResolveManifest resolveManifest(final JsonNode manifest)
      throws IOException, AirbyteCdkInvalidInputException, ConnectorBuilderException;

  StreamsListRead listStreams(final JsonNode manifest, JsonNode config) throws IOException, AirbyteCdkInvalidInputException, CdkProcessException;

}
