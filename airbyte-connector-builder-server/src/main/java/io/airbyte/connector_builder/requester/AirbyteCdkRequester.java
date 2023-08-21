/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.connector_builder.requester;

import com.fasterxml.jackson.databind.JsonNode;
import io.airbyte.connector_builder.api.model.generated.ResolveManifest;
import io.airbyte.connector_builder.api.model.generated.StreamRead;
import io.airbyte.connector_builder.exceptions.AirbyteCdkInvalidInputException;
import io.airbyte.connector_builder.exceptions.ConnectorBuilderException;
import java.io.IOException;

/**
 * Exposes a way of handling synchronous Connector Builder requests. Blocks until the job completes.
 */
public interface AirbyteCdkRequester {

  ResolveManifest resolveManifest(final JsonNode manifest)
      throws IOException, AirbyteCdkInvalidInputException, ConnectorBuilderException;

  StreamRead readStream(final JsonNode manifest, final JsonNode config, final String stream, final Integer recordLimit)
      throws IOException, AirbyteCdkInvalidInputException, ConnectorBuilderException;

}
