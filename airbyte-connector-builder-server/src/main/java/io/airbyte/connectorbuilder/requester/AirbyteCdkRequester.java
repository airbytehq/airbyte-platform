/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.connectorbuilder.requester;

import com.fasterxml.jackson.databind.JsonNode;
import io.airbyte.connectorbuilder.api.model.generated.ResolveManifest;
import io.airbyte.connectorbuilder.api.model.generated.StreamRead;
import io.airbyte.connectorbuilder.exceptions.AirbyteCdkInvalidInputException;
import io.airbyte.connectorbuilder.exceptions.ConnectorBuilderException;
import java.io.IOException;
import java.util.List;

/**
 * Exposes a way of handling synchronous Connector Builder requests. Blocks until the job completes.
 */
public interface AirbyteCdkRequester {

  ResolveManifest resolveManifest(final JsonNode manifest)
      throws IOException, AirbyteCdkInvalidInputException, ConnectorBuilderException;

  ResolveManifest fullResolveManifest(final JsonNode manifest, final JsonNode config, final Integer streamLimit)
      throws IOException, AirbyteCdkInvalidInputException, ConnectorBuilderException;

  StreamRead readStream(final JsonNode manifest,
                        final String customComponentsCode,
                        final JsonNode config,
                        final List<JsonNode> state,
                        final String stream,
                        final Integer recordLimit,
                        final Integer pageLimit,
                        final Integer sliceLimit)
      throws IOException, AirbyteCdkInvalidInputException, ConnectorBuilderException;

}
