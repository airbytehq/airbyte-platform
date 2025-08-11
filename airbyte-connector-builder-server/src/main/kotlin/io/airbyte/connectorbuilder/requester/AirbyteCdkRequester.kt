/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.connectorbuilder.requester

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.commons.server.builder.exceptions.ConnectorBuilderException
import io.airbyte.connectorbuilder.api.model.generated.ResolveManifest
import io.airbyte.connectorbuilder.api.model.generated.StreamRead
import io.airbyte.connectorbuilder.exceptions.AirbyteCdkInvalidInputException
import java.io.IOException

/**
 * Exposes a way of handling synchronous Connector Builder requests. Blocks until the job completes.
 */
interface AirbyteCdkRequester {
  @Throws(IOException::class, AirbyteCdkInvalidInputException::class, ConnectorBuilderException::class)
  fun resolveManifest(manifest: JsonNode): ResolveManifest

  @Throws(IOException::class, AirbyteCdkInvalidInputException::class, ConnectorBuilderException::class)
  fun fullResolveManifest(
    manifest: JsonNode,
    config: JsonNode,
    streamLimit: Int?,
  ): ResolveManifest

  @Throws(IOException::class, AirbyteCdkInvalidInputException::class, ConnectorBuilderException::class)
  fun readStream(
    manifest: JsonNode,
    customComponentsCode: String?,
    config: JsonNode,
    state: List<JsonNode>?,
    stream: String?,
    recordLimit: Int?,
    pageLimit: Int?,
    sliceLimit: Int?,
  ): StreamRead
}
