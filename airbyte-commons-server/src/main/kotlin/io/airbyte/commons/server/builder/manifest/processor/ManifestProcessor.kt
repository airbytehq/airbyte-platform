/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.builder.manifest.processor

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.api.model.generated.ConnectorBuilderCapabilities
import io.airbyte.api.model.generated.ConnectorBuilderProjectStreamRead
import io.airbyte.api.model.generated.ConnectorBuilderResolvedManifest
import java.util.UUID

/**
 * Interface for processing manifests through different services (builder-server vs manifest-runner).
 */
interface ManifestProcessor {
  fun resolveManifest(
    manifest: JsonNode,
    builderProjectId: UUID?,
    workspaceId: UUID?,
  ): JsonNode

  fun fullResolveManifest(
    config: JsonNode,
    manifest: JsonNode,
    streamLimit: Int?,
    builderProjectId: UUID?,
    workspaceId: UUID?,
  ): ConnectorBuilderResolvedManifest

  fun streamTestRead(
    config: JsonNode,
    manifest: JsonNode,
    streamName: String,
    customComponentsCode: String?,
    formGeneratedManifest: Boolean?,
    builderProjectId: UUID?,
    recordLimit: Int?,
    pageLimit: Int?,
    sliceLimit: Int?,
    state: List<Any>?,
    workspaceId: UUID?,
  ): ConnectorBuilderProjectStreamRead

  fun getCapabilities(): ConnectorBuilderCapabilities
}
