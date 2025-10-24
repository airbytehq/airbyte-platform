/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.connectorbuilder.handlers

import io.airbyte.commons.server.builder.exceptions.ConnectorBuilderException
import io.airbyte.connectorbuilder.TracingHelper
import io.airbyte.connectorbuilder.api.model.generated.FullResolveManifestRequestBody
import io.airbyte.connectorbuilder.api.model.generated.ResolveManifest
import io.airbyte.connectorbuilder.exceptions.AirbyteCdkInvalidInputException
import io.airbyte.connectorbuilder.requester.AirbyteCdkRequester
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Inject
import jakarta.inject.Singleton
import java.io.IOException

/**
 * Handle /manifest/full_resolve requests.
 */
@Singleton
internal open class FullResolveManifestHandler
  @Inject
  constructor(
    private val requester: AirbyteCdkRequester,
  ) {
    /**
     * Use the requester to send the full_resolve_manifest request to the CDK.
     */
    fun fullResolveManifest(fullResolveManifestRequestBody: FullResolveManifestRequestBody): ResolveManifest {
      try {
        TracingHelper.addWorkspaceAndProjectIdsToTrace(
          fullResolveManifestRequestBody.workspaceId,
          fullResolveManifestRequestBody.projectId,
        )
        log.info {
          "Handling full_resolve_manifest request for workspace '${fullResolveManifestRequestBody.workspaceId}' with project ID = '${fullResolveManifestRequestBody.projectId}'"
        }
        return requester.fullResolveManifest(
          fullResolveManifestRequestBody.manifest,
          fullResolveManifestRequestBody.config,
          fullResolveManifestRequestBody.streamLimit,
        )
      } catch (exc: IOException) {
        log.error(exc) { "Error handling resolve_manifest request." }
        throw ConnectorBuilderException("Error handling resolve_manifest request.", exc)
      }
    }

    companion object {
      private val log = KotlinLogging.logger {}
    }
  }
