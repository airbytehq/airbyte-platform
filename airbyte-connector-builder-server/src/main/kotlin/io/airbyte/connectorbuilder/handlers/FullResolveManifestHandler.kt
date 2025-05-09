/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.connectorbuilder.handlers

import io.airbyte.connectorbuilder.TracingHelper
import io.airbyte.connectorbuilder.api.model.generated.FullResolveManifestRequestBody
import io.airbyte.connectorbuilder.api.model.generated.ResolveManifest
import io.airbyte.connectorbuilder.exceptions.AirbyteCdkInvalidInputException
import io.airbyte.connectorbuilder.exceptions.ConnectorBuilderException
import io.airbyte.connectorbuilder.requester.AirbyteCdkRequester
import jakarta.inject.Inject
import jakarta.inject.Singleton
import org.slf4j.Logger
import org.slf4j.LoggerFactory
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
    @Throws(AirbyteCdkInvalidInputException::class, ConnectorBuilderException::class)
    fun fullResolveManifest(fullResolveManifestRequestBody: FullResolveManifestRequestBody): ResolveManifest {
      try {
        TracingHelper.addWorkspaceAndProjectIdsToTrace(
          fullResolveManifestRequestBody.workspaceId,
          fullResolveManifestRequestBody.projectId,
        )
        LOGGER.info(
          "Handling full_resolve_manifest request for workspace '{}' with project ID = '{}'",
          fullResolveManifestRequestBody.workspaceId,
          fullResolveManifestRequestBody.projectId,
        )
        return requester.fullResolveManifest(
          fullResolveManifestRequestBody.manifest,
          fullResolveManifestRequestBody.config,
          fullResolveManifestRequestBody.streamLimit,
        )
      } catch (exc: IOException) {
        LOGGER.error("Error handling resolve_manifest request.", exc)
        throw ConnectorBuilderException("Error handling resolve_manifest request.", exc)
      }
    }

    companion object {
      private val LOGGER: Logger = LoggerFactory.getLogger(FullResolveManifestHandler::class.java)
    }
  }
