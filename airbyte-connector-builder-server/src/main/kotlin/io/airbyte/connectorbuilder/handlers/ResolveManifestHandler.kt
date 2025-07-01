/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.connectorbuilder.handlers

import io.airbyte.connectorbuilder.TracingHelper.addWorkspaceAndProjectIdsToTrace
import io.airbyte.connectorbuilder.api.model.generated.ResolveManifest
import io.airbyte.connectorbuilder.api.model.generated.ResolveManifestRequestBody
import io.airbyte.connectorbuilder.exceptions.AirbyteCdkInvalidInputException
import io.airbyte.connectorbuilder.exceptions.ConnectorBuilderException
import io.airbyte.connectorbuilder.requester.AirbyteCdkRequester
import jakarta.inject.Inject
import jakarta.inject.Singleton
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.io.IOException

/**
 * Handle /manifest/resolve requests.
 */
@Singleton
open class ResolveManifestHandler
  @Inject
  constructor(
    private val requester: AirbyteCdkRequester,
  ) {
    /**
     * Use the requester to send the resolve_manifest request to the CDK.
     */
    @Throws(AirbyteCdkInvalidInputException::class, ConnectorBuilderException::class)
    fun resolveManifest(resolveManifestRequestBody: ResolveManifestRequestBody): ResolveManifest {
      try {
        addWorkspaceAndProjectIdsToTrace(resolveManifestRequestBody.workspaceId, resolveManifestRequestBody.projectId)
        LOGGER.info(
          "Handling resolve_manifest request for workspace '{}' with project ID = '{}'",
          resolveManifestRequestBody.workspaceId,
          resolveManifestRequestBody.projectId,
        )
        return requester.resolveManifest(resolveManifestRequestBody.manifest)
      } catch (exc: IOException) {
        LOGGER.error("Error handling resolve_manifest request.", exc)
        throw ConnectorBuilderException("Error handling resolve_manifest request.", exc)
      }
    }

    companion object {
      private val LOGGER: Logger = LoggerFactory.getLogger(ResolveManifestHandler::class.java)
    }
  }
