/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.connectorbuilder.handlers

import io.airbyte.commons.server.builder.exceptions.ConnectorBuilderException
import io.airbyte.connectorbuilder.TracingHelper.addWorkspaceAndProjectIdsToTrace
import io.airbyte.connectorbuilder.api.model.generated.ResolveManifest
import io.airbyte.connectorbuilder.api.model.generated.ResolveManifestRequestBody
import io.airbyte.connectorbuilder.exceptions.AirbyteCdkInvalidInputException
import io.airbyte.connectorbuilder.requester.AirbyteCdkRequester
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Inject
import jakarta.inject.Singleton
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
        log.info(
          "Handling resolve_manifest request for workspace '{}' with project ID = '{}'",
          resolveManifestRequestBody.workspaceId,
          resolveManifestRequestBody.projectId,
        )
        return requester.resolveManifest(resolveManifestRequestBody.manifest)
      } catch (exc: IOException) {
        log.error(exc) { "Error handling resolve_manifest request." }
        throw ConnectorBuilderException("Error handling resolve_manifest request.", exc)
      }
    }

    companion object {
      private val log = KotlinLogging.logger {}
    }
  }
