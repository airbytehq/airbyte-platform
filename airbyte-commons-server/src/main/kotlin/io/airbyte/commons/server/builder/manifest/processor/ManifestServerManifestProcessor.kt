/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.builder.manifest.processor

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.api.model.generated.ConnectorBuilderAuxiliaryRequest
import io.airbyte.api.model.generated.ConnectorBuilderCapabilities
import io.airbyte.api.model.generated.ConnectorBuilderHttpRequest
import io.airbyte.api.model.generated.ConnectorBuilderHttpResponse
import io.airbyte.api.model.generated.ConnectorBuilderProjectStreamRead
import io.airbyte.api.model.generated.ConnectorBuilderProjectStreamReadLogsInner
import io.airbyte.api.model.generated.ConnectorBuilderProjectStreamReadSlicesInner
import io.airbyte.api.model.generated.ConnectorBuilderProjectStreamReadSlicesInnerPagesInner
import io.airbyte.api.model.generated.ConnectorBuilderResolvedManifest
import io.airbyte.manifestserver.api.client.ManifestServerApiClient
import io.airbyte.manifestserver.api.client.model.generated.FullResolveRequest
import io.airbyte.manifestserver.api.client.model.generated.HttpRequest
import io.airbyte.manifestserver.api.client.model.generated.HttpResponse
import io.airbyte.manifestserver.api.client.model.generated.ResolveRequest
import io.airbyte.manifestserver.api.client.model.generated.StreamTestReadRequest
import jakarta.inject.Singleton
import java.util.UUID

/**
 * Implementation of ManifestProcessor that uses the manifest-runner service.
 */
@Singleton
class ManifestServerManifestProcessor(
  private val manifestServerApiClient: ManifestServerApiClient,
) : ManifestProcessor {
  override fun resolveManifest(
    manifest: JsonNode,
    builderProjectId: UUID?,
    workspaceId: UUID?,
  ): JsonNode {
    val resolveRequest = ResolveRequest(manifest = manifest)
    val resolveManifest = manifestServerApiClient.manifestApi.resolve(resolveRequest)
    return resolveManifest.manifest
  }

  override fun fullResolveManifest(
    config: JsonNode,
    manifest: JsonNode,
    streamLimit: Int?,
    builderProjectId: UUID?,
    workspaceId: UUID?,
  ): ConnectorBuilderResolvedManifest {
    val fullResolveRequest =
      FullResolveRequest(
        config = config,
        manifest = manifest,
        streamLimit = streamLimit,
      )
    val resolveManifest =
      manifestServerApiClient.manifestApi.fullResolve(
        fullResolveRequest,
      )
    return ConnectorBuilderResolvedManifest()
      .manifest(resolveManifest.manifest)
  }

  override fun streamTestRead(
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
  ): ConnectorBuilderProjectStreamRead {
    val testReadRequest =
      StreamTestReadRequest(
        config = config,
        manifest = manifest,
        streamName = streamName,
        state = state,
        customComponentsCode = customComponentsCode,
        recordLimit = recordLimit,
        pageLimit = pageLimit,
        sliceLimit = sliceLimit,
      )
    val manifestserverStreamRead = manifestServerApiClient.manifestApi.testRead(testReadRequest)
    return convertStreamRead(manifestserverStreamRead)
  }

  override fun getCapabilities(): ConnectorBuilderCapabilities {
    val capabilities = manifestServerApiClient.capabilitiesApi.getCapabilities()
    return ConnectorBuilderCapabilities().customCodeExecution(capabilities.customCodeExecution)
  }

  private fun convertStreamRead(
    manifestserverStreamRead: io.airbyte.manifestserver.api.client.model.generated.StreamReadResponse,
  ): ConnectorBuilderProjectStreamRead {
    // Convert logs
    val convertedLogs =
      manifestserverStreamRead.logs.map { log ->
        ConnectorBuilderProjectStreamReadLogsInner()
          .message(log.message)
          .level(ConnectorBuilderProjectStreamReadLogsInner.LevelEnum.valueOf(log.level.uppercase()))
          .internalMessage(log.internalMessage)
          .stacktrace(log.stacktrace)
      }

    // Convert slices
    val convertedSlices =
      manifestserverStreamRead.slices.map { slice ->
        ConnectorBuilderProjectStreamReadSlicesInner()
          .sliceDescriptor(slice.sliceDescriptor)
          .state(slice.state ?: listOf())
          .pages(
            slice.pages.map { page ->
              ConnectorBuilderProjectStreamReadSlicesInnerPagesInner()
                .records(page.records)
                .request(convertHttpRequest(page.request))
                .response(convertHttpResponse(page.response))
            },
          ).auxiliaryRequests(convertAuxiliaryRequests(slice.auxiliaryRequests))
      }

    // Convert auxiliary requests
    val convertedAuxiliaryRequests = convertAuxiliaryRequests(manifestserverStreamRead.auxiliaryRequests)

    return ConnectorBuilderProjectStreamRead()
      .logs(convertedLogs)
      .slices(convertedSlices)
      .testReadLimitReached(manifestserverStreamRead.testReadLimitReached)
      .auxiliaryRequests(convertedAuxiliaryRequests)
      .inferredSchema(manifestserverStreamRead.inferredSchema)
      .inferredDatetimeFormats(manifestserverStreamRead.inferredDatetimeFormats)
  }

  private fun convertAuxiliaryRequests(
    auxiliaryRequests: List<io.airbyte.manifestserver.api.client.model.generated.AuxiliaryRequest>?,
  ): List<ConnectorBuilderAuxiliaryRequest>? =
    auxiliaryRequests?.map { auxRequest ->
      ConnectorBuilderAuxiliaryRequest()
        .description(auxRequest.description)
        .title(auxRequest.title)
        .type(ConnectorBuilderAuxiliaryRequest.TypeEnum.valueOf(auxRequest.type.uppercase()))
        .request(convertHttpRequest(auxRequest.request)!!)
        .response(convertHttpResponse(auxRequest.response)!!)
    }

  private fun convertHttpRequest(request: HttpRequest?): ConnectorBuilderHttpRequest? =
    request?.let {
      ConnectorBuilderHttpRequest()
        .url(it.url)
        .httpMethod(ConnectorBuilderHttpRequest.HttpMethodEnum.valueOf(it.httpMethod.uppercase()))
        .body(it.body)
        .headers(it.headers)
    }

  private fun convertHttpResponse(response: HttpResponse?): ConnectorBuilderHttpResponse? =
    response?.let {
      ConnectorBuilderHttpResponse()
        .status(it.status)
        .body(it.body)
        .headers(it.headers)
    }
}
