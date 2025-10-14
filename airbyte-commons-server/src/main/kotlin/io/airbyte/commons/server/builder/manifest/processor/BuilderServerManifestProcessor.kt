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
import io.airbyte.commons.json.Jsons
import io.airbyte.connectorbuilderserver.api.client.ConnectorBuilderServerApiClient
import io.airbyte.connectorbuilderserver.api.client.model.generated.AuxiliaryRequest
import io.airbyte.connectorbuilderserver.api.client.model.generated.FullResolveManifestRequestBody
import io.airbyte.connectorbuilderserver.api.client.model.generated.HttpRequest
import io.airbyte.connectorbuilderserver.api.client.model.generated.HttpResponse
import io.airbyte.connectorbuilderserver.api.client.model.generated.ResolveManifestRequestBody
import io.airbyte.connectorbuilderserver.api.client.model.generated.StreamRead
import io.airbyte.connectorbuilderserver.api.client.model.generated.StreamReadLogsInner
import io.airbyte.connectorbuilderserver.api.client.model.generated.StreamReadRequestBody
import io.airbyte.connectorbuilderserver.api.client.model.generated.StreamReadSlicesInner
import io.airbyte.connectorbuilderserver.api.client.model.generated.StreamReadSlicesInnerPagesInner
import io.micronaut.core.util.CollectionUtils
import jakarta.inject.Singleton
import java.util.UUID

/**
 * Implementation of ManifestProcessor that uses the connector-builder-server.
 */
@Singleton
class BuilderServerManifestProcessor(
  private val connectorBuilderServerApiClient: ConnectorBuilderServerApiClient,
) : ManifestProcessor {
  override fun resolveManifest(
    manifest: JsonNode,
    builderProjectId: UUID?,
    workspaceId: UUID?,
  ): JsonNode {
    val resolveManifestRequestBody =
      ResolveManifestRequestBody(
        workspaceId = workspaceId?.toString(),
        projectId = builderProjectId?.toString(),
        manifest = manifest,
      )
    val resolveManifest = connectorBuilderServerApiClient.builderServerApi.resolveManifest(resolveManifestRequestBody)
    return Jsons.jsonNode(resolveManifest.manifest)
  }

  override fun fullResolveManifest(
    config: JsonNode,
    manifest: JsonNode,
    streamLimit: Int?,
    builderProjectId: UUID?,
    workspaceId: UUID?,
  ): ConnectorBuilderResolvedManifest {
    val fullResolveManifestRequestBody =
      FullResolveManifestRequestBody(
        config,
        manifest,
        streamLimit,
        builderProjectId?.toString(),
        workspaceId?.toString(),
      )
    val resolveManifest = connectorBuilderServerApiClient.builderServerApi.fullResolveManifest(fullResolveManifestRequestBody)
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
    val streamReadRequestBody =
      StreamReadRequestBody(
        config,
        manifest,
        streamName,
        customComponentsCode,
        formGeneratedManifest,
        builderProjectId?.toString(),
        recordLimit,
        pageLimit,
        sliceLimit,
        state,
        workspaceId?.toString(),
      )
    val streamRead = connectorBuilderServerApiClient.builderServerApi.readStream(streamReadRequestBody)
    return convertStreamRead(streamRead)
  }

  override fun getCapabilities(): ConnectorBuilderCapabilities {
    val healthCheck = connectorBuilderServerApiClient.healthApi.getHealthCheck()
    val customCodeExecution = healthCheck.capabilities?.customCodeExecution ?: false
    return ConnectorBuilderCapabilities().customCodeExecution(customCodeExecution)
  }

  private fun convertStreamRead(streamRead: StreamRead): ConnectorBuilderProjectStreamRead =
    ConnectorBuilderProjectStreamRead()
      .logs(mapStreamReadLogs(streamRead))
      .slices(mapStreamReadSlices(streamRead))
      .testReadLimitReached(streamRead.testReadLimitReached)
      .auxiliaryRequests(mapGlobalAuxiliaryRequests(streamRead))
      .inferredSchema(streamRead.inferredSchema)
      .inferredDatetimeFormats(streamRead.inferredDatetimeFormats)
      .latestConfigUpdate(streamRead.latestConfigUpdate?.let { Jsons.convertValue(it, JsonNode::class.java) })

  private fun mapStreamReadLogs(streamRead: StreamRead): List<ConnectorBuilderProjectStreamReadLogsInner> =
    streamRead.logs
      .stream()
      .map { log: StreamReadLogsInner ->
        ConnectorBuilderProjectStreamReadLogsInner()
          .message(log.message)
          .level(ConnectorBuilderProjectStreamReadLogsInner.LevelEnum.fromString(log.level.value))
          .internalMessage(log.internalMessage)
          .stacktrace(log.stacktrace)
      }.toList()

  private fun mapStreamReadSlices(streamRead: StreamRead): List<ConnectorBuilderProjectStreamReadSlicesInner> =
    streamRead.slices
      .stream()
      .map { slice: StreamReadSlicesInner ->
        ConnectorBuilderProjectStreamReadSlicesInner()
          .sliceDescriptor(slice.sliceDescriptor)
          .state(if (CollectionUtils.isNotEmpty(slice.state)) slice.state else listOf())
          .pages(
            slice.pages
              .stream()
              .map { page: StreamReadSlicesInnerPagesInner ->
                ConnectorBuilderProjectStreamReadSlicesInnerPagesInner()
                  .records(page.records)
                  .request(convertHttpRequest(page.request))
                  .response(convertHttpResponse(page.response))
              }.toList(),
          ).auxiliaryRequests(mapSliceAuxiliaryRequests(slice))
      }.toList()

  private fun mapAuxiliaryRequests(auxiliaryRequests: List<AuxiliaryRequest?>?): List<ConnectorBuilderAuxiliaryRequest> =
    if (CollectionUtils.isNotEmpty(auxiliaryRequests)) {
      auxiliaryRequests!!
        .stream()
        .map { auxRequest: AuxiliaryRequest? ->
          ConnectorBuilderAuxiliaryRequest()
            .description(auxRequest!!.description)
            .request(convertHttpRequest(auxRequest.request))
            .response(convertHttpResponse(auxRequest.response))
            .title(auxRequest.title)
            .type(ConnectorBuilderAuxiliaryRequest.TypeEnum.fromString(auxRequest.type.value))
        }.toList()
    } else {
      listOf()
    }

  private fun mapGlobalAuxiliaryRequests(streamRead: StreamRead): List<ConnectorBuilderAuxiliaryRequest> =
    mapAuxiliaryRequests(streamRead.auxiliaryRequests)

  private fun mapSliceAuxiliaryRequests(slice: StreamReadSlicesInner): List<ConnectorBuilderAuxiliaryRequest> =
    mapAuxiliaryRequests(slice.auxiliaryRequests)

  private fun convertHttpRequest(request: HttpRequest?): ConnectorBuilderHttpRequest? =
    if (request != null) {
      ConnectorBuilderHttpRequest()
        .url(request.url)
        .httpMethod(ConnectorBuilderHttpRequest.HttpMethodEnum.fromString(request.httpMethod.value))
        .body(request.body)
        .headers(request.headers)
    } else {
      null
    }

  private fun convertHttpResponse(response: HttpResponse?): ConnectorBuilderHttpResponse? =
    if (response != null) {
      ConnectorBuilderHttpResponse()
        .status(response.status)
        .body(response.body)
        .headers(response.headers)
    } else {
      null
    }
}
