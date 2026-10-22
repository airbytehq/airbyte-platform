/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.builder.manifest.processor

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.api.server.generated.models.ConnectorBuilderAuxiliaryRequest
import io.airbyte.api.server.generated.models.ConnectorBuilderCapabilities
import io.airbyte.api.server.generated.models.ConnectorBuilderHttpRequest
import io.airbyte.api.server.generated.models.ConnectorBuilderHttpResponse
import io.airbyte.api.server.generated.models.ConnectorBuilderProjectStreamRead
import io.airbyte.api.server.generated.models.ConnectorBuilderProjectStreamReadLogsInner
import io.airbyte.api.server.generated.models.ConnectorBuilderProjectStreamReadSlicesInner
import io.airbyte.api.server.generated.models.ConnectorBuilderProjectStreamReadSlicesInnerPagesInner
import io.airbyte.api.server.generated.models.ConnectorBuilderResolvedManifest
import io.airbyte.commons.json.Jsons
import io.airbyte.commons.server.errors.BadRequestException
import io.airbyte.commons.server.errors.KnownException
import io.airbyte.commons.server.errors.UnprocessableContentException
import io.airbyte.manifestserver.api.client.ManifestServerApiClient
import io.airbyte.manifestserver.api.client.model.generated.FullResolveRequest
import io.airbyte.manifestserver.api.client.model.generated.HttpRequest
import io.airbyte.manifestserver.api.client.model.generated.HttpResponse
import io.airbyte.manifestserver.api.client.model.generated.RequestContext
import io.airbyte.manifestserver.api.client.model.generated.ResolveRequest
import io.airbyte.manifestserver.api.client.model.generated.StreamTestReadRequest
import io.micronaut.http.HttpStatus
import jakarta.inject.Singleton
import org.openapitools.client.infrastructure.ClientError
import org.openapitools.client.infrastructure.ClientException
import java.util.UUID
import kotlin.jvm.optionals.getOrNull

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
    val requestContext =
      RequestContext(
        workspaceId = workspaceId?.toString(),
        projectId = builderProjectId?.toString(),
      )
    val resolveRequest = ResolveRequest(manifest = manifest, requestContext)
    val resolveManifest = callManifestServer { manifestServerApiClient.manifestApi.resolve(resolveRequest) }
    return resolveManifest.manifest
  }

  override fun fullResolveManifest(
    config: JsonNode,
    manifest: JsonNode,
    streamLimit: Int?,
    builderProjectId: UUID?,
    workspaceId: UUID?,
  ): ConnectorBuilderResolvedManifest {
    val requestContext =
      RequestContext(
        workspaceId = workspaceId?.toString(),
        projectId = builderProjectId?.toString(),
      )
    val fullResolveRequest =
      FullResolveRequest(
        config = config,
        manifest = manifest,
        streamLimit = streamLimit,
        context = requestContext,
      )
    val resolveManifest =
      callManifestServer {
        manifestServerApiClient.manifestApi.fullResolve(
          fullResolveRequest,
        )
      }
    return ConnectorBuilderResolvedManifest(manifest = resolveManifest.manifest)
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
    val requestContext =
      RequestContext(
        workspaceId = workspaceId?.toString(),
        projectId = builderProjectId?.toString(),
      )
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
        context = requestContext,
      )
    val manifestServerStreamRead = callManifestServer { manifestServerApiClient.manifestApi.testRead(testReadRequest) }
    return convertStreamRead(manifestServerStreamRead)
  }

  override fun getCapabilities(): ConnectorBuilderCapabilities {
    val capabilities = manifestServerApiClient.capabilitiesApi.getCapabilities()
    return ConnectorBuilderCapabilities(customCodeExecution = capabilities.customCodeExecution)
  }

  private fun <T> callManifestServer(call: () -> T): T =
    try {
      call()
    } catch (e: ClientException) {
      throw toKnownException(e) ?: e
    }

  private fun toKnownException(e: ClientException): KnownException? {
    val body = (e.response as? ClientError<*>)?.body as? String ?: return null
    val detail =
      Jsons
        .tryDeserialize(body)
        .getOrNull()
        ?.get("detail")
        ?.takeIf { it.isTextual }
        ?.asText()
        ?.takeIf { it.isNotBlank() } ?: return null
    return when (e.statusCode) {
      HttpStatus.BAD_REQUEST.code -> BadRequestException(detail, e)
      HttpStatus.UNPROCESSABLE_ENTITY.code -> UnprocessableContentException(detail, e)
      else -> null
    }
  }

  private fun convertStreamRead(
    manifestServerStreamRead: io.airbyte.manifestserver.api.client.model.generated.StreamReadResponse,
  ): ConnectorBuilderProjectStreamRead {
    // Convert logs
    val convertedLogs =
      manifestServerStreamRead.logs.map { log ->
        ConnectorBuilderProjectStreamReadLogsInner(
          message = log.message,
          level = ConnectorBuilderProjectStreamReadLogsInner.Level.valueOf(log.level.uppercase()),
          internalMessage = log.internalMessage,
          stacktrace = log.stacktrace,
        )
      }

    // Convert slices
    val convertedSlices =
      manifestServerStreamRead.slices.map { slice ->
        ConnectorBuilderProjectStreamReadSlicesInner(
          sliceDescriptor = slice.sliceDescriptor,
          state = slice.state ?: listOf(),
          pages =
            slice.pages.map { page ->
              ConnectorBuilderProjectStreamReadSlicesInnerPagesInner(
                records = page.records,
                request = convertHttpRequest(page.request),
                response = convertHttpResponse(page.response),
              )
            },
          auxiliaryRequests = convertAuxiliaryRequests(slice.auxiliaryRequests),
        )
      }

    // Convert auxiliary requests
    val convertedAuxiliaryRequests = convertAuxiliaryRequests(manifestServerStreamRead.auxiliaryRequests)

    return ConnectorBuilderProjectStreamRead(
      logs = convertedLogs,
      slices = convertedSlices,
      testReadLimitReached = manifestServerStreamRead.testReadLimitReached,
      auxiliaryRequests = convertedAuxiliaryRequests,
      inferredSchema = manifestServerStreamRead.inferredSchema,
      inferredDatetimeFormats = manifestServerStreamRead.inferredDatetimeFormats,
    )
  }

  private fun convertAuxiliaryRequests(
    auxiliaryRequests: List<io.airbyte.manifestserver.api.client.model.generated.AuxiliaryRequest>?,
  ): List<ConnectorBuilderAuxiliaryRequest>? =
    auxiliaryRequests?.map { auxRequest ->
      ConnectorBuilderAuxiliaryRequest(
        description = auxRequest.description,
        title = auxRequest.title,
        type = ConnectorBuilderAuxiliaryRequest.Type.valueOf(auxRequest.type.uppercase()),
        request = convertHttpRequest(auxRequest.request)!!,
        response = convertHttpResponse(auxRequest.response)!!,
      )
    }

  private fun convertHttpRequest(request: HttpRequest?): ConnectorBuilderHttpRequest? =
    request?.let {
      ConnectorBuilderHttpRequest(
        url = it.url,
        httpMethod = ConnectorBuilderHttpRequest.HttpMethod.valueOf(it.httpMethod.uppercase()),
        body = it.body,
        headers = it.headers,
      )
    }

  private fun convertHttpResponse(response: HttpResponse?): ConnectorBuilderHttpResponse? =
    response?.let {
      ConnectorBuilderHttpResponse(status = it.status, body = it.body, headers = it.headers)
    }
}
