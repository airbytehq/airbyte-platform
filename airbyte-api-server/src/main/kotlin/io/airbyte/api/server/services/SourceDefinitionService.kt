/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.api.server.services

import io.airbyte.api.client.model.generated.SourceDefinitionIdWithWorkspaceId
import io.airbyte.api.client.model.generated.SourceDefinitionSpecificationRead
import io.airbyte.api.server.constants.HTTP_RESPONSE_BODY_DEBUG_MESSAGE
import io.airbyte.api.server.errorHandlers.ConfigClientErrorHandler
import io.airbyte.api.server.forwardingClient.ConfigApiClient
import io.airbyte.api.server.mappers.SourceDefinitionSpecificationReadMapper
import io.micronaut.context.annotation.Secondary
import io.micronaut.http.HttpResponse
import io.micronaut.http.client.exceptions.HttpClientResponseException
import jakarta.inject.Singleton
import org.slf4j.LoggerFactory
import java.util.Objects
import java.util.UUID

interface SourceDefinitionService {
  fun getSourceDefinitionSpecification(
    sourceDefinitionId: UUID,
    workspaceId: UUID,
    authorization: String?,
    userInfo: String?,
  ): SourceDefinitionSpecificationRead?
}

@Singleton
@Secondary
class SourceDefinitionServiceImpl(private val configApiClient: ConfigApiClient) : SourceDefinitionService {
  companion object {
    private val log = LoggerFactory.getLogger(SourceDefinitionServiceImpl::class.java)
  }

  override fun getSourceDefinitionSpecification(
    sourceDefinitionId: UUID,
    workspaceId: UUID,
    authorization: String?,
    userInfo: String?,
  ): SourceDefinitionSpecificationRead? {
    val sourceDefinitionIdWithWorkspaceId = SourceDefinitionIdWithWorkspaceId().sourceDefinitionId(sourceDefinitionId).workspaceId(workspaceId)

    var response: HttpResponse<SourceDefinitionSpecificationRead>
    try {
      response = configApiClient.getSourceDefinitionSpecification(sourceDefinitionIdWithWorkspaceId, authorization, userInfo!!)
    } catch (e: HttpClientResponseException) {
      log.error("Config api response error for cancelJob: ", e)
      response = e.response as HttpResponse<SourceDefinitionSpecificationRead>
    }
    ConfigClientErrorHandler.handleError(response, sourceDefinitionId.toString())
    log.debug(HTTP_RESPONSE_BODY_DEBUG_MESSAGE + response.body())
    return SourceDefinitionSpecificationReadMapper.from(
      Objects.requireNonNull<SourceDefinitionSpecificationRead?>(
        response.body(),
      ),
    )
  }
}
