/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.publicapi.services

import io.airbyte.api.model.generated.SourceDefinitionIdWithWorkspaceId
import io.airbyte.api.model.generated.SourceDefinitionSpecificationRead
import io.airbyte.commons.server.handlers.ConnectorDefinitionSpecificationHandler
import io.airbyte.server.apis.publicapi.constants.HTTP_RESPONSE_BODY_DEBUG_MESSAGE
import io.airbyte.server.apis.publicapi.errorHandlers.ConfigClientErrorHandler
import io.airbyte.server.apis.publicapi.mappers.SourceDefinitionSpecificationReadMapper
import io.micronaut.context.annotation.Secondary
import jakarta.inject.Singleton
import org.slf4j.LoggerFactory
import java.util.UUID

interface SourceDefinitionService {
  fun getSourceDefinitionSpecification(
    sourceDefinitionId: UUID,
    workspaceId: UUID,
  ): SourceDefinitionSpecificationRead?
}

@Singleton
@Secondary
class SourceDefinitionServiceImpl(
  private val sourceDefinitionSpecificationHandler: ConnectorDefinitionSpecificationHandler,
) : io.airbyte.server.apis.publicapi.services.SourceDefinitionService {
  companion object {
    private val log = LoggerFactory.getLogger(io.airbyte.server.apis.publicapi.services.SourceDefinitionServiceImpl::class.java)
  }

  override fun getSourceDefinitionSpecification(
    sourceDefinitionId: UUID,
    workspaceId: UUID,
  ): SourceDefinitionSpecificationRead? {
    val sourceDefinitionIdWithWorkspaceId = SourceDefinitionIdWithWorkspaceId().sourceDefinitionId(sourceDefinitionId).workspaceId(workspaceId)

    val result =
      kotlin.runCatching { sourceDefinitionSpecificationHandler.getSourceDefinitionSpecification(sourceDefinitionIdWithWorkspaceId) }
        .onFailure {
          io.airbyte.server.apis.publicapi.services.SourceDefinitionServiceImpl.Companion.log.error("Error for getSourceDefinitionSpecification", it)
          ConfigClientErrorHandler.handleError(it, sourceDefinitionId.toString())
        }

    io.airbyte.server.apis.publicapi.services.SourceDefinitionServiceImpl.Companion.log.debug(HTTP_RESPONSE_BODY_DEBUG_MESSAGE + result)
    return SourceDefinitionSpecificationReadMapper.from(
      result.getOrNull()!!,
    )
  }
}
