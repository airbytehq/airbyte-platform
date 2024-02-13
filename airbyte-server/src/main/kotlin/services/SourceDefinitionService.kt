/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.api.server.services

import io.airbyte.api.model.generated.SourceDefinitionIdWithWorkspaceId
import io.airbyte.api.model.generated.SourceDefinitionSpecificationRead
import io.airbyte.api.server.constants.HTTP_RESPONSE_BODY_DEBUG_MESSAGE
import io.airbyte.api.server.errorHandlers.ConfigClientErrorHandler
import io.airbyte.api.server.mappers.SourceDefinitionSpecificationReadMapper
import io.airbyte.commons.server.handlers.ConnectorDefinitionSpecificationHandler
import io.micronaut.context.annotation.Secondary
import jakarta.inject.Singleton
import org.slf4j.LoggerFactory
import java.util.UUID

interface SourceDefinitionService {
  fun getSourceDefinitionSpecification(
    sourceDefinitionId: UUID,
    workspaceId: UUID,
    userInfo: String?,
  ): SourceDefinitionSpecificationRead?
}

@Singleton
@Secondary
class SourceDefinitionServiceImpl(
  private val sourceDefinitionSpecificationHandler: ConnectorDefinitionSpecificationHandler,
) : SourceDefinitionService {
  companion object {
    private val log = LoggerFactory.getLogger(SourceDefinitionServiceImpl::class.java)
  }

  override fun getSourceDefinitionSpecification(
    sourceDefinitionId: UUID,
    workspaceId: UUID,
    userInfo: String?,
  ): SourceDefinitionSpecificationRead? {
    val sourceDefinitionIdWithWorkspaceId = SourceDefinitionIdWithWorkspaceId().sourceDefinitionId(sourceDefinitionId).workspaceId(workspaceId)

    val result =
      kotlin.runCatching { sourceDefinitionSpecificationHandler.getSourceDefinitionSpecification(sourceDefinitionIdWithWorkspaceId) }
        .onFailure {
          log.error("Error for getSourceDefinitionSpecification", it)
          ConfigClientErrorHandler.handleError(it, sourceDefinitionId.toString())
        }

    log.debug(HTTP_RESPONSE_BODY_DEBUG_MESSAGE + result)
    return SourceDefinitionSpecificationReadMapper.from(
      result.getOrNull()!!,
    )
  }
}
