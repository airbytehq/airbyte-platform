/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.publicapi.services

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.api.model.generated.ActorTypeEnum
import io.airbyte.api.model.generated.ListOrganizationsByUserRequestBody
import io.airbyte.api.problems.model.generated.ProblemValueData
import io.airbyte.api.problems.throwable.generated.UnknownValueProblem
import io.airbyte.commons.server.handlers.OAuthHandler
import io.airbyte.commons.server.handlers.OrganizationsHandler
import io.airbyte.domain.models.ActorDefinitionId
import io.airbyte.domain.models.OrganizationId
import io.airbyte.publicApi.server.generated.models.OrganizationsResponse
import io.airbyte.server.apis.publicapi.constants.HTTP_RESPONSE_BODY_DEBUG_MESSAGE
import io.airbyte.server.apis.publicapi.errorHandlers.ConfigClientErrorHandler
import io.airbyte.server.apis.publicapi.mappers.DESTINATION_NAME_TO_DEFINITION_ID
import io.airbyte.server.apis.publicapi.mappers.OrganizationReadMapper
import io.airbyte.server.apis.publicapi.mappers.SOURCE_NAME_TO_DEFINITION_ID
import io.micronaut.context.annotation.Secondary
import jakarta.inject.Singleton
import org.slf4j.LoggerFactory
import java.util.UUID

interface OrganizationService {
  fun getOrganizationsByUser(userId: UUID): OrganizationsResponse

  fun setOrganizationOverrideOauthParams(
    organizationId: OrganizationId,
    actorName: String,
    actorType: ActorTypeEnum,
    configuration: JsonNode,
  ): Unit
}

@Singleton
@Secondary
open class OrganizationServiceImpl(
  private val organizationsHandler: OrganizationsHandler,
  private val oAuthHandler: OAuthHandler,
) : OrganizationService {
  companion object {
    private val log = LoggerFactory.getLogger(OrganizationServiceImpl::class.java)
  }

  override fun getOrganizationsByUser(userId: UUID): OrganizationsResponse {
    val result =
      kotlin
        .runCatching {
          organizationsHandler.listOrganizationsByUser(ListOrganizationsByUserRequestBody().userId(userId))
        }.onFailure {
          log.error("Error for getOrganizationsByUser", it)
          ConfigClientErrorHandler.handleError(it)
        }
    log.debug(HTTP_RESPONSE_BODY_DEBUG_MESSAGE + result)
    val userOrganizationsReadList = result.getOrThrow()
    return OrganizationsResponse(data = userOrganizationsReadList.organizations.mapNotNull { OrganizationReadMapper.from(it) })
  }

  override fun setOrganizationOverrideOauthParams(
    organizationId: OrganizationId,
    actorName: String,
    actorType: ActorTypeEnum,
    configuration: JsonNode,
  ) {
    val nameToDefinitionMap: Map<String, UUID> =
      when (actorType) {
        ActorTypeEnum.SOURCE -> SOURCE_NAME_TO_DEFINITION_ID
        ActorTypeEnum.DESTINATION -> DESTINATION_NAME_TO_DEFINITION_ID
      }

    val definitionId: UUID =
      nameToDefinitionMap[actorName] ?: throw UnknownValueProblem(
        ProblemValueData().value(actorName),
      )

    oAuthHandler.setOrganizationOverrideOAuthParams(organizationId, ActorDefinitionId(definitionId), actorType, configuration)
  }
}
