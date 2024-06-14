/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.publicapi.services

import io.airbyte.api.model.generated.ListOrganizationsByUserRequestBody
import io.airbyte.commons.server.handlers.OrganizationsHandler
import io.airbyte.publicApi.server.generated.models.OrganizationsResponse
import io.airbyte.server.apis.publicapi.constants.HTTP_RESPONSE_BODY_DEBUG_MESSAGE
import io.airbyte.server.apis.publicapi.errorHandlers.ConfigClientErrorHandler
import io.airbyte.server.apis.publicapi.mappers.OrganizationReadMapper
import io.micronaut.context.annotation.Secondary
import jakarta.inject.Singleton
import org.slf4j.LoggerFactory
import java.util.UUID

interface OrganizationService {
  fun getOrganizationsByUser(userId: UUID): OrganizationsResponse
}

@Singleton
@Secondary
abstract class OrganizationServiceImpl(
  private val organizationsHandler: OrganizationsHandler,
) : OrganizationService {
  companion object {
    private val log = LoggerFactory.getLogger(OrganizationServiceImpl::class.java)
  }

  override fun getOrganizationsByUser(userId: UUID): OrganizationsResponse {
    val result =
      kotlin.runCatching {
        organizationsHandler.listOrganizationsByUser(ListOrganizationsByUserRequestBody().userId(userId))
      }
        .onFailure {
          log.error("Error for getOrganizationsByUser", it)
          ConfigClientErrorHandler.handleError(it, "airbyte-organization")
        }
    log.debug(HTTP_RESPONSE_BODY_DEBUG_MESSAGE + result)
    val userOrganizationsReadList = result.getOrThrow()
    return OrganizationsResponse(data = userOrganizationsReadList.organizations.mapNotNull { OrganizationReadMapper.from(it) })
  }
}
