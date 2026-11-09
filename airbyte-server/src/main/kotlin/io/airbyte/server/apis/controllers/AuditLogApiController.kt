/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.server.generated.apis.AuditLogsApi
import io.airbyte.api.server.generated.models.AuditLogListRequestBody
import io.airbyte.api.server.generated.models.AuditLogReadList
import io.airbyte.audit.logging.read.AuditLogReadService
import io.airbyte.commons.auth.route.AuthorizeOperation
import io.airbyte.commons.auth.route.RouteAccessPolicy
import io.airbyte.commons.server.errors.BadRequestException
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors
import io.airbyte.server.apis.mappers.AuditLogApiMapper
import io.airbyte.server.auth.AuditLogOrganizationRule
import io.micronaut.http.annotation.Controller
import io.micronaut.scheduling.annotation.ExecuteOn
import io.micronaut.security.annotation.Secured
import io.micronaut.security.rules.SecurityRule

/**
 * Customer-facing audit log reads. AuditLogOrganizationRule authorizes the requested organization
 * before this controller runs, and the read service lists only that organization's storage prefix.
 */
@Controller
open class AuditLogApiController(
  private val auditLogReadService: AuditLogReadService,
) : AuditLogsApi {
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Secured(SecurityRule.IS_AUTHENTICATED)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    rule = AuditLogOrganizationRule::class,
  )
  override fun listAuditLogs(auditLogListRequestBody: AuditLogListRequestBody): AuditLogReadList {
    val page =
      try {
        auditLogReadService.listAuditLogs(AuditLogApiMapper.toQuery(auditLogListRequestBody))
      } catch (e: IllegalArgumentException) {
        throw BadRequestException("Invalid audit log query: ${e.message}", e)
      }

    return AuditLogApiMapper.toApi(page)
  }
}
