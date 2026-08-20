/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.problems.model.generated.ProblemMessageData
import io.airbyte.api.problems.throwable.generated.ForbiddenProblem
import io.airbyte.api.server.generated.apis.AuditLogsApi
import io.airbyte.api.server.generated.models.AuditLogListRequestBody
import io.airbyte.api.server.generated.models.AuditLogReadList
import io.airbyte.audit.logging.read.AuditLogReadService
import io.airbyte.commons.auth.roles.AuthRoleConstants
import io.airbyte.commons.server.errors.BadRequestException
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors
import io.airbyte.commons.server.support.AuthenticationHeaderResolver
import io.airbyte.server.apis.mappers.AuditLogApiMapper
import io.micronaut.http.annotation.Controller
import io.micronaut.http.context.ServerRequestContext
import io.micronaut.scheduling.annotation.ExecuteOn
import io.micronaut.security.annotation.Secured
import java.util.UUID

/**
 * Customer-facing audit log reads. Tenant isolation is structural: the organization is derived
 * from the authenticated request context (the same headers the security layer authorizes), a
 * request body naming any other organization is rejected, and the read service only ever lists
 * the resolved organization's storage prefix.
 */
@Controller
open class AuditLogApiController(
  private val auditLogReadService: AuditLogReadService,
  private val authenticationHeaderResolver: AuthenticationHeaderResolver,
) : AuditLogsApi {
  @Secured(AuthRoleConstants.ORGANIZATION_ADMIN)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun listAuditLogs(auditLogListRequestBody: AuditLogListRequestBody): AuditLogReadList {
    val authorizedOrganizationIds = resolveAuthorizedOrganizationIds()
    if (auditLogListRequestBody.organizationId !in authorizedOrganizationIds) {
      throw ForbiddenProblem(
        ProblemMessageData().message("The requested organization does not match the organization authorized for this request."),
      )
    }

    val page =
      try {
        auditLogReadService.listAuditLogs(AuditLogApiMapper.toQuery(auditLogListRequestBody))
      } catch (e: IllegalArgumentException) {
        throw BadRequestException("Invalid audit log query: ${e.message}", e)
      }

    return AuditLogApiMapper.toApi(page)
  }

  /**
   * Resolves the organization(s) the current request is authorized for from the request headers,
   * mirroring the resolution the security layer performs for the @Secured check.
   */
  private fun resolveAuthorizedOrganizationIds(): List<UUID> {
    val request =
      ServerRequestContext.currentRequest<Any>().orElseThrow {
        ForbiddenProblem(ProblemMessageData().message("No request context available."))
      }
    val headers = request.headers.asMap(String::class.java, String::class.java)
    return authenticationHeaderResolver.resolveOrganization(headers) ?: emptyList()
  }
}
