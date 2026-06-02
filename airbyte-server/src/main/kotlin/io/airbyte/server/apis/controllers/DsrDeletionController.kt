/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import io.airbyte.commons.annotation.AuditLogging
import io.airbyte.commons.annotation.AuditLoggingProvider
import io.airbyte.commons.auth.roles.AuthRoleConstants
import io.airbyte.commons.server.handlers.dsr.DsrDeletionService
import io.airbyte.commons.server.handlers.dsr.DsrInvalidConfirmationException
import io.airbyte.commons.server.handlers.dsr.DsrInvalidStateException
import io.airbyte.commons.server.handlers.dsr.DsrRequestNotFoundException
import io.airbyte.commons.server.handlers.dsr.DsrUserNotFoundException
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors
import io.airbyte.data.repositories.entities.DataSubjectDeletionRequest
import io.airbyte.domain.services.dsr.DsrManifest
import io.airbyte.server.apis.execute
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.Context
import io.micronaut.core.annotation.Introspected
import io.micronaut.http.HttpResponse
import io.micronaut.http.HttpStatus
import io.micronaut.http.annotation.Body
import io.micronaut.http.annotation.Controller
import io.micronaut.http.annotation.Get
import io.micronaut.http.annotation.PathVariable
import io.micronaut.http.annotation.Post
import io.micronaut.http.annotation.QueryValue
import io.micronaut.http.annotation.Status
import io.micronaut.scheduling.annotation.ExecuteOn
import io.micronaut.security.annotation.Secured
import java.time.OffsetDateTime
import java.util.UUID

private val log = KotlinLogging.logger {}

/**
 * Internal-only API controller for GDPR / DSR deletion automation.
 *
 * These endpoints intentionally stay outside the public OpenAPI contract and are mounted under
 * `/api/v1/internal/dsr`. The flow is preview-first:
 *
 * 1. `POST /preview` resolves and stores the exact manifest Support must review. This endpoint is
 *    read-only.
 * 2. `POST /{requestId}/execute` validates request id, email, DataGrail id, and on-call issue
 *    number, then performs the irreversible deletes.
 */
@Controller("/api/v1/internal/dsr")
@Context
@Secured(AuthRoleConstants.ADMIN)
open class DsrDeletionController(
  private val dsrDeletionService: DsrDeletionService,
  private val objectMapper: ObjectMapper,
) {
  @Post("/preview")
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuditLogging(provider = AuditLoggingProvider.ONLY_ACTOR)
  open fun preview(
    @Body request: DsrPreviewRequest,
  ): DsrPreviewResponse? =
    execute {
      log.warn {
        "DSR preview endpoint called: emailHash=${DsrDeletionService.emailHash(request.email)}, datagrailId=${request.datagrailId}, " +
          "oncallIssueNumber=${request.oncallIssueNumber}, requestedBy=${request.requestedBy}"
      }
      val result =
        dsrDeletionService.preview(
          email = request.email,
          datagrailId = request.datagrailId,
          oncallIssueNumber = request.oncallIssueNumber,
          requestedBy = request.requestedBy,
        )
      DsrPreviewResponse(
        requestId = result.requestId,
        status = result.status,
        manifest = result.manifest,
        warnings = result.warnings,
      )
    }

  @Post("/{requestId}/execute")
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuditLogging(provider = AuditLoggingProvider.ONLY_ACTOR)
  open fun execute(
    @PathVariable requestId: UUID,
    @Body request: DsrExecuteRequest,
  ): DsrExecuteResponse? =
    execute {
      log.warn {
        "DSR execute endpoint called: requestId=$requestId, emailHash=${DsrDeletionService.emailHash(request.email)}, " +
          "datagrailId=${request.datagrailId}, oncallIssueNumber=${request.oncallIssueNumber}, executedBy=${request.executedBy}"
      }
      val result =
        dsrDeletionService.execute(
          requestId = requestId,
          email = request.email,
          datagrailId = request.datagrailId,
          oncallIssueNumber = request.oncallIssueNumber,
          executedBy = request.executedBy,
        )
      DsrExecuteResponse(
        requestId = result.requestId,
        status = result.status,
        deletedJobsCount = result.deletedJobsCount,
        deletedAttemptsCount = result.deletedAttemptsCount,
        deletedConnectionsCount = result.deletedConnectionsCount,
        deletedActorsCount = result.deletedActorsCount,
        deletedBuilderProjectsCount = result.deletedBuilderProjectsCount,
        deletedPermissionsCount = result.deletedPermissionsCount,
        deletedWorkspacesCount = result.deletedWorkspacesCount,
        deletedOrganizationsCount = result.deletedOrganizationsCount,
        deletedAuthUsersCount = result.deletedAuthUsersCount,
        tombstonedUser = result.tombstonedUser,
        deletedKeycloakUserCount = result.deletedKeycloakUserCount,
        terminatedTemporalWorkflowCount = result.terminatedTemporalWorkflowCount,
        errors = result.errors,
      )
    }

  @Get("/{requestId}")
  @ExecuteOn(AirbyteTaskExecutors.IO)
  open fun getById(
    @PathVariable requestId: UUID,
  ): HttpResponse<DsrRequestRead> {
    val row = dsrDeletionService.get(requestId) ?: return HttpResponse.notFound()
    return HttpResponse.ok(row.toRead(objectMapper))
  }

  @Get
  @ExecuteOn(AirbyteTaskExecutors.IO)
  open fun listByEmail(
    @QueryValue email: String,
  ): DsrRequestListRead =
    DsrRequestListRead(
      requests = dsrDeletionService.listByEmail(email).map { it.toRead(objectMapper) },
    )

  @Post("/{requestId}/cancel")
  @Status(HttpStatus.OK)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuditLogging(provider = AuditLoggingProvider.ONLY_ACTOR)
  open fun cancel(
    @PathVariable requestId: UUID,
    @Body request: DsrCancelRequest,
  ): DsrRequestRead? =
    execute {
      dsrDeletionService.cancel(requestId, request.canceledBy).toRead(objectMapper)
    }

  @io.micronaut.http.annotation.Error(exception = DsrUserNotFoundException::class, global = false)
  fun userNotFound(e: DsrUserNotFoundException): HttpResponse<Map<String, String>> {
    log.warn { "DSR: user not found - ${e.message}" }
    return HttpResponse.notFound(mapOf("error" to (e.message ?: "User not found")))
  }

  @io.micronaut.http.annotation.Error(exception = DsrRequestNotFoundException::class, global = false)
  fun requestNotFound(e: DsrRequestNotFoundException): HttpResponse<Map<String, String>> {
    log.warn { "DSR: request not found - ${e.message}" }
    return HttpResponse.notFound(mapOf("error" to (e.message ?: "Request not found")))
  }

  @io.micronaut.http.annotation.Error(exception = DsrInvalidStateException::class, global = false)
  fun invalidState(e: DsrInvalidStateException): HttpResponse<Map<String, String>> {
    log.warn { "DSR: invalid state - ${e.message}" }
    return HttpResponse
      .status<Map<String, String>>(HttpStatus.CONFLICT)
      .body(mapOf("error" to (e.message ?: "Invalid state")))
  }

  @io.micronaut.http.annotation.Error(exception = DsrInvalidConfirmationException::class, global = false)
  fun invalidConfirmation(e: DsrInvalidConfirmationException): HttpResponse<Map<String, String>> {
    log.warn { "DSR: invalid confirmation - ${e.message}" }
    return HttpResponse
      .status<Map<String, String>>(HttpStatus.BAD_REQUEST)
      .body(mapOf("error" to (e.message ?: "Invalid confirmation")))
  }
}

@Introspected
data class DsrPreviewRequest(
  @JsonProperty("email") val email: String,
  @JsonProperty("datagrail_id") val datagrailId: String,
  @JsonProperty("oncall_issue_number") val oncallIssueNumber: String,
  @JsonProperty("requested_by") val requestedBy: String,
)

@Introspected
data class DsrPreviewResponse(
  @JsonProperty("request_id") val requestId: UUID?,
  @JsonProperty("status") val status: String,
  @JsonProperty("manifest") val manifest: DsrManifest,
  @JsonProperty("warnings") val warnings: List<String>,
)

@Introspected
data class DsrExecuteRequest(
  @JsonProperty("email") val email: String,
  @JsonProperty("datagrail_id") val datagrailId: String,
  @JsonProperty("oncall_issue_number") val oncallIssueNumber: String,
  @JsonProperty("executed_by") val executedBy: String,
)

@Introspected
data class DsrExecuteResponse(
  @JsonProperty("request_id") val requestId: UUID,
  @JsonProperty("status") val status: String,
  @JsonProperty("deleted_jobs_count") val deletedJobsCount: Int,
  @JsonProperty("deleted_attempts_count") val deletedAttemptsCount: Int,
  @JsonProperty("deleted_connections_count") val deletedConnectionsCount: Int,
  @JsonProperty("deleted_actors_count") val deletedActorsCount: Int,
  @JsonProperty("deleted_builder_projects_count") val deletedBuilderProjectsCount: Int,
  @JsonProperty("deleted_permissions_count") val deletedPermissionsCount: Int,
  @JsonProperty("deleted_workspaces_count") val deletedWorkspacesCount: Int,
  @JsonProperty("deleted_organizations_count") val deletedOrganizationsCount: Int,
  @JsonProperty("deleted_auth_users_count") val deletedAuthUsersCount: Int,
  @JsonProperty("tombstoned_user") val tombstonedUser: Boolean,
  @JsonProperty("deleted_keycloak_user_count") val deletedKeycloakUserCount: Int,
  @JsonProperty("terminated_temporal_workflow_count") val terminatedTemporalWorkflowCount: Int,
  @JsonProperty("errors") val errors: List<String>,
)

@Introspected
data class DsrCancelRequest(
  @JsonProperty("canceled_by") val canceledBy: String,
)

@Introspected
data class DsrRequestRead(
  @JsonProperty("request_id") val requestId: UUID?,
  @JsonProperty("email") val email: String,
  @JsonProperty("datagrail_id") val datagrailId: String,
  @JsonProperty("status") val status: String,
  @JsonProperty("user_id") val userId: UUID?,
  @JsonProperty("requested_by") val requestedBy: String,
  @JsonProperty("oncall_issue_number") val oncallIssueNumber: String,
  @JsonProperty("confirmed_by") val confirmedBy: String?,
  @JsonProperty("manifest") val manifest: JsonNode,
  @JsonProperty("preview_warnings") val previewWarnings: JsonNode?,
  @JsonProperty("execution_errors") val executionErrors: JsonNode?,
  @JsonProperty("previewed_at") val previewedAt: OffsetDateTime?,
  @JsonProperty("executed_at") val executedAt: OffsetDateTime?,
  @JsonProperty("completed_at") val completedAt: OffsetDateTime?,
  @JsonProperty("updated_at") val updatedAt: OffsetDateTime?,
)

@Introspected
data class DsrRequestListRead(
  @JsonProperty("requests") val requests: List<DsrRequestRead>,
)

private fun DataSubjectDeletionRequest.toRead(objectMapper: ObjectMapper): DsrRequestRead =
  DsrRequestRead(
    requestId = id,
    email = email,
    datagrailId = datagrailId,
    status = status.literal.uppercase(),
    userId = userId,
    requestedBy = requestedBy,
    oncallIssueNumber = oncallIssueNumber,
    confirmedBy = confirmedBy,
    manifest = objectMapper.readTree(manifest),
    previewWarnings = prepareWarnings?.let { objectMapper.readTree(it) },
    executionErrors = confirmErrors?.let { objectMapper.readTree(it) },
    previewedAt = preparedAt,
    executedAt = confirmedAt,
    completedAt = completedAt,
    updatedAt = updatedAt,
  )
