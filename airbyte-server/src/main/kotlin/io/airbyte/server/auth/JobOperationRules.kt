/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.auth

import io.airbyte.api.problems.throwable.generated.BadRequestProblem
import io.airbyte.api.problems.throwable.generated.ForbiddenProblem
import io.airbyte.api.server.generated.models.CancelQueuedJobRequest
import io.airbyte.api.server.generated.models.CheckDataWorkerCapacityRequest
import io.airbyte.api.server.generated.models.ConnectionIdRequestBody
import io.airbyte.api.server.generated.models.ConnectionJobRequestBody
import io.airbyte.api.server.generated.models.CreateNewAttemptNumberRequest
import io.airbyte.api.server.generated.models.DeleteStreamResetRecordsForJobRequest
import io.airbyte.api.server.generated.models.FailAttemptRequest
import io.airbyte.api.server.generated.models.GetAttemptStatsRequestBody
import io.airbyte.api.server.generated.models.GetWebhookConfigRequest
import io.airbyte.api.server.generated.models.JobConfigType
import io.airbyte.api.server.generated.models.JobCreate
import io.airbyte.api.server.generated.models.JobFailureRequest
import io.airbyte.api.server.generated.models.JobIdRequestBody
import io.airbyte.api.server.generated.models.JobListForWorkspacesRequestBody
import io.airbyte.api.server.generated.models.JobListRequestBody
import io.airbyte.api.server.generated.models.JobRetryStateRequestBody
import io.airbyte.api.server.generated.models.JobSuccessWithAttemptNumberRequest
import io.airbyte.api.server.generated.models.PersistCancelJobRequestBody
import io.airbyte.api.server.generated.models.ReportJobStartRequest
import io.airbyte.api.server.generated.models.SaveAttemptSyncConfigRequestBody
import io.airbyte.api.server.generated.models.SetJobQueuedRequest
import io.airbyte.api.server.generated.models.SyncInput
import io.airbyte.commons.auth.generated.Intent
import io.airbyte.commons.auth.roles.AuthRoleConstants
import io.airbyte.commons.auth.route.OperationInput
import io.airbyte.commons.auth.route.OperationRule
import io.airbyte.domain.services.auth.AuthorizationDataService
import io.micronaut.security.authentication.Authentication
import jakarta.inject.Singleton
import java.util.Optional
import java.util.UUID

sealed interface JobOperationInput : OperationInput {
  data class Saved(
    val jobId: Long,
    val attemptNumber: Optional<Int> = Optional.empty(),
    val connectionId: Optional<UUID> = Optional.empty(),
    val organizationId: Optional<UUID> = Optional.empty(),
  ) : JobOperationInput

  data class Connection(
    val connectionId: UUID,
  ) : JobOperationInput

  data class Workspaces(
    val workspaceIds: List<UUID>,
  ) : JobOperationInput

  data object GlobalList : JobOperationInput
}

private fun jobInput(args: Array<out Any?>): JobOperationInput =
  when (val body = args.singleOrNull()) {
    is JobIdRequestBody -> {
      if (body.id <= 0) throw BadRequestProblem()
      JobOperationInput.Saved(body.id)
    }
    is GetWebhookConfigRequest -> JobOperationInput.Saved(body.jobId)
    is CreateNewAttemptNumberRequest -> JobOperationInput.Saved(body.jobId)
    is SetJobQueuedRequest -> JobOperationInput.Saved(body.jobId)
    is CancelQueuedJobRequest -> JobOperationInput.Saved(body.jobId)
    // reportJobStart does not consume its legacy connectionId tracking field.
    is ReportJobStartRequest -> JobOperationInput.Saved(body.jobId)
    is JobCreate -> JobOperationInput.Connection(body.connectionId)
    is ConnectionIdRequestBody -> JobOperationInput.Connection(body.connectionId)
    is JobFailureRequest ->
      JobOperationInput.Saved(
        body.jobId,
        connectionId =
          Optional.of(body.connectionId),
      )
    is ConnectionJobRequestBody ->
      JobOperationInput.Saved(
        body.jobId,
        connectionId =
          Optional.of(body.connectionId),
      )
    is DeleteStreamResetRecordsForJobRequest ->
      JobOperationInput.Saved(
        body.jobId,
        connectionId =
          Optional.of(body.connectionId),
      )
    is JobRetryStateRequestBody -> JobOperationInput.Saved(body.jobId, connectionId = Optional.of(body.connectionId))
    is CheckDataWorkerCapacityRequest ->
      JobOperationInput.Saved(
        body.jobId,
        connectionId = Optional.of(body.connectionId),
        organizationId = Optional.of(body.organizationId),
      )
    is SyncInput -> JobOperationInput.Saved(body.jobId, Optional.of(body.attemptNumber))
    is GetAttemptStatsRequestBody ->
      JobOperationInput.Saved(
        body.jobId,
        Optional.of(body.attemptNumber),
      )
    is SaveAttemptSyncConfigRequestBody ->
      JobOperationInput.Saved(
        body.jobId,
        Optional.of(body.attemptNumber),
      )
    is FailAttemptRequest ->
      JobOperationInput.Saved(
        body.jobId,
        Optional.of(body.attemptNumber),
      )
    is JobSuccessWithAttemptNumberRequest ->
      JobOperationInput.Saved(
        body.jobId,
        Optional.of(body.attemptNumber),
        Optional.of(body.connectionId),
      )
    is PersistCancelJobRequestBody ->
      JobOperationInput.Saved(
        body.jobId,
        Optional.of(body.attemptNumber),
        Optional.of(body.connectionId),
      )
    is JobListForWorkspacesRequestBody -> {
      requireConfigTypes(body.configTypes)
      JobOperationInput.Workspaces(copyIds(body.workspaceIds))
    }
    is JobListRequestBody -> {
      val types = requireConfigTypes(body.configTypes)
      val configId = body.configId
      if (configId == null || types.any { it !in replicationJobTypes }) {
        JobOperationInput.GlobalList
      } else {
        // The includingJobId anchor is queried inside this same scope; a foreign/missing anchor returns an empty page.
        JobOperationInput.Connection(
          try {
            UUID.fromString(configId)
          } catch (_: IllegalArgumentException) {
            throw BadRequestProblem()
          },
        )
      }
    }
    else -> throw BadRequestProblem()
  }

private val replicationJobTypes =
  io.airbyte.config.Job.REPLICATION_TYPES
    .map { JobConfigType.valueOf(it.name) }
    .toSet()

private val jobReaders = setOf(OperationCaller.USER, OperationCaller.INTERNAL_CLIENT, OperationCaller.DATAPLANE, OperationCaller.SERVICE_ACCOUNT)
private val jobWorkers = setOf(OperationCaller.INTERNAL_CLIENT, OperationCaller.DATAPLANE, OperationCaller.SERVICE_ACCOUNT)

private fun requireConfigTypes(types: List<JobConfigType?>?): List<JobConfigType> {
  requireNotNull(types) { "configType cannot be null." }
  check(types.isNotEmpty()) { "Must include at least one configType." }
  return types.map { it ?: throw BadRequestProblem() }.toList()
}

abstract class JobOperationRule(
  private val data: AuthorizationDataService,
  private val permissions: OperationPermissions,
  private val roles: Set<String>,
  private val callers: Set<OperationCaller> = jobReaders,
) : OperationRule {
  override fun readInput(args: Array<out Any?>): JobOperationInput = jobInput(args)

  override fun authorize(
    caller: Authentication,
    input: OperationInput,
  ) {
    when (val request = input as JobOperationInput) {
      is JobOperationInput.Saved -> {
        val connection =
          if (request.attemptNumber.isPresent) {
            data.savedAttemptConnectionId(
              request.jobId,
              request.attemptNumber.get(),
            )
          } else {
            data.replicationJobConnectionId(request.jobId)
          }
        val owner = connectionOwner(data, connection)
        permissions.requireWorkspaces(caller, listOf(owner), roles, callers)
        request.connectionId.ifPresent { if (it != connection) throw BadRequestProblem() }
        request.organizationId.ifPresent { if (it != owner.organizationId) throw BadRequestProblem() }
      }
      is JobOperationInput.Connection -> permissions.requireWorkspaces(caller, listOf(connectionOwner(data, request.connectionId)), roles, callers)
      is JobOperationInput.Workspaces -> {
        if (request.workspaceIds.isEmpty()) {
          permissions.requireGlobal(caller, roles, callers)
        } else {
          permissions.requireWorkspaces(caller, request.workspaceIds.map(data::workspaceAndOrganization), roles, callers)
        }
      }
      JobOperationInput.GlobalList -> permissions.requireGlobal(caller, roles, callers)
    }
  }
}

@Singleton
class JobReadRule(
  data: AuthorizationDataService,
  permissions: OperationPermissions,
) : JobOperationRule(data, permissions, workspaceReadRoles)

data class JobExplainUnsupportedInput(
  val job: Optional<Long>,
) : OperationInput

/** Job ownership is needed only for a workspace-scoped action check in the static OSS explanation. */
@Singleton
class JobExplainUnsupportedRule(
  private val data: AuthorizationDataService,
  private val permissions: OperationPermissions,
) : OperationRule {
  override fun readInput(args: Array<out Any?>): OperationInput {
    val body = args.singleOrNull() as? JobIdRequestBody ?: throw BadRequestProblem()
    return JobExplainUnsupportedInput(Optional.ofNullable(body.id))
  }

  override fun authorize(
    caller: Authentication,
    input: OperationInput,
  ) {
    try {
      permissions.requireGlobal(caller, workspaceReadRoles, jobReaders)
      return
    } catch (_: ForbiddenProblem) {
      // Only a workspace-scoped reader needs the job's saved workspace to evaluate its action.
    }
    val job = (input as JobExplainUnsupportedInput).job.orElseThrow { ForbiddenProblem() }
    val owner = connectionOwner(data, data.replicationJobConnectionId(job))
    permissions.requireWorkspaces(caller, listOf(owner), workspaceReadRoles, jobReaders)
  }
}

@Singleton
class JobReadOrWorkerRule(
  data: AuthorizationDataService,
  permissions: OperationPermissions,
) : JobOperationRule(
    data,
    permissions,
    workspaceReadRoles + AuthRoleConstants.DATAPLANE,
  )

@Singleton
class JobRunRule(
  data: AuthorizationDataService,
  permissions: OperationPermissions,
) : JobOperationRule(data, permissions, Intent.RunAndCancelConnectionSyncAndRefresh.roles)

@Singleton
class JobCreateRule(
  data: AuthorizationDataService,
  permissions: OperationPermissions,
) : JobOperationRule(data, permissions, Intent.RunAndCancelConnectionSyncAndRefresh.roles, jobWorkers)

@Singleton
class JobInputRule(
  data: AuthorizationDataService,
  permissions: OperationPermissions,
) : JobOperationRule(
    data,
    permissions,
    workspaceReadRoles + AuthRoleConstants.DATAPLANE,
    jobWorkers,
  )

@Singleton
class JobAdminRule(
  data: AuthorizationDataService,
  permissions: OperationPermissions,
) : JobOperationRule(data, permissions, setOf(AuthRoleConstants.ADMIN), jobWorkers)

@Singleton
class JobAdminReadRule(
  data: AuthorizationDataService,
  permissions: OperationPermissions,
) : JobOperationRule(data, permissions, setOf(AuthRoleConstants.ADMIN))
