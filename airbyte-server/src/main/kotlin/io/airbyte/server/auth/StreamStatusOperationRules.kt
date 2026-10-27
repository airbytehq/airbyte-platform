/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.auth

import io.airbyte.api.problems.throwable.generated.BadRequestProblem
import io.airbyte.api.server.generated.models.ConnectionIdRequestBody
import io.airbyte.api.server.generated.models.StreamStatusCreateRequestBody
import io.airbyte.api.server.generated.models.StreamStatusJobType
import io.airbyte.api.server.generated.models.StreamStatusListRequestBody
import io.airbyte.api.server.generated.models.StreamStatusUpdateRequestBody
import io.airbyte.commons.auth.roles.AuthRoleConstants
import io.airbyte.commons.auth.route.OperationInput
import io.airbyte.commons.auth.route.OperationRule
import io.airbyte.domain.models.auth.WorkspaceAndOrganization
import io.airbyte.domain.services.auth.AuthorizationDataService
import io.airbyte.domain.services.auth.StreamStatusAuthorizationDataService
import io.micronaut.security.authentication.Authentication
import jakarta.inject.Singleton
import java.util.Locale
import java.util.Optional
import java.util.UUID

data class StreamStatusWriteInput(
  val workspaceId: UUID,
  val connectionId: UUID,
  val jobId: Long,
  val attemptNumber: Int,
  val jobType: StreamStatusJobType,
) : OperationInput

private fun requireStreamStatusWrite(
  data: AuthorizationDataService,
  permissions: OperationPermissions,
  caller: Authentication,
  request: StreamStatusWriteInput,
): WorkspaceAndOrganization {
  val owner = data.workspaceAndOrganization(request.workspaceId)
  permissions.requireWorkspaces(
    caller,
    listOf(owner),
    setOf(AuthRoleConstants.ADMIN, AuthRoleConstants.DATAPLANE),
    setOf(OperationCaller.INTERNAL_CLIENT, OperationCaller.DATAPLANE, OperationCaller.SERVICE_ACCOUNT),
  )
  if (connectionOwner(data, request.connectionId) != owner) throw BadRequestProblem()
  if (data.savedAttemptConnectionId(request.jobId, request.attemptNumber) != request.connectionId) throw BadRequestProblem()
  return owner
}

@Singleton
class StreamStatusCreateRule(
  private val data: AuthorizationDataService,
  private val permissions: OperationPermissions,
) : OperationRule {
  override fun readInput(args: Array<out Any?>): StreamStatusWriteInput {
    val body = args.singleOrNull() as? StreamStatusCreateRequestBody ?: throw BadRequestProblem()
    return StreamStatusWriteInput(body.workspaceId, body.connectionId, body.jobId, body.attemptNumber, body.jobType)
  }

  override fun authorize(
    caller: Authentication,
    input: OperationInput,
  ) {
    requireStreamStatusWrite(data, permissions, caller, input as StreamStatusWriteInput)
  }
}

data class StreamStatusUpdateInput(
  val id: UUID,
  val resource: StreamStatusWriteInput,
) : OperationInput

@Singleton
class StreamStatusUpdateRule(
  private val data: AuthorizationDataService,
  private val permissions: OperationPermissions,
  private val statuses: StreamStatusAuthorizationDataService,
) : OperationRule {
  override fun readInput(args: Array<out Any?>): StreamStatusUpdateInput {
    val body = args.singleOrNull() as? StreamStatusUpdateRequestBody ?: throw BadRequestProblem()
    return StreamStatusUpdateInput(body.id, StreamStatusWriteInput(body.workspaceId, body.connectionId, body.jobId, body.attemptNumber, body.jobType))
  }

  override fun authorize(
    caller: Authentication,
    input: OperationInput,
  ) {
    val request = input as StreamStatusUpdateInput
    val resource = request.resource
    val owner = requireStreamStatusWrite(data, permissions, caller, resource)
    val saved = statuses.status(request.id, owner.workspaceId)
    if (saved.connectionId != resource.connectionId || saved.jobId != resource.jobId ||
      saved.attemptNumber != resource.attemptNumber || saved.jobType != resource.jobType.value.lowercase(Locale.ROOT)
    ) {
      throw BadRequestProblem()
    }
  }
}

data class StreamStatusListInput(
  val workspaceId: UUID,
  val connectionId: Optional<UUID>,
  val jobId: Optional<Long>,
  val attemptNumber: Optional<Int>,
) : OperationInput

@Singleton
class StreamStatusListRule(
  private val data: AuthorizationDataService,
  private val permissions: OperationPermissions,
) : OperationRule {
  override fun readInput(args: Array<out Any?>): StreamStatusListInput {
    val body = args.singleOrNull() as? StreamStatusListRequestBody ?: throw BadRequestProblem()
    return StreamStatusListInput(
      body.workspaceId,
      Optional.ofNullable(body.connectionId),
      Optional.ofNullable(body.jobId),
      Optional.ofNullable(body.attemptNumber),
    )
  }

  override fun authorize(
    caller: Authentication,
    input: OperationInput,
  ) {
    val request = input as StreamStatusListInput
    val owner = data.workspaceAndOrganization(request.workspaceId)
    permissions.requireWorkspaces(caller, listOf(owner), workspaceReadRoles, organizationCallers)
    request.connectionId.ifPresent { if (connectionOwner(data, it) != owner) throw BadRequestProblem() }
    request.jobId.ifPresent { job ->
      val connection =
        if (request.attemptNumber.isPresent) {
          data.savedAttemptConnectionId(
            job,
            request.attemptNumber.get(),
          )
        } else {
          data.replicationJobConnectionId(job)
        }
      if (request.connectionId.isPresent && request.connectionId.get() != connection) throw BadRequestProblem()
      if (connectionOwner(data, connection) != owner) throw BadRequestProblem()
    }
  }
}

data class StreamStatusConnectionInput(
  val connectionId: UUID,
) : OperationInput

@Singleton
class StreamStatusRunStateRule(
  private val data: AuthorizationDataService,
  private val permissions: OperationPermissions,
) : OperationRule {
  override fun readInput(args: Array<out Any?>): StreamStatusConnectionInput {
    val body = args.singleOrNull() as? ConnectionIdRequestBody ?: throw BadRequestProblem()
    return StreamStatusConnectionInput(body.connectionId)
  }

  override fun authorize(
    caller: Authentication,
    input: OperationInput,
  ) {
    val owner = connectionOwner(data, (input as StreamStatusConnectionInput).connectionId)
    permissions.requireWorkspaces(caller, listOf(owner), workspaceReadRoles, organizationCallers)
  }
}
