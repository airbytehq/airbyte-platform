/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.auth

import io.airbyte.api.problems.throwable.generated.BadRequestProblem
import io.airbyte.api.server.generated.models.SaveStatsRequestBody
import io.airbyte.api.server.generated.models.SaveStreamAttemptMetadataRequestBody
import io.airbyte.commons.auth.roles.AuthRoleConstants
import io.airbyte.commons.auth.route.OperationInput
import io.airbyte.commons.auth.route.OperationRule
import io.airbyte.domain.services.auth.AuthorizationDataService
import io.micronaut.security.authentication.Authentication
import jakarta.inject.Singleton
import java.util.Optional
import java.util.UUID

data class SaveAttemptStatsInput(
  val jobId: Long,
  val attemptNumber: Int,
  val connectionId: Optional<UUID>,
) : OperationInput

data class SaveAttemptStreamMetadataInput(
  val jobId: Long,
  val attemptNumber: Int,
) : OperationInput

@Singleton
class SaveAttemptStatsRule(
  private val data: AuthorizationDataService,
  private val permissions: OperationPermissions,
) : OperationRule {
  override fun readInput(args: Array<out Any?>): SaveAttemptStatsInput {
    val body = args.singleOrNull() as? SaveStatsRequestBody ?: throw BadRequestProblem()
    return SaveAttemptStatsInput(
      body.jobId,
      body.attemptNumber,
      Optional.ofNullable(body.connectionId),
    )
  }

  override fun authorize(
    caller: Authentication,
    input: OperationInput,
  ) {
    val request = input as SaveAttemptStatsInput
    val connectionId = data.savedAttemptConnectionId(request.jobId, request.attemptNumber)
    if (request.connectionId.isPresent && request.connectionId.get() != connectionId) throw BadRequestProblem()
    requireAttemptWorker(caller, connectionId, data, permissions)
  }
}

@Singleton
class SaveAttemptStreamMetadataRule(
  private val data: AuthorizationDataService,
  private val permissions: OperationPermissions,
) : OperationRule {
  override fun readInput(args: Array<out Any?>): SaveAttemptStreamMetadataInput {
    val body = args.singleOrNull() as? SaveStreamAttemptMetadataRequestBody ?: throw BadRequestProblem()
    return SaveAttemptStreamMetadataInput(body.jobId, body.attemptNumber)
  }

  override fun authorize(
    caller: Authentication,
    input: OperationInput,
  ) {
    val request = input as SaveAttemptStreamMetadataInput
    val connectionId = data.savedAttemptConnectionId(request.jobId, request.attemptNumber)
    requireAttemptWorker(caller, connectionId, data, permissions)
  }
}

private fun requireAttemptWorker(
  caller: Authentication,
  connectionId: UUID,
  data: AuthorizationDataService,
  permissions: OperationPermissions,
) {
  val owner = data.savedConnectionWorkspaceAndOrganization(connectionId)
  permissions.requireWorkspaces(
    caller,
    listOf(owner),
    setOf(AuthRoleConstants.DATAPLANE),
    // Internal clients retain their existing privilege; accounts need current saved grants.
    setOf(OperationCaller.INTERNAL_CLIENT, OperationCaller.DATAPLANE, OperationCaller.SERVICE_ACCOUNT),
  )
}
