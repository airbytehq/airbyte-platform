/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.auth

import io.airbyte.api.problems.throwable.generated.BadRequestProblem
import io.airbyte.api.problems.throwable.generated.ForbiddenProblem
import io.airbyte.api.server.generated.models.WorkloadOutputWriteRequest
import io.airbyte.commons.auth.roles.AuthRoleConstants
import io.airbyte.commons.auth.route.OperationInput
import io.airbyte.commons.auth.route.OperationRule
import io.airbyte.domain.models.auth.AuthorizationTarget
import io.airbyte.domain.services.auth.SavedExecutionAuthorizationDataService
import io.micronaut.security.authentication.Authentication
import jakarta.inject.Singleton

data class WorkloadOutputInput(
  val workload: String,
) : OperationInput

@Singleton
class WorkloadOutputRule(
  private val data: SavedExecutionAuthorizationDataService,
  private val permissions: OperationPermissions,
) : OperationRule {
  override fun readInput(args: Array<out Any?>): OperationInput {
    val body = args.singleOrNull() as? WorkloadOutputWriteRequest ?: throw BadRequestProblem()
    return WorkloadOutputInput(body.workloadId.takeUnless(String::isBlank) ?: throw BadRequestProblem())
  }

  override fun authorize(
    caller: Authentication,
    input: OperationInput,
  ) {
    val callers = setOf(OperationCaller.INTERNAL_CLIENT, OperationCaller.DATAPLANE, OperationCaller.SERVICE_ACCOUNT, OperationCaller.WORKLOAD_API)
    when (val target = data.workload((input as WorkloadOutputInput).workload)) {
      is AuthorizationTarget.Workspace ->
        permissions.requireWorkspaces(
          caller,
          listOf(target.workspaceAndOrganization),
          setOf(AuthRoleConstants.DATAPLANE),
          callers,
        )
      is AuthorizationTarget.Organization ->
        permissions.requireOrganizations(
          caller,
          listOf(target.organizationId),
          setOf(AuthRoleConstants.DATAPLANE),
          callers,
        )
      AuthorizationTarget.Global -> throw ForbiddenProblem()
    }
  }
}
