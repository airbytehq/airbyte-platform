/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.auth

import io.airbyte.api.problems.throwable.generated.BadRequestProblem
import io.airbyte.api.server.generated.models.SignalInput
import io.airbyte.commons.auth.roles.AuthRoleConstants
import io.airbyte.commons.auth.route.OperationInput
import io.airbyte.commons.auth.route.OperationRule
import io.airbyte.commons.server.handlers.dsr.DsrRequestNotFoundException
import io.airbyte.commons.server.support.CurrentUserService
import io.airbyte.domain.services.auth.DsrAuthorizationDataService
import io.airbyte.server.apis.controllers.DsrExecuteRequest
import io.airbyte.server.apis.controllers.DsrForbiddenActorException
import io.airbyte.server.apis.controllers.DsrPreviewRequest
import io.airbyte.server.apis.controllers.ForceCleanupConnectionsRequestBody
import io.micronaut.security.authentication.Authentication
import jakarta.inject.Singleton
import java.util.Locale
import java.util.Optional
import java.util.UUID

data class RecoveryConnectionsInput(
  val connections: List<UUID>,
) : OperationInput

data class TerminalSignalInput(
  val workflowType: String,
  val workflowId: String,
) : OperationInput

data class DsrConfirmation(
  val email: String,
  val datagrailId: String,
  val oncallIssue: String,
)

data class DsrPreviewInput(
  val confirmation: DsrConfirmation,
) : OperationInput

data class DsrSavedInput(
  val requestId: UUID,
  val confirmation: Optional<DsrConfirmation>,
) : OperationInput

data class DsrEmailFilterInput(
  val email: String,
) : OperationInput

@Singleton
class ForceCleanupConnectionsRule(
  private val permissions: OperationPermissions,
) : OperationRule {
  override fun readInput(args: Array<out Any?>): OperationInput {
    val body = args.singleOrNull() as? ForceCleanupConnectionsRequestBody ?: throw BadRequestProblem()
    val ids: List<UUID?> = body.connectionIds
    return RecoveryConnectionsInput(ids.map { it ?: throw BadRequestProblem() })
  }

  override fun authorize(
    caller: Authentication,
    input: OperationInput,
  ) {
    permissions.requireGlobal(caller, setOf(AuthRoleConstants.ADMIN), organizationCallers)
  }
}

@Singleton
class TerminalWorkflowSignalRule(
  private val permissions: OperationPermissions,
) : OperationRule {
  override fun readInput(args: Array<out Any?>): OperationInput {
    val body = args.singleOrNull() as? SignalInput ?: throw BadRequestProblem()
    return TerminalSignalInput(body.workflowType, body.workflowId)
  }

  override fun authorize(
    caller: Authentication,
    input: OperationInput,
  ) {
    permissions.requireGlobal(
      caller,
      setOf(AuthRoleConstants.ADMIN),
      setOf(OperationCaller.INTERNAL_CLIENT, OperationCaller.DATAPLANE, OperationCaller.SERVICE_ACCOUNT),
    )
  }
}

abstract class DsrAdminRule(
  private val permissions: OperationPermissions,
  private val currentUser: CurrentUserService,
) : OperationRule {
  protected fun requireActor(caller: Authentication) {
    permissions.requireGlobal(caller, setOf(AuthRoleConstants.ADMIN), organizationCallers)
    val email: String? = currentUser.getCurrentUser().email
    val normalized = email?.trim()?.lowercase(Locale.US).orEmpty()
    if (normalized.isBlank() || normalized.substringAfterLast('@', "") !in setOf("airbyte.io", "airbyte.com")) {
      throw DsrForbiddenActorException("DSR endpoints require an authenticated Airbyte actor.")
    }
  }
}

@Singleton
class DsrPreviewRule(
  permissions: OperationPermissions,
  currentUser: CurrentUserService,
) : DsrAdminRule(permissions, currentUser) {
  override fun readInput(args: Array<out Any?>): OperationInput {
    val body = args.singleOrNull() as? DsrPreviewRequest ?: throw BadRequestProblem()
    return DsrPreviewInput(DsrConfirmation(body.email, body.datagrailId, body.oncallIssueNumber))
  }

  override fun authorize(
    caller: Authentication,
    input: OperationInput,
  ) = requireActor(caller)
}

@Singleton
class DsrSavedRequestRule(
  private val data: DsrAuthorizationDataService,
  permissions: OperationPermissions,
  currentUser: CurrentUserService,
) : DsrAdminRule(permissions, currentUser) {
  override fun readInput(args: Array<out Any?>): OperationInput {
    val id = args.firstOrNull() as? UUID ?: throw BadRequestProblem()
    val confirmation =
      when {
        args.size == 1 -> Optional.empty()
        args.size == 2 && args[1] is DsrExecuteRequest ->
          (args[1] as DsrExecuteRequest).let {
            Optional.of(DsrConfirmation(it.email, it.datagrailId, it.oncallIssueNumber))
          }
        else -> throw BadRequestProblem()
      }
    return DsrSavedInput(id, confirmation)
  }

  override fun authorize(
    caller: Authentication,
    input: OperationInput,
  ) {
    requireActor(caller)
    val id = (input as DsrSavedInput).requestId
    if (!data.exists(id)) throw DsrRequestNotFoundException("No DSR deletion request with id $id")
  }
}

@Singleton
class DsrEmailFilterRule(
  permissions: OperationPermissions,
  currentUser: CurrentUserService,
) : DsrAdminRule(permissions, currentUser) {
  override fun readInput(args: Array<out Any?>): OperationInput = DsrEmailFilterInput(args.singleOrNull() as? String ?: throw BadRequestProblem())

  override fun authorize(
    caller: Authentication,
    input: OperationInput,
  ) = requireActor(caller)
}
