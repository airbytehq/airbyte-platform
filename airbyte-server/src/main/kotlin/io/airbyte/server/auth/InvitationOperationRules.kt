/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.auth

import io.airbyte.api.problems.throwable.generated.BadRequestProblem
import io.airbyte.api.server.generated.models.InviteCodeRequestBody
import io.airbyte.api.server.generated.models.ScopeType
import io.airbyte.api.server.generated.models.UserInvitationCancelRequestBody
import io.airbyte.api.server.generated.models.UserInvitationCreateRequestBody
import io.airbyte.api.server.generated.models.UserInvitationListRequestBody
import io.airbyte.commons.auth.route.OperationInput
import io.airbyte.commons.auth.route.OperationRule
import io.airbyte.commons.server.errors.AuthException
import io.airbyte.commons.server.errors.OperationNotAllowedException
import io.airbyte.domain.models.auth.AuthorizationTarget
import io.airbyte.domain.services.auth.AuthorizationDataService
import io.airbyte.domain.services.auth.AuthorizationUserIdentity
import io.airbyte.domain.services.auth.MembershipAuthorizationDataService
import io.micronaut.context.annotation.Value
import io.micronaut.security.authentication.Authentication
import jakarta.inject.Singleton
import java.util.UUID

sealed interface InvitationOperationInput : OperationInput {
  data class Scope(
    val type: ScopeType,
    val id: UUID,
  ) : InvitationOperationInput

  data class Code(
    val value: String,
  ) : InvitationOperationInput

  data class Id(
    val value: UUID,
  ) : InvitationOperationInput
}

private fun invitationInput(args: Array<out Any?>): InvitationOperationInput =
  when (val body = args.singleOrNull()) {
    is UserInvitationCreateRequestBody -> InvitationOperationInput.Scope(body.scopeType, body.scopeId)
    is UserInvitationListRequestBody -> InvitationOperationInput.Scope(body.scopeType, body.scopeId)
    is InviteCodeRequestBody -> InvitationOperationInput.Code(body.inviteCode)
    is String -> InvitationOperationInput.Code(body)
    is UserInvitationCancelRequestBody -> {
      val id = body.id
      val code = body.inviteCode
      when {
        id != null -> InvitationOperationInput.Id(id)
        code != null -> InvitationOperationInput.Code(code)
        else -> throw BadRequestProblem()
      }
    }
    else -> throw BadRequestProblem()
  }

private fun invitationTarget(
  data: AuthorizationDataService,
  input: InvitationOperationInput.Scope,
): AuthorizationTarget =
  when (input.type) {
    ScopeType.WORKSPACE -> AuthorizationTarget.Workspace(data.workspaceAndOrganization(input.id))
    ScopeType.ORGANIZATION -> AuthorizationTarget.Organization(data.organization(input.id))
  }

private fun invitationIdentity(
  membership: MembershipAuthorizationDataService,
  caller: Authentication,
  security: Boolean,
): AuthorizationUserIdentity =
  try {
    if (security) membership.currentIdentity(caller.name) else membership.defaultIdentity()
  } catch (failure: Exception) {
    // Match the configured CurrentUserService's existing error contract before any business call.
    if (security) throw AuthException("Could not get the current Airbyte user due to an internal error.", failure)
    throw RuntimeException("Could not get the current user due to an internal error.", failure)
  }

@Singleton
class InvitationScopeReadRule(
  private val data: AuthorizationDataService,
  private val permissions: OperationPermissions,
) : OperationRule {
  override fun readInput(args: Array<out Any?>): InvitationOperationInput = invitationInput(args)

  override fun authorize(
    caller: Authentication,
    input: OperationInput,
  ) {
    requireAuthorizationTarget(permissions, caller, invitationTarget(data, input as InvitationOperationInput.Scope), workspaceReadRoles)
  }
}

@Singleton
class InvitationScopeAdminRule(
  private val data: AuthorizationDataService,
  private val membership: MembershipAuthorizationDataService,
  private val permissions: OperationPermissions,
  @Value("\${micronaut.security.enabled:false}") private val security: Boolean,
) : OperationRule {
  override fun readInput(args: Array<out Any?>): InvitationOperationInput = invitationInput(args)

  override fun authorize(
    caller: Authentication,
    input: OperationInput,
  ) {
    requireAuthorizationTarget(
      permissions,
      caller,
      invitationTarget(data, input as InvitationOperationInput.Scope),
      permissionAdminRoles,
      setOf(OperationCaller.USER),
    )
    invitationIdentity(membership, caller, security)
  }
}

abstract class InvitationRecipientRule(
  private val membership: MembershipAuthorizationDataService,
  private val permissions: OperationPermissions,
  private val security: Boolean,
  private val ignoreCase: Boolean,
) : OperationRule {
  override fun readInput(args: Array<out Any?>): InvitationOperationInput = invitationInput(args)

  override fun authorize(
    caller: Authentication,
    input: OperationInput,
  ) {
    permissions.requireAuthenticated(caller, setOf(OperationCaller.USER))
    val identity = invitationIdentity(membership, caller, security)
    val invitation = membership.invitationByCode((input as InvitationOperationInput.Code).value)
    if (!identity.email
        .map {
          invitation.email.equals(it, ignoreCase)
        }.orElse(false)
    ) {
      throw OperationNotAllowedException("Invited email does not match current user email.")
    }
  }
}

@Singleton
class InvitationReadRule(
  membership: MembershipAuthorizationDataService,
  permissions: OperationPermissions,
  @Value("\${micronaut.security.enabled:false}") security: Boolean,
) : InvitationRecipientRule(membership, permissions, security, false)

@Singleton
class InvitationAcceptRule(
  membership: MembershipAuthorizationDataService,
  permissions: OperationPermissions,
  @Value("\${micronaut.security.enabled:false}") security: Boolean,
) : InvitationRecipientRule(membership, permissions, security, true)

@Singleton
class InvitationCancelRule(
  private val membership: MembershipAuthorizationDataService,
  private val permissions: OperationPermissions,
  @Value("\${micronaut.security.enabled:false}") private val security: Boolean,
) : OperationRule {
  override fun readInput(args: Array<out Any?>): InvitationOperationInput = invitationInput(args)

  override fun authorize(
    caller: Authentication,
    input: OperationInput,
  ) {
    permissions.requireAuthenticated(caller, setOf(OperationCaller.USER))
    invitationIdentity(membership, caller, security)
    try {
      val invitation =
        when (val request = input as InvitationOperationInput) {
          is InvitationOperationInput.Id -> membership.invitationById(request.value)
          is InvitationOperationInput.Code -> membership.invitationByCode(request.value)
          else -> throw BadRequestProblem()
        }
      requireAuthorizationTarget(permissions, caller, invitation.target, permissionAdminRoles, setOf(OperationCaller.USER))
    } catch (failure: Exception) {
      // Existing cancellation deliberately wraps saved-lookup and role failures as OperationNotAllowed.
      throw OperationNotAllowedException("Admin authorization failed for invitation.", failure)
    }
  }
}
