/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.auth

import io.airbyte.api.problems.throwable.generated.BadRequestProblem
import io.airbyte.api.problems.throwable.generated.ForbiddenProblem
import io.airbyte.api.server.generated.models.UserAuthIdRequestBody
import io.airbyte.api.server.generated.models.UserEmailRequestBody
import io.airbyte.api.server.generated.models.UserIdRequestBody
import io.airbyte.api.server.generated.models.UserUpdate
import io.airbyte.api.server.generated.models.WorkspaceIdRequestBody
import io.airbyte.commons.auth.roles.AuthRoleConstants
import io.airbyte.commons.auth.route.OperationInput
import io.airbyte.commons.auth.route.OperationRule
import io.airbyte.commons.auth.support.UserAuthenticationResolver
import io.airbyte.domain.services.auth.AuthorizationDataService
import io.micronaut.security.authentication.Authentication
import jakarta.inject.Singleton
import java.util.UUID

sealed interface UserOperationInput : OperationInput {
  data class User(
    val userId: UUID,
  ) : UserOperationInput

  data class Subject(
    val authUserId: String,
  ) : UserOperationInput

  data class Email(
    val email: String,
  ) : UserOperationInput

  data class Workspace(
    val workspaceId: UUID,
  ) : UserOperationInput
}

@Singleton
class UserSelfRule(
  private val data: AuthorizationDataService,
  private val permissions: OperationPermissions,
) : OperationRule {
  override fun readInput(args: Array<out Any?>): UserOperationInput =
    when (val body = args.singleOrNull()) {
      is UserIdRequestBody -> UserOperationInput.User(body.userId)
      is UserUpdate -> UserOperationInput.User(body.userId)
      is UserAuthIdRequestBody -> UserOperationInput.Subject(body.authUserId)
      else -> throw BadRequestProblem()
    }

  override fun authorize(
    caller: Authentication,
    input: OperationInput,
  ) {
    val request = input as UserOperationInput
    try {
      permissions.requireGlobal(caller, setOf(AuthRoleConstants.ADMIN), organizationCallers)
    } catch (_: ForbiddenProblem) {
      val subjects =
        when (request) {
          is UserOperationInput.User -> data.userAuthSubjects(request.userId)
          is UserOperationInput.Subject -> setOf(request.authUserId)
          else -> throw BadRequestProblem()
        }
      permissions.requireSelfOrInstanceAdmin(caller, subjects, organizationCallers)
    }
    when (request) {
      is UserOperationInput.User -> data.requireUser(request.userId)
      is UserOperationInput.Subject -> data.requireAuthUserSubject(request.authUserId)
      else -> throw BadRequestProblem()
    }
  }
}

@Singleton
class UserEmailRule(
  private val data: AuthorizationDataService,
  private val permissions: OperationPermissions,
) : OperationRule {
  override fun readInput(args: Array<out Any?>): UserOperationInput.Email {
    val body = args.singleOrNull() as? UserEmailRequestBody ?: throw BadRequestProblem()
    return UserOperationInput.Email(body.email)
  }

  override fun authorize(
    caller: Authentication,
    input: OperationInput,
  ) {
    permissions.requireGlobal(caller, setOf(AuthRoleConstants.ADMIN), organizationCallers)
    data.requireUserByEmail((input as UserOperationInput.Email).email)
  }
}

@Singleton
class UserWorkspaceReadRule(
  private val data: AuthorizationDataService,
  private val permissions: OperationPermissions,
) : OperationRule {
  override fun readInput(args: Array<out Any?>): UserOperationInput.Workspace {
    val body = args.singleOrNull() as? WorkspaceIdRequestBody ?: throw BadRequestProblem()
    return UserOperationInput.Workspace(body.workspaceId)
  }

  override fun authorize(
    caller: Authentication,
    input: OperationInput,
  ) {
    permissions.requireWorkspaces(
      caller,
      listOf(data.workspaceAndOrganization((input as UserOperationInput.Workspace).workspaceId)),
      workspaceReadRoles,
      organizationCallers,
    )
  }
}

@Singleton
class UserOnboardingRule(
  private val identities: UserAuthenticationResolver,
  private val permissions: OperationPermissions,
) : OperationRule {
  override fun readInput(args: Array<out Any?>): UserOperationInput.Subject {
    val body = args.singleOrNull() as? UserAuthIdRequestBody ?: throw BadRequestProblem()
    return UserOperationInput.Subject(body.authUserId)
  }

  override fun authorize(
    caller: Authentication,
    input: OperationInput,
  ) {
    permissions.requireAuthenticated(caller, setOf(OperationCaller.USER))
    // Reuse the configured provider's exact JWT/OIDC subject semantics before provisioning business work.
    identities.resolveUser((input as UserOperationInput.Subject).authUserId)
  }
}
