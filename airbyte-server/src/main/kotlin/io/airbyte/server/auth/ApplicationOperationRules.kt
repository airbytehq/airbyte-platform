/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.auth

import io.airbyte.api.problems.throwable.generated.BadRequestProblem
import io.airbyte.api.server.generated.models.ApplicationCreate
import io.airbyte.api.server.generated.models.ApplicationIdRequestBody
import io.airbyte.commons.auth.route.OperationInput
import io.airbyte.commons.auth.route.OperationRule
import io.airbyte.commons.server.errors.AuthException
import io.airbyte.domain.services.auth.ApplicationAuthorizationService
import io.micronaut.context.annotation.Requires
import io.micronaut.context.annotation.Value
import io.micronaut.security.authentication.Authentication
import jakarta.inject.Singleton
import java.util.UUID

data class OwnedApplicationInput(
  val applicationId: UUID,
) : OperationInput

abstract class UserApplicationOperationRule(
  protected val applications: ApplicationAuthorizationService,
  private val permissions: OperationPermissions,
  private val securityEnabled: Boolean,
) : OperationRule {
  protected fun currentSubject(caller: Authentication): String {
    permissions.requireAuthenticated(caller, setOf(OperationCaller.USER))
    return try {
      if (securityEnabled) applications.currentUserSubject(caller.name) else applications.defaultUserSubject()
    } catch (failure: Exception) {
      // Match the existing SecurityAware/CommunityCurrentUserService identity-resolution error contracts.
      if (securityEnabled) throw AuthException("Could not get the current Airbyte user due to an internal error.", failure)
      throw RuntimeException("Could not get the current user due to an internal error.", failure)
    }
  }
}

@Singleton
@Requires(bean = ApplicationAuthorizationService::class)
class CurrentUserApplicationRule(
  applications: ApplicationAuthorizationService,
  permissions: OperationPermissions,
  @Value("\${micronaut.security.enabled:false}") securityEnabled: Boolean,
) : UserApplicationOperationRule(applications, permissions, securityEnabled) {
  override fun readInput(args: Array<out Any?>): OperationInput {
    if (args.isNotEmpty() && args.singleOrNull() !is ApplicationCreate) throw BadRequestProblem()
    return MetadataOperationInput
  }

  override fun authorize(
    caller: Authentication,
    input: OperationInput,
  ) {
    currentSubject(caller)
  }
}

@Singleton
@Requires(bean = ApplicationAuthorizationService::class)
class OwnedApplicationRule(
  applications: ApplicationAuthorizationService,
  permissions: OperationPermissions,
  @Value("\${micronaut.security.enabled:false}") securityEnabled: Boolean,
) : UserApplicationOperationRule(applications, permissions, securityEnabled) {
  override fun readInput(args: Array<out Any?>): OwnedApplicationInput {
    val body = args.singleOrNull() as? ApplicationIdRequestBody ?: throw BadRequestProblem()
    return OwnedApplicationInput(body.applicationId)
  }

  override fun authorize(
    caller: Authentication,
    input: OperationInput,
  ) {
    val owner = currentSubject(caller)
    applications.requireApplicationOwner(owner, (input as OwnedApplicationInput).applicationId)
  }
}
