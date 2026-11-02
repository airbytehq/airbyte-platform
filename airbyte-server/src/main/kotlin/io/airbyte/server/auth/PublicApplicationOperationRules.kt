/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.auth

import io.airbyte.api.problems.throwable.generated.BadRequestProblem
import io.airbyte.api.problems.throwable.generated.ResourceNotFoundProblem
import io.airbyte.commons.auth.route.OperationInput
import io.airbyte.domain.services.auth.ApplicationAuthorizationService
import io.micronaut.context.annotation.Requires
import io.micronaut.context.annotation.Value
import io.micronaut.security.authentication.Authentication
import jakarta.inject.Singleton

data class PublicApplicationInput(
  val id: String,
) : OperationInput

@Singleton
@Requires(bean = ApplicationAuthorizationService::class)
class PublicOwnedApplicationRule(
  applications: ApplicationAuthorizationService,
  permissions: OperationPermissions,
  @Value("\${micronaut.security.enabled:false}") securityEnabled: Boolean,
) : UserApplicationOperationRule(applications, permissions, securityEnabled) {
  override fun readInput(args: Array<out Any?>): OperationInput = PublicApplicationInput(args.firstOrNull() as? String ?: throw BadRequestProblem())

  override fun authorize(
    caller: Authentication,
    input: OperationInput,
  ) {
    val owner = currentSubject(caller)
    if (!applications.ownsPublicApplication(
        owner,
        (input as PublicApplicationInput).id,
      )
    ) {
      throw ResourceNotFoundProblem(detail = "The application with the provided id was not found.", data = null)
    }
  }
}
