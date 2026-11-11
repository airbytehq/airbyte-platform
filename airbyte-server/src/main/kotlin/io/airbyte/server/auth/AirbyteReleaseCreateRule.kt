/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.auth

import io.airbyte.api.problems.throwable.generated.BadRequestProblem
import io.airbyte.api.server.generated.models.AirbyteReleaseCreateRequestBody
import io.airbyte.commons.auth.roles.AuthRoleConstants
import io.airbyte.commons.auth.route.OperationInput
import io.airbyte.commons.auth.route.OperationRule
import io.micronaut.security.authentication.Authentication
import jakarta.inject.Singleton

/** Release publication runs from CI as an instance admin user or service account, never as a worker or data plane. */
private val releaseRecorderCallers = setOf(OperationCaller.USER, OperationCaller.SERVICE_ACCOUNT)

@Singleton
class AirbyteReleaseCreateRule(
  private val permissions: OperationPermissions,
) : OperationRule {
  override fun readInput(args: Array<out Any?>): OperationInput {
    if (args.singleOrNull() !is AirbyteReleaseCreateRequestBody) throw BadRequestProblem()
    return MetadataOperationInput
  }

  override fun authorize(
    caller: Authentication,
    input: OperationInput,
  ) {
    permissions.requireGlobal(caller, setOf(AuthRoleConstants.ADMIN), releaseRecorderCallers)
  }
}
