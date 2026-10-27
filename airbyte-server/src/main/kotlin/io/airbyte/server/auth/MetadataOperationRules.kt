/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.auth

import io.airbyte.api.problems.throwable.generated.BadRequestProblem
import io.airbyte.api.server.generated.models.OrganizationSemanticSearchEnablementUpdateRequestBody
import io.airbyte.commons.auth.roles.AuthRoleConstants
import io.airbyte.commons.auth.route.OperationInput
import io.airbyte.commons.auth.route.OperationRule
import io.micronaut.security.authentication.Authentication
import jakarta.inject.Singleton
import java.util.UUID

/** These operations consume inline content or global metadata and have no saved resource selector. */
data object MetadataOperationInput : OperationInput

@Singleton
class AuthenticatedMetadataRule(
  private val permissions: OperationPermissions,
) : OperationRule {
  override fun readInput(args: Array<out Any?>): OperationInput = MetadataOperationInput

  override fun authorize(
    caller: Authentication,
    input: OperationInput,
  ) {
    permissions.requireAuthenticated(
      caller,
      setOf(OperationCaller.USER, OperationCaller.INTERNAL_CLIENT, OperationCaller.DATAPLANE, OperationCaller.SERVICE_ACCOUNT),
    )
  }
}

@Singleton
class DeploymentMetadataRule(
  private val permissions: OperationPermissions,
) : OperationRule {
  override fun readInput(args: Array<out Any?>): OperationInput = MetadataOperationInput

  override fun authorize(
    caller: Authentication,
    input: OperationInput,
  ) {
    permissions.requireAuthenticated(caller, OperationCaller.entries.toSet())
  }
}

/** Static OSS stubs consume no tenant resource and preserve their authenticated unsupported response. */
@Singleton
class OssUnsupportedOperationRule(
  private val permissions: OperationPermissions,
) : OperationRule {
  override fun readInput(args: Array<out Any?>): OperationInput = MetadataOperationInput

  override fun authorize(
    caller: Authentication,
    input: OperationInput,
  ) {
    permissions.requireAuthenticated(caller, OperationCaller.entries.toSet())
  }
}

/** A static OSS instance-admin operation has no consumed tenant or child resource. */
@Singleton
class OssInstanceAdminUnsupportedRule(
  private val permissions: OperationPermissions,
) : OperationRule {
  override fun readInput(args: Array<out Any?>): OperationInput = MetadataOperationInput

  override fun authorize(
    caller: Authentication,
    input: OperationInput,
  ) {
    permissions.requireGlobal(caller, setOf(AuthRoleConstants.ADMIN), organizationCallers)
  }
}

/** The OSS semantic-search update keeps its explicit admin action but consumes no saved organization. */
@Singleton
class OssOrganizationAdminUnsupportedRule(
  private val permissions: OperationPermissions,
) : OperationRule {
  override fun readInput(args: Array<out Any?>): OperationInput {
    val body = args.singleOrNull() as? OrganizationSemanticSearchEnablementUpdateRequestBody ?: throw BadRequestProblem()
    val organization: UUID? = body.organizationId
    return if (organization == null) MetadataOperationInput else OrganizationOperationInput(organization)
  }

  override fun authorize(
    caller: Authentication,
    input: OperationInput,
  ) {
    if (input == MetadataOperationInput) {
      permissions.requireGlobal(caller, setOf(AuthRoleConstants.ORGANIZATION_ADMIN), organizationCallers)
      return
    }
    permissions.requireOrganizations(
      caller,
      listOf((input as OrganizationOperationInput).organizationId),
      setOf(AuthRoleConstants.ORGANIZATION_ADMIN),
      organizationCallers,
    )
  }
}
