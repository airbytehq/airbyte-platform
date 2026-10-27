/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.auth

import io.airbyte.api.problems.throwable.generated.BadRequestProblem
import io.airbyte.api.problems.throwable.generated.ForbiddenProblem
import io.airbyte.api.server.generated.models.EnableScimRequestBody
import io.airbyte.api.server.generated.models.GroupCreate
import io.airbyte.api.server.generated.models.ListOrganizationSummariesRequestBody
import io.airbyte.api.server.generated.models.ListOrganizationsByUserRequestBody
import io.airbyte.api.server.generated.models.OrganizationAgenticStatusUpdateRequestBody
import io.airbyte.api.server.generated.models.OrganizationCreateRequestBody
import io.airbyte.api.server.generated.models.OrganizationDataWorkerUsageRequestBody
import io.airbyte.api.server.generated.models.OrganizationIdRequestBody
import io.airbyte.api.server.generated.models.OrganizationIsEntitledRequestBody
import io.airbyte.api.server.generated.models.OrganizationSemanticSearchEnablementUpdateRequestBody
import io.airbyte.api.server.generated.models.OrganizationUpdateRequestBody
import io.airbyte.api.server.generated.models.OrganizationUsageRequestBody
import io.airbyte.api.server.generated.models.UpdateOrganizationEntitlementPlanRequestBody
import io.airbyte.commons.auth.generated.Intent
import io.airbyte.commons.auth.roles.AuthRoleConstants
import io.airbyte.commons.auth.route.OperationInput
import io.airbyte.commons.auth.route.OperationRule
import io.airbyte.domain.services.auth.AuthorizationDataService
import io.airbyte.domain.services.auth.OrganizationAccessService
import io.micronaut.security.authentication.Authentication
import jakarta.inject.Singleton
import java.util.UUID

internal val organizationCallers =
  setOf(OperationCaller.USER, OperationCaller.INTERNAL_CLIENT, OperationCaller.DATAPLANE, OperationCaller.SERVICE_ACCOUNT)

private fun organizationInput(args: Array<out Any?>): OrganizationOperationInput =
  OrganizationOperationInput(
    when (val body = args.singleOrNull()) {
      is OrganizationIdRequestBody -> body.organizationId
      is OrganizationUpdateRequestBody -> body.organizationId
      is OrganizationAgenticStatusUpdateRequestBody -> body.organizationId
      is OrganizationUsageRequestBody -> body.organizationId
      is OrganizationDataWorkerUsageRequestBody -> body.organizationId
      is EnableScimRequestBody -> body.organizationId
      is GroupCreate -> body.organizationId
      is OrganizationSemanticSearchEnablementUpdateRequestBody -> body.organizationId
      is OrganizationIsEntitledRequestBody -> body.organizationId
      is UpdateOrganizationEntitlementPlanRequestBody -> body.organizationId
      is UUID -> body
      else -> throw BadRequestProblem()
    },
  )

@Singleton
class OrganizationAccessibleRule(
  data: AuthorizationDataService,
  access: OrganizationAccessService,
  permissions: OperationPermissions,
) : OrganizationAccessibleOperationRule(data, access, permissions) {
  override fun readInput(args: Array<out Any?>): OrganizationOperationInput = organizationInput(args)
}

abstract class OrganizationScopedRule(
  private val data: AuthorizationDataService,
  private val permissions: OperationPermissions,
  private val roles: Set<String>,
  private val requireSavedOrganization: Boolean = true,
) : OperationRule {
  override fun readInput(args: Array<out Any?>): OperationInput =
    if (!requireSavedOrganization && args.singleOrNull() == null) MetadataOperationInput else organizationInput(args)

  override fun authorize(
    caller: Authentication,
    input: OperationInput,
  ) {
    if (!requireSavedOrganization && input == MetadataOperationInput) {
      permissions.requireGlobal(caller, roles, organizationCallers)
      return
    }
    val requested = (input as OrganizationOperationInput).organizationId
    val organization = if (requireSavedOrganization) data.organization(requested) else requested
    permissions.requireOrganizations(caller, listOf(organization), roles, organizationCallers)
  }
}

@Singleton
class OrganizationEditorRule(
  data: AuthorizationDataService,
  permissions: OperationPermissions,
) : OrganizationScopedRule(data, permissions, setOf(AuthRoleConstants.ORGANIZATION_EDITOR))

@Singleton
class OrganizationAdminRule(
  data: AuthorizationDataService,
  permissions: OperationPermissions,
) : OrganizationScopedRule(data, permissions, setOf(AuthRoleConstants.ORGANIZATION_ADMIN))

@Singleton
class OrganizationInstanceAdminRule(
  data: AuthorizationDataService,
  permissions: OperationPermissions,
) : OrganizationScopedRule(data, permissions, setOf(AuthRoleConstants.ADMIN))

@Singleton
class OrganizationMemberRule(
  data: AuthorizationDataService,
  permissions: OperationPermissions,
) : OrganizationScopedRule(data, permissions, setOf(AuthRoleConstants.ORGANIZATION_MEMBER))

@Singleton
class OrganizationUsageRule(
  data: AuthorizationDataService,
  permissions: OperationPermissions,
) : OrganizationScopedRule(data, permissions, Intent.ViewOrganizationUsage.roles)

/** These OSS bodies consume no organization; its ID only scopes the original action check. */
@Singleton
class OrganizationUsageUnsupportedRule(
  data: AuthorizationDataService,
  permissions: OperationPermissions,
) : OrganizationScopedRule(data, permissions, Intent.ViewOrganizationUsage.roles, requireSavedOrganization = false)

@Singleton
class OrganizationMemberUnsupportedRule(
  data: AuthorizationDataService,
  permissions: OperationPermissions,
) : OrganizationScopedRule(data, permissions, setOf(AuthRoleConstants.ORGANIZATION_MEMBER), requireSavedOrganization = false)

data class OrganizationUserInput(
  val userId: UUID,
) : OperationInput

@Singleton
class OrganizationUserListRule(
  private val data: AuthorizationDataService,
  private val permissions: OperationPermissions,
) : OperationRule {
  override fun readInput(args: Array<out Any?>): OrganizationUserInput =
    OrganizationUserInput(
      when (val body = args.singleOrNull()) {
        is ListOrganizationsByUserRequestBody -> body.userId
        is ListOrganizationSummariesRequestBody -> body.userId
        else -> throw BadRequestProblem()
      },
    )

  override fun authorize(
    caller: Authentication,
    input: OperationInput,
  ) {
    try {
      // These handlers filter by userId; an administrator may receive an empty list for an unknown user.
      permissions.requireGlobal(caller, setOf(AuthRoleConstants.ADMIN), organizationCallers)
      return
    } catch (_: ForbiddenProblem) {
      // Only SELF needs the saved authentication-subject mapping. Lookup errors must still propagate.
    }
    val subjects = data.userAuthSubjects((input as OrganizationUserInput).userId)
    permissions.requireSelfOrInstanceAdmin(caller, subjects, organizationCallers)
  }
}

@Singleton
class OrganizationCreateRule(
  private val data: AuthorizationDataService,
  private val permissions: OperationPermissions,
) : OperationRule {
  override fun readInput(args: Array<out Any?>): OrganizationUserInput {
    val body = args.singleOrNull() as? OrganizationCreateRequestBody ?: throw BadRequestProblem()
    return OrganizationUserInput(body.userId)
  }

  override fun authorize(
    caller: Authentication,
    input: OperationInput,
  ) {
    permissions.requireGlobal(caller, setOf(AuthRoleConstants.ADMIN), organizationCallers)
    data.requireUser((input as OrganizationUserInput).userId)
  }
}

/** The existing delete placeholder consumes no resource and does no business work. */
@Singleton
class OrganizationDeletePlaceholderRule(
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
