/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.auth

import io.airbyte.api.problems.model.generated.ProblemResourceData
import io.airbyte.api.problems.model.generated.ProblemSSODeletionData
import io.airbyte.api.problems.throwable.generated.BadRequestProblem
import io.airbyte.api.problems.throwable.generated.ResourceNotFoundProblem
import io.airbyte.api.problems.throwable.generated.SSODeletionProblem
import io.airbyte.api.server.generated.models.ActivateSSOConfigRequestBody
import io.airbyte.api.server.generated.models.CreateSSOConfigRequestBody
import io.airbyte.api.server.generated.models.DeleteSSOConfigRequestBody
import io.airbyte.api.server.generated.models.DomainVerificationCreateRequestBody
import io.airbyte.api.server.generated.models.DomainVerificationIdRequestBody
import io.airbyte.api.server.generated.models.GetSSOConfigRequestBody
import io.airbyte.api.server.generated.models.OrganizationIdRequestBody
import io.airbyte.api.server.generated.models.TagCreateRequestBody
import io.airbyte.api.server.generated.models.TagDeleteRequestBody
import io.airbyte.api.server.generated.models.TagListRequestBody
import io.airbyte.api.server.generated.models.TagUpdateRequestBody
import io.airbyte.api.server.generated.models.UpdateSSOCredentialsRequestBody
import io.airbyte.api.server.generated.models.ValidateSSOTokenRequestBody
import io.airbyte.commons.auth.generated.Intent
import io.airbyte.commons.auth.roles.AuthRoleConstants
import io.airbyte.commons.auth.route.OperationInput
import io.airbyte.commons.auth.route.OperationRule
import io.airbyte.domain.models.auth.AuthorizationResourceNotFoundException
import io.airbyte.domain.services.auth.AuthorizationDataService
import io.airbyte.domain.services.auth.SsoAuthorizationDataService
import io.micronaut.security.authentication.Authentication
import jakarta.inject.Singleton
import java.util.UUID

@Singleton
class SsoAdminRule(
  data: AuthorizationDataService,
  permissions: OperationPermissions,
) : OrganizationScopedRule(data, permissions, setOf(AuthRoleConstants.ORGANIZATION_ADMIN)) {
  override fun readInput(args: Array<out Any?>): OrganizationOperationInput =
    OrganizationOperationInput(
      when (val body = args.singleOrNull()) {
        is GetSSOConfigRequestBody -> body.organizationId
        is CreateSSOConfigRequestBody -> body.organizationId
        is UpdateSSOCredentialsRequestBody -> body.organizationId
        is ActivateSSOConfigRequestBody -> body.organizationId
        is ValidateSSOTokenRequestBody -> body.organizationId
        else -> throw BadRequestProblem()
      },
    )
}

data class SsoDeleteInput(
  val organizationId: UUID,
  val realm: String,
) : OperationInput

@Singleton
class SsoDeleteRule(
  private val data: AuthorizationDataService,
  private val sso: SsoAuthorizationDataService,
  private val permissions: OperationPermissions,
) : OperationRule {
  override fun readInput(args: Array<out Any?>): SsoDeleteInput {
    val body = args.singleOrNull() as? DeleteSSOConfigRequestBody ?: throw BadRequestProblem()
    return SsoDeleteInput(body.organizationId, body.companyIdentifier)
  }

  override fun authorize(
    caller: Authentication,
    input: OperationInput,
  ) {
    val request = input as SsoDeleteInput
    val organization = data.organization(request.organizationId)
    permissions.requireOrganizations(caller, listOf(organization), setOf(AuthRoleConstants.ORGANIZATION_ADMIN), organizationCallers)
    val realm =
      sso.savedRealm(organization).orElseThrow {
        deletionFailure(request, "No SSO config found for organization $organization")
      }
    if (realm !=
      request.realm
    ) {
      throw deletionFailure(
        request,
        "Company identifier mismatch: provided '${request.realm}' does not match DB realm '$realm' for organization $organization",
      )
    }
  }

  private fun deletionFailure(
    request: SsoDeleteInput,
    message: String,
  ) = SSODeletionProblem(
    ProblemSSODeletionData().organizationId(request.organizationId).companyIdentifier(request.realm).errorMessage(message),
  )
}

@Singleton
class DomainVerificationOrganizationRule(
  data: AuthorizationDataService,
  permissions: OperationPermissions,
) : OrganizationScopedRule(data, permissions, Intent.ManageDomainVerification.roles) {
  override fun readInput(args: Array<out Any?>): OrganizationOperationInput =
    OrganizationOperationInput(
      when (val body = args.singleOrNull()) {
        is OrganizationIdRequestBody -> body.organizationId
        is DomainVerificationCreateRequestBody -> body.organizationId
        else -> throw BadRequestProblem()
      },
    )
}

data class DomainVerificationInput(
  val verificationId: UUID,
) : OperationInput

@Singleton
class SavedDomainVerificationRule(
  private val data: AuthorizationDataService,
  private val permissions: OperationPermissions,
) : OperationRule {
  override fun readInput(args: Array<out Any?>): DomainVerificationInput {
    val body = args.singleOrNull() as? DomainVerificationIdRequestBody ?: throw BadRequestProblem()
    return DomainVerificationInput(body.domainVerificationId)
  }

  override fun authorize(
    caller: Authentication,
    input: OperationInput,
  ) {
    val id = (input as DomainVerificationInput).verificationId
    val organization =
      try {
        data.domainVerificationOrganization(id)
      } catch (_: AuthorizationResourceNotFoundException) {
        throw ResourceNotFoundProblem(ProblemResourceData().resourceType("domain_verification").resourceId(id.toString()))
      }
    permissions.requireOrganizations(caller, listOf(organization), setOf(AuthRoleConstants.ORGANIZATION_ADMIN), organizationCallers)
  }
}

@Singleton
class GlobalAdminRule(
  private val permissions: OperationPermissions,
) : OperationRule {
  override fun readInput(args: Array<out Any?>): OperationInput {
    if (args.isNotEmpty()) throw BadRequestProblem()
    return MetadataOperationInput
  }

  override fun authorize(
    caller: Authentication,
    input: OperationInput,
  ) {
    permissions.requireGlobal(caller, setOf(AuthRoleConstants.ADMIN), organizationCallers)
  }
}

sealed interface TagOperationInput : OperationInput {
  data class Workspace(
    val workspaceId: UUID,
  ) : TagOperationInput

  data class Saved(
    val tagId: UUID,
    val workspaceId: UUID,
  ) : TagOperationInput
}

abstract class TagOperationRule(
  private val data: AuthorizationDataService,
  private val permissions: OperationPermissions,
  private val roles: Set<String>,
) : OperationRule {
  override fun readInput(args: Array<out Any?>): TagOperationInput =
    when (val body = args.singleOrNull()) {
      is TagListRequestBody -> TagOperationInput.Workspace(body.workspaceId)
      is TagCreateRequestBody -> TagOperationInput.Workspace(body.workspaceId)
      is TagUpdateRequestBody -> TagOperationInput.Saved(body.tagId, body.workspaceId)
      is TagDeleteRequestBody -> TagOperationInput.Saved(body.tagId, body.workspaceId)
      else -> throw BadRequestProblem()
    }

  override fun authorize(
    caller: Authentication,
    input: OperationInput,
  ) {
    when (val request = input as TagOperationInput) {
      is TagOperationInput.Workspace ->
        permissions.requireWorkspaces(
          caller,
          listOf(data.workspaceAndOrganization(request.workspaceId)),
          roles,
          organizationCallers,
        )
      is TagOperationInput.Saved -> {
        val owner = data.tagWorkspaceAndOrganization(request.tagId)
        permissions.requireWorkspaces(caller, listOf(owner), roles, organizationCallers)
        if (owner.workspaceId != request.workspaceId) throw AuthorizationResourceNotFoundException()
      }
    }
  }
}

@Singleton
class TagReadRule(
  data: AuthorizationDataService,
  permissions: OperationPermissions,
) : TagOperationRule(data, permissions, Intent.ViewConnection.roles)

@Singleton
class TagWriteRule(
  data: AuthorizationDataService,
  permissions: OperationPermissions,
) : TagOperationRule(data, permissions, Intent.CreateOrEditConnection.roles)
