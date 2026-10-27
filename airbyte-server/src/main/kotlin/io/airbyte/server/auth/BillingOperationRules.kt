/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.auth

import io.airbyte.api.problems.ResourceType
import io.airbyte.api.problems.model.generated.ProblemResourceData
import io.airbyte.api.problems.throwable.generated.BadRequestProblem
import io.airbyte.api.problems.throwable.generated.ResourceNotFoundProblem
import io.airbyte.api.server.generated.models.BillingNotificationSettingsUpdateRequestBody
import io.airbyte.api.server.generated.models.CustomerPortalRequestBody
import io.airbyte.api.server.generated.models.OrganizationIdRequestBody
import io.airbyte.api.server.generated.models.OrganizationPaymentConfigUpdateRequestBody
import io.airbyte.commons.auth.generated.Intent
import io.airbyte.commons.auth.roles.AuthRoleConstants
import io.airbyte.commons.auth.route.OperationInput
import io.airbyte.commons.auth.route.OperationRule
import io.airbyte.domain.services.auth.AuthorizationDataService
import io.airbyte.domain.services.auth.BillingAuthorizationDataService
import io.micronaut.security.authentication.Authentication
import jakarta.inject.Singleton
import java.util.UUID

abstract class BillingOrganizationRule(
  private val data: AuthorizationDataService,
  private val permissions: OperationPermissions,
  private val roles: Set<String>,
  private val requireSavedOrganization: Boolean = true,
) : OperationRule {
  override fun readInput(args: Array<out Any?>): OperationInput {
    val organization: UUID? =
      when (val body = args.singleOrNull()) {
        is CustomerPortalRequestBody -> body.organizationId
        is OrganizationIdRequestBody -> body.organizationId
        is BillingNotificationSettingsUpdateRequestBody -> body.organizationId
        else -> throw BadRequestProblem()
      }
    if (organization != null) return OrganizationOperationInput(organization)
    if (!requireSavedOrganization) return MetadataOperationInput
    throw BadRequestProblem()
  }

  override fun authorize(
    caller: Authentication,
    input: OperationInput,
  ) {
    if (!requireSavedOrganization && input == MetadataOperationInput) {
      permissions.requireGlobal(caller, roles, organizationCallers)
      return
    }
    val requested = (input as OrganizationOperationInput).organizationId
    permissions.requireOrganizations(
      caller,
      listOf(if (requireSavedOrganization) data.organization(requested) else requested),
      roles,
      organizationCallers,
    )
  }
}

@Singleton
class BillingManageRule(
  data: AuthorizationDataService,
  permissions: OperationPermissions,
) : BillingOrganizationRule(data, permissions, Intent.ManageOrganizationBilling.roles)

@Singleton
class BillingSubscriptionRule(
  data: AuthorizationDataService,
  permissions: OperationPermissions,
) : BillingOrganizationRule(data, permissions, Intent.ViewOrganizationDetails.roles)

@Singleton
class BillingTrialRule(
  data: AuthorizationDataService,
  permissions: OperationPermissions,
) : BillingOrganizationRule(data, permissions, Intent.ViewOrganizationTrialStatus.roles)

/** Static OSS billing responses need the original organization action, without a business owner read. */
@Singleton
class BillingManageUnsupportedRule(
  data: AuthorizationDataService,
  permissions: OperationPermissions,
) : BillingOrganizationRule(data, permissions, Intent.ManageOrganizationBilling.roles, requireSavedOrganization = false)

@Singleton
class BillingSubscriptionUnsupportedRule(
  data: AuthorizationDataService,
  permissions: OperationPermissions,
) : BillingOrganizationRule(data, permissions, Intent.ViewOrganizationDetails.roles, requireSavedOrganization = false)

@Singleton
class BillingTrialUnsupportedRule(
  data: AuthorizationDataService,
  permissions: OperationPermissions,
) : BillingOrganizationRule(data, permissions, Intent.ViewOrganizationTrialStatus.roles, requireSavedOrganization = false)

data class BillingBackfillInput(
  val jobIds: List<Long>,
) : OperationInput

/** Community's stub does not consume jobs. */
@Singleton
class BillingBackfillStubRule(
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

@Singleton
class OrganizationPaymentConfigRule(
  private val data: AuthorizationDataService,
  private val billing: BillingAuthorizationDataService,
  private val permissions: OperationPermissions,
) : OperationRule {
  override fun readInput(args: Array<out Any?>): OperationInput =
    OrganizationOperationInput(
      when (val body = args.singleOrNull()) {
        is UUID -> body
        is OrganizationPaymentConfigUpdateRequestBody -> body.organizationId
        else -> throw BadRequestProblem()
      },
    )

  override fun authorize(
    caller: Authentication,
    input: OperationInput,
  ) {
    val organization = data.organization((input as OrganizationOperationInput).organizationId)
    permissions.requireOrganizations(caller, listOf(organization), Intent.ManageOrganizationPaymentConfigs.roles, organizationCallers)
    if (!billing.hasPaymentConfiguration(
        organization,
      )
    ) {
      throw ResourceNotFoundProblem(
        ProblemResourceData().resourceId(organization.toString()).resourceType(ResourceType.ORGANIZATION_PAYMENT_CONFIG),
      )
    }
  }
}

/** An edition-specific protected placeholder has no supported authenticated caller. */
@Singleton
class UnavailableProtectedRule(
  private val permissions: OperationPermissions,
) : OperationRule {
  override fun readInput(args: Array<out Any?>): OperationInput = MetadataOperationInput

  override fun authorize(
    caller: Authentication,
    input: OperationInput,
  ) {
    permissions.requireAuthenticated(caller, emptySet())
  }
}
