/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.api.problems.throwable.generated.ApiNotImplementedInOssProblem
import io.airbyte.api.server.generated.apis.BillingApi
import io.airbyte.api.server.generated.models.BillingBackfillUsageRequest
import io.airbyte.api.server.generated.models.CancelSubscriptionRead
import io.airbyte.api.server.generated.models.CustomerPortalRead
import io.airbyte.api.server.generated.models.CustomerPortalRequestBody
import io.airbyte.api.server.generated.models.ListInvoicesRead
import io.airbyte.api.server.generated.models.OrganizationIdRequestBody
import io.airbyte.api.server.generated.models.OrganizationSubscriptionInfoRead
import io.airbyte.api.server.generated.models.OrganizationTrialStatusRead
import io.airbyte.api.server.generated.models.PaymentInformationRead
import io.airbyte.commons.auth.generated.Intent
import io.airbyte.commons.auth.permissions.RequiresIntent
import io.airbyte.commons.auth.roles.AuthRoleConstants
import io.airbyte.commons.auth.route.AuthorizeOperation
import io.airbyte.commons.auth.route.RouteAccessPolicy
import io.airbyte.commons.auth.route.RouteMigration
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors
import io.micronaut.http.annotation.Body
import io.micronaut.http.annotation.Controller
import io.micronaut.scheduling.annotation.ExecuteOn
import io.micronaut.security.annotation.Secured

@Controller
open class BillingController : BillingApi {
  @RequiresIntent(Intent.ManageOrganizationBilling)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.LEGACY,
  )
  override fun getCustomerPortalLink(
    @Body customerPortalRequestBody: CustomerPortalRequestBody?,
  ): CustomerPortalRead = throw ApiNotImplementedInOssProblem()

  @RequiresIntent(Intent.ManageOrganizationBilling)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.LEGACY,
  )
  override fun listPastInvoices(
    @Body organizationIdRequestBody: OrganizationIdRequestBody?,
  ): ListInvoicesRead = throw ApiNotImplementedInOssProblem()

  @RequiresIntent(Intent.ManageOrganizationBilling)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.LEGACY,
  )
  override fun getPaymentInformation(
    @Body organizationIdRequestBody: OrganizationIdRequestBody?,
  ): PaymentInformationRead = throw ApiNotImplementedInOssProblem()

  @RequiresIntent(Intent.ViewOrganizationDetails)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.LEGACY,
  )
  override fun getSubscriptionInfo(
    @Body organizationIdRequestBody: OrganizationIdRequestBody?,
  ): OrganizationSubscriptionInfoRead = throw ApiNotImplementedInOssProblem()

  @ExecuteOn(AirbyteTaskExecutors.WEBHOOK)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.LEGACY,
  )
  override fun handleWebhook(
    @Body event: JsonNode?,
  ): Unit = throw ApiNotImplementedInOssProblem()

  @RequiresIntent(Intent.ViewOrganizationTrialStatus)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.LEGACY,
  )
  override fun getOrganizationTrialStatus(
    @Body organizationIdRequestBody: OrganizationIdRequestBody?,
  ): OrganizationTrialStatusRead = throw ApiNotImplementedInOssProblem()

  @Secured(AuthRoleConstants.ADMIN)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.LEGACY,
  )
  override fun backfillUsage(billingBackfillUsageRequest: BillingBackfillUsageRequest?): Unit = throw ApiNotImplementedInOssProblem()

  @RequiresIntent(Intent.ManageOrganizationBilling)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.LEGACY,
  )
  override fun cancelSubscription(
    @Body organizationIdRequestBody: OrganizationIdRequestBody?,
  ): CancelSubscriptionRead = throw ApiNotImplementedInOssProblem()

  @Secured(AuthRoleConstants.ADMIN)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.LEGACY,
  )
  override fun disableDelinquentWorkspaces(): Unit = throw ApiNotImplementedInOssProblem()

  @RequiresIntent(Intent.ManageOrganizationBilling)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.LEGACY,
  )
  override fun unscheduleCancelSubscription(
    @Body organizationIdRequestBody: OrganizationIdRequestBody?,
  ): Unit = throw ApiNotImplementedInOssProblem()

  @RequiresIntent(Intent.ManageOrganizationBilling)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.LEGACY,
  )
  override fun unschedulePlanChange(
    @Body organizationIdRequestBody: OrganizationIdRequestBody?,
  ): Unit = throw ApiNotImplementedInOssProblem()
}
