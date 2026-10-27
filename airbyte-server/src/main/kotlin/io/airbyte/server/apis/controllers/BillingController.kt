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
import io.airbyte.commons.auth.route.AuthorizeOperation
import io.airbyte.commons.auth.route.RouteAccessPolicy
import io.airbyte.commons.auth.route.RouteMigration
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors
import io.micronaut.http.annotation.Body
import io.micronaut.http.annotation.Controller
import io.micronaut.scheduling.annotation.ExecuteOn
import io.micronaut.security.annotation.Secured
import io.micronaut.security.rules.SecurityRule

@Controller
open class BillingController : BillingApi {
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Secured(SecurityRule.IS_AUTHENTICATED)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = io.airbyte.server.auth.BillingManageUnsupportedRule::class,
  )
  override fun getCustomerPortalLink(
    @Body customerPortalRequestBody: CustomerPortalRequestBody?,
  ): CustomerPortalRead = throw ApiNotImplementedInOssProblem()

  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Secured(SecurityRule.IS_AUTHENTICATED)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = io.airbyte.server.auth.BillingManageUnsupportedRule::class,
  )
  override fun listPastInvoices(
    @Body organizationIdRequestBody: OrganizationIdRequestBody?,
  ): ListInvoicesRead = throw ApiNotImplementedInOssProblem()

  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Secured(SecurityRule.IS_AUTHENTICATED)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = io.airbyte.server.auth.BillingManageUnsupportedRule::class,
  )
  override fun getPaymentInformation(
    @Body organizationIdRequestBody: OrganizationIdRequestBody?,
  ): PaymentInformationRead = throw ApiNotImplementedInOssProblem()

  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Secured(SecurityRule.IS_AUTHENTICATED)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = io.airbyte.server.auth.BillingSubscriptionUnsupportedRule::class,
  )
  override fun getSubscriptionInfo(
    @Body organizationIdRequestBody: OrganizationIdRequestBody?,
  ): OrganizationSubscriptionInfoRead = throw ApiNotImplementedInOssProblem()

  @ExecuteOn(AirbyteTaskExecutors.WEBHOOK)
  @Secured(SecurityRule.IS_AUTHENTICATED)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = io.airbyte.server.auth.UnavailableProtectedRule::class,
  )
  override fun handleWebhook(
    @Body event: JsonNode?,
  ): Unit = throw ApiNotImplementedInOssProblem()

  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Secured(SecurityRule.IS_AUTHENTICATED)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = io.airbyte.server.auth.BillingTrialUnsupportedRule::class,
  )
  override fun getOrganizationTrialStatus(
    @Body organizationIdRequestBody: OrganizationIdRequestBody?,
  ): OrganizationTrialStatusRead = throw ApiNotImplementedInOssProblem()

  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Secured(SecurityRule.IS_AUTHENTICATED)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = io.airbyte.server.auth.BillingBackfillStubRule::class,
  )
  override fun backfillUsage(billingBackfillUsageRequest: BillingBackfillUsageRequest?): Unit = throw ApiNotImplementedInOssProblem()

  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Secured(SecurityRule.IS_AUTHENTICATED)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = io.airbyte.server.auth.BillingManageUnsupportedRule::class,
  )
  override fun cancelSubscription(
    @Body organizationIdRequestBody: OrganizationIdRequestBody?,
  ): CancelSubscriptionRead = throw ApiNotImplementedInOssProblem()

  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Secured(SecurityRule.IS_AUTHENTICATED)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = io.airbyte.server.auth.GlobalAdminRule::class,
  )
  override fun disableDelinquentWorkspaces(): Unit = throw ApiNotImplementedInOssProblem()

  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Secured(SecurityRule.IS_AUTHENTICATED)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = io.airbyte.server.auth.BillingManageUnsupportedRule::class,
  )
  override fun unscheduleCancelSubscription(
    @Body organizationIdRequestBody: OrganizationIdRequestBody?,
  ): Unit = throw ApiNotImplementedInOssProblem()

  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Secured(SecurityRule.IS_AUTHENTICATED)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = io.airbyte.server.auth.BillingManageUnsupportedRule::class,
  )
  override fun unschedulePlanChange(
    @Body organizationIdRequestBody: OrganizationIdRequestBody?,
  ): Unit = throw ApiNotImplementedInOssProblem()
}
