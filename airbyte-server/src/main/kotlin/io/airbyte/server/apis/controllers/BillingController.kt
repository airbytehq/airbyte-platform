package io.airbyte.server.apis.controllers

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.api.generated.BillingApi
import io.airbyte.api.model.generated.CancelSubscriptionRead
import io.airbyte.api.model.generated.CustomerPortalRead
import io.airbyte.api.model.generated.CustomerPortalRequestBody
import io.airbyte.api.model.generated.ListInvoicesRead
import io.airbyte.api.model.generated.OrganizationIdRequestBody
import io.airbyte.api.model.generated.OrganizationSubscriptionInfoRead
import io.airbyte.api.model.generated.OrganizationTrialStatusRead
import io.airbyte.api.model.generated.PaymentInformationRead
import io.airbyte.api.problems.throwable.generated.ApiNotImplementedInOssProblem
import io.airbyte.commons.auth.generated.Intent
import io.airbyte.commons.auth.permissions.RequiresIntent
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors
import io.micronaut.http.annotation.Body
import io.micronaut.http.annotation.Controller
import io.micronaut.http.annotation.Post
import io.micronaut.scheduling.annotation.ExecuteOn

@Controller("/api/v1/billing")
open class BillingController : BillingApi {
  @RequiresIntent(Intent.ManageOrganizationBilling)
  @Post("/customer_portal")
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun getCustomerPortalLink(
    @Body customerPortalRequestBody: CustomerPortalRequestBody,
  ): CustomerPortalRead = throw ApiNotImplementedInOssProblem()

  @RequiresIntent(Intent.ManageOrganizationBilling)
  @Post("/list_invoices")
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun listPastInvoices(
    @Body organizationIdRequestBody: OrganizationIdRequestBody,
  ): ListInvoicesRead = throw ApiNotImplementedInOssProblem()

  @RequiresIntent(Intent.ManageOrganizationBilling)
  @Post("/payment_information")
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun getPaymentInformation(
    @Body organizationIdRequestBody: OrganizationIdRequestBody,
  ): PaymentInformationRead = throw ApiNotImplementedInOssProblem()

  @RequiresIntent(Intent.ManageOrganizationBilling)
  @Post("/subscription_info")
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun getSubscriptionInfo(
    @Body organizationIdRequestBody: OrganizationIdRequestBody,
  ): OrganizationSubscriptionInfoRead = throw ApiNotImplementedInOssProblem()

  @Post("/handle_webhook")
  @ExecuteOn(AirbyteTaskExecutors.WEBHOOK)
  override fun handleWebhook(
    @Body event: JsonNode,
  ): Unit = throw ApiNotImplementedInOssProblem()

  @RequiresIntent(Intent.ViewOrganizationTrialStatus)
  @Post("/trial_status")
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun getOrganizationTrialStatus(
    @Body organizationIdRequestBody: OrganizationIdRequestBody,
  ): OrganizationTrialStatusRead = throw ApiNotImplementedInOssProblem()

  @RequiresIntent(Intent.ManageOrganizationBilling)
  @Post("/cancel_subscription")
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun cancelSubscription(
    @Body organizationIdRequestBody: OrganizationIdRequestBody,
  ): CancelSubscriptionRead = throw ApiNotImplementedInOssProblem()

  @RequiresIntent(Intent.ManageOrganizationBilling)
  @Post("/unschedule_cancel_subscription")
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun unscheduleCancelSubscription(
    @Body organizationIdRequestBody: OrganizationIdRequestBody,
  ): Unit = throw ApiNotImplementedInOssProblem()
}
