/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.problems.throwable.generated.ApiNotImplementedInOssProblem
import io.airbyte.api.server.generated.apis.BillingNotificationSettingsApi
import io.airbyte.api.server.generated.models.BillingNotificationSettingsRead
import io.airbyte.api.server.generated.models.BillingNotificationSettingsUpdateRequestBody
import io.airbyte.api.server.generated.models.OrganizationIdRequestBody
import io.airbyte.commons.auth.generated.Intent
import io.airbyte.commons.auth.permissions.RequiresIntent
import io.airbyte.commons.auth.route.AuthorizeOperation
import io.airbyte.commons.auth.route.RouteAccessPolicy
import io.airbyte.commons.auth.route.RouteMigration
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors
import io.micronaut.http.annotation.Body
import io.micronaut.http.annotation.Controller
import io.micronaut.scheduling.annotation.ExecuteOn

/**
 * Billing notification settings are a Cloud feature. The Cloud implementation lives in
 * `airbyte-server-wrapped` and replaces this bean.
 */
@Controller
open class BillingNotificationSettingsController : BillingNotificationSettingsApi {
  @RequiresIntent(Intent.ManageOrganizationBilling)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.LEGACY,
  )
  override fun getBillingNotificationSettings(
    @Body organizationIdRequestBody: OrganizationIdRequestBody,
  ): BillingNotificationSettingsRead = throw ApiNotImplementedInOssProblem()

  @RequiresIntent(Intent.ManageOrganizationBilling)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.LEGACY,
  )
  override fun updateBillingNotificationSettings(
    @Body billingNotificationSettingsUpdateRequestBody: BillingNotificationSettingsUpdateRequestBody,
  ): BillingNotificationSettingsRead = throw ApiNotImplementedInOssProblem()
}
