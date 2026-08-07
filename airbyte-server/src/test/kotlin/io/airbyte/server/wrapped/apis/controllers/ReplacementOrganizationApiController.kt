/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.wrapped.apis.controllers

import io.airbyte.api.model.generated.OrganizationRead
import io.airbyte.api.model.generated.OrganizationUpdateRequestBody
import io.airbyte.commons.server.handlers.OrganizationsHandler
import io.airbyte.domain.services.dataworker.DataWorkerCapacityService
import io.airbyte.domain.services.dataworker.DataWorkerUsageService
import io.airbyte.server.apis.controllers.OrganizationApiController
import io.airbyte.server.helpers.OrganizationAccessAuthorizationHelper
import io.micronaut.context.annotation.Replaces
import io.micronaut.context.annotation.Requires
import io.micronaut.http.annotation.Body
import io.micronaut.http.annotation.Controller
import jakarta.inject.Singleton
import java.util.concurrent.atomic.AtomicInteger

@Controller("/api/v1/organizations")
@Replaces(OrganizationApiController::class)
@Requires(property = "spec.name", value = "StrictJsonDeserializationTest")
open class ReplacementOrganizationApiController(
  organizationsHandler: OrganizationsHandler,
  organizationAccessAuthorizationHelper: OrganizationAccessAuthorizationHelper,
  dataWorkerUsageService: DataWorkerUsageService,
  dataWorkerCapacityService: DataWorkerCapacityService,
  private val updateTracker: WrappedOrganizationUpdateTracker,
) : OrganizationApiController(
    organizationsHandler,
    organizationAccessAuthorizationHelper,
    dataWorkerUsageService,
    dataWorkerCapacityService,
  ) {
  override fun updateOrganization(
    @Body organizationUpdateRequestBody: OrganizationUpdateRequestBody,
  ): OrganizationRead? {
    updateTracker.recordUpdate()
    return super.updateOrganization(organizationUpdateRequestBody)
  }
}

@Singleton
@Requires(property = "spec.name", value = "StrictJsonDeserializationTest")
class WrappedOrganizationUpdateTracker {
  private val updateCount = AtomicInteger()

  fun recordUpdate() {
    updateCount.incrementAndGet()
  }

  fun reset() {
    updateCount.set(0)
  }

  fun count(): Int = updateCount.get()
}
