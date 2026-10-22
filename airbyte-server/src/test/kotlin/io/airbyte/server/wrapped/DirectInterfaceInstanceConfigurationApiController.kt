/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.wrapped

import io.airbyte.api.server.generated.apis.InstanceConfigurationApi
import io.airbyte.api.server.generated.models.AuthConfiguration
import io.airbyte.api.server.generated.models.InstanceConfigurationResponse
import io.airbyte.api.server.generated.models.InstanceConfigurationSetupRequestBody
import io.airbyte.commons.auth.route.AuthorizeOperation
import io.airbyte.commons.auth.route.RouteAccessPolicy
import io.airbyte.commons.auth.route.RouteMigration
import io.airbyte.server.apis.controllers.InstanceConfigurationApiController
import io.micronaut.context.annotation.Replaces
import io.micronaut.context.annotation.Requires
import io.micronaut.http.HttpStatus
import io.micronaut.http.annotation.Body
import io.micronaut.http.annotation.Controller
import io.micronaut.http.annotation.Status
import jakarta.inject.Singleton
import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger

@Controller
@Replaces(InstanceConfigurationApiController::class)
@Requires(property = "spec.name", value = "StrictJsonDeserializationTest")
class DirectInterfaceInstanceConfigurationApiController(
  private val tracker: InstanceConfigurationSetupTracker,
) : InstanceConfigurationApi {
  @Status(HttpStatus.NO_CONTENT)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.LEGACY,
  )
  override fun getInstanceConfiguration(): InstanceConfigurationResponse = instanceConfigurationResponse()

  @Status(HttpStatus.NO_CONTENT)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.LEGACY,
  )
  override fun setupInstanceConfiguration(
    @Body instanceConfigurationSetupRequestBody: InstanceConfigurationSetupRequestBody?,
  ): InstanceConfigurationResponse {
    tracker.recordSetup()
    return instanceConfigurationResponse()
  }

  private fun instanceConfigurationResponse(): InstanceConfigurationResponse =
    InstanceConfigurationResponse(
      edition = InstanceConfigurationResponse.Edition.COMMUNITY,
      version = "dev",
      auth = AuthConfiguration(mode = AuthConfiguration.Mode.NONE),
      airbyteUrl = "http://localhost:8000",
      initialSetupComplete = true,
      defaultUserId = UUID.randomUUID(),
      defaultOrganizationId = UUID.randomUUID(),
      defaultOrganizationEmail = "test@airbyte.io",
      trackingStrategy = InstanceConfigurationResponse.TrackingStrategy.LOGGING,
    )
}

@Singleton
@Requires(property = "spec.name", value = "StrictJsonDeserializationTest")
class InstanceConfigurationSetupTracker {
  private val setupCount = AtomicInteger()

  fun recordSetup() {
    setupCount.incrementAndGet()
  }

  fun reset() {
    setupCount.set(0)
  }

  fun count(): Int = setupCount.get()
}
