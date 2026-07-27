/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.wrapped

import io.airbyte.api.generated.InstanceConfigurationApi
import io.airbyte.api.model.generated.InstanceConfigurationResponse
import io.airbyte.api.model.generated.InstanceConfigurationSetupRequestBody
import io.airbyte.api.model.generated.LicenseInfoResponse
import io.airbyte.server.apis.controllers.InstanceConfigurationApiController
import io.micronaut.context.annotation.Replaces
import io.micronaut.context.annotation.Requires
import io.micronaut.http.HttpStatus
import io.micronaut.http.annotation.Controller
import io.micronaut.http.annotation.Status
import jakarta.inject.Singleton
import java.util.concurrent.atomic.AtomicInteger

@Controller("/api/v1/instance_configuration")
@Replaces(InstanceConfigurationApiController::class)
@Requires(property = "spec.name", value = "StrictJsonDeserializationTest")
class DirectInterfaceInstanceConfigurationApiController(
  private val tracker: InstanceConfigurationSetupTracker,
) : InstanceConfigurationApi {
  @Status(HttpStatus.NO_CONTENT)
  override fun getInstanceConfiguration(): InstanceConfigurationResponse? = null

  @Status(HttpStatus.NO_CONTENT)
  override fun licenseInfo(): LicenseInfoResponse? = null

  @Status(HttpStatus.NO_CONTENT)
  override fun setupInstanceConfiguration(
    instanceConfigurationSetupRequestBody: InstanceConfigurationSetupRequestBody,
  ): InstanceConfigurationResponse? {
    tracker.recordSetup()
    return InstanceConfigurationResponse()
  }
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
