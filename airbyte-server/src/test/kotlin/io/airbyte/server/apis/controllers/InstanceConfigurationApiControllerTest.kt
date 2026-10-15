/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.server.generated.models.AuthConfiguration
import io.airbyte.api.server.generated.models.InstanceConfigurationResponse
import io.airbyte.api.server.generated.models.InstanceConfigurationSetupRequestBody
import io.airbyte.commons.server.handlers.InstanceConfigurationHandler
import io.airbyte.server.assertStatus
import io.airbyte.server.status
import io.fabric8.kubernetes.client.KubernetesClient
import io.micronaut.context.ApplicationContext
import io.micronaut.http.HttpRequest
import io.micronaut.http.HttpStatus
import io.micronaut.http.client.HttpClient
import io.micronaut.http.client.annotation.Client
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import io.mockk.every
import io.mockk.mockk
import jakarta.inject.Inject
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.util.UUID

private const val PATH: String = "/api/v1/instance_configuration"

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@MicronautTest
internal class InstanceConfigurationApiControllerTest {
  @Inject
  lateinit var context: ApplicationContext

  lateinit var instanceConfigurationHandler: InstanceConfigurationHandler

  @Inject
  @Client("/")
  lateinit var client: HttpClient

  @BeforeAll
  fun setupMock() {
    instanceConfigurationHandler = mockk()
    context.registerSingleton(InstanceConfigurationHandler::class.java, instanceConfigurationHandler)
    val kubernetesClient = mockk<KubernetesClient>()
    context.registerSingleton(KubernetesClient::class.java, kubernetesClient)
  }

  @Test
  fun testGetInstanceConfiguration() {
    every { instanceConfigurationHandler.instanceConfiguration } returns instanceConfigurationResponse()

    assertStatus(HttpStatus.OK, client.status(HttpRequest.GET<Any>(PATH)))
  }

  @Test
  fun testSetupInstanceConfiguration() {
    every { instanceConfigurationHandler.setupInstanceConfiguration(any()) } returns instanceConfigurationResponse()

    assertStatus(HttpStatus.OK, client.status(HttpRequest.POST("$PATH/setup", setupRequestBody())))
  }
}

private fun instanceConfigurationResponse(): InstanceConfigurationResponse =
  InstanceConfigurationResponse(
    edition = InstanceConfigurationResponse.Edition.COMMUNITY,
    version = "0.50.1",
    auth = AuthConfiguration(mode = AuthConfiguration.Mode.NONE),
    airbyteUrl = "http://localhost:8000",
    initialSetupComplete = true,
    defaultUserId = UUID.randomUUID(),
    defaultOrganizationId = UUID.randomUUID(),
    defaultOrganizationEmail = "test@airbyte.io",
    trackingStrategy = InstanceConfigurationResponse.TrackingStrategy.LOGGING,
  )

private fun setupRequestBody(): InstanceConfigurationSetupRequestBody =
  InstanceConfigurationSetupRequestBody(
    email = "test@airbyte.io",
    anonymousDataCollection = false,
    initialSetupComplete = true,
    displaySetupWizard = false,
  )
