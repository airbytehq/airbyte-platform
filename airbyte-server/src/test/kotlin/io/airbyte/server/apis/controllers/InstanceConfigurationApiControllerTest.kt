/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.model.generated.InstanceConfigurationResponse
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
    every { instanceConfigurationHandler.instanceConfiguration } returns InstanceConfigurationResponse()

    assertStatus(HttpStatus.OK, client.status(HttpRequest.GET<Any>(PATH)))
  }

  @Test
  fun testSetupInstanceConfiguration() {
    every { instanceConfigurationHandler.setupInstanceConfiguration(any()) } returns InstanceConfigurationResponse()

    assertStatus(HttpStatus.OK, client.status(HttpRequest.POST("$PATH/setup", InstanceConfigurationResponse())))
  }
}
