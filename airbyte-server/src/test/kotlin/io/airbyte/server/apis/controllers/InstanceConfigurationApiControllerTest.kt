/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */
package io.airbyte.server.apis.controllers

import io.airbyte.api.model.generated.InstanceConfigurationResponse
import io.airbyte.commons.server.handlers.InstanceConfigurationHandler
import io.airbyte.server.assertStatus
import io.airbyte.server.status
import io.fabric8.kubernetes.client.KubernetesClient
import io.micronaut.http.HttpRequest
import io.micronaut.http.HttpStatus
import io.micronaut.http.client.HttpClient
import io.micronaut.http.client.annotation.Client
import io.micronaut.test.annotation.MockBean
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import io.mockk.every
import io.mockk.mockk
import jakarta.inject.Inject
import org.junit.jupiter.api.Test

private const val PATH: String = "/api/v1/instance_configuration"

@MicronautTest
internal class InstanceConfigurationApiControllerTest {
  @Inject
  lateinit var instanceConfigurationHandler: InstanceConfigurationHandler

  @Inject
  @Client("/")
  lateinit var client: HttpClient

  @MockBean(InstanceConfigurationHandler::class)
  fun mmInstanceConfigurationHandler(): InstanceConfigurationHandler = mockk()

  @MockBean(KubernetesClient::class)
  fun kubernetesClient(): KubernetesClient = mockk()

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
