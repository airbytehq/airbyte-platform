/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */
package io.airbyte.server.apis.controllers

import io.airbyte.api.model.generated.DeploymentMetadataRead
import io.airbyte.commons.server.handlers.DeploymentMetadataHandler
import io.airbyte.config.Configs
import io.airbyte.server.assertStatus
import io.airbyte.server.status
import io.micronaut.context.annotation.Requires
import io.micronaut.context.env.Environment
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
import java.util.UUID

@MicronautTest
@Requires(env = [Environment.TEST])
internal class DeploymentMetadataApiControllerTest {
  @Inject
  lateinit var deploymentMetadataHandler: DeploymentMetadataHandler

  @Inject
  @Client("/")
  lateinit var client: HttpClient

  @MockBean(DeploymentMetadataHandler::class)
  fun deploymentMetadataHandler(): DeploymentMetadataHandler = mockk()

  @Test
  fun testFetchDeploymentMetadata() {
    val deploymentMetadataRead =
      DeploymentMetadataRead()
        .id(UUID.randomUUID())
        .mode(Configs.DeploymentMode.OSS.name)
        .version("0.2.3")

    every { deploymentMetadataHandler.deploymentMetadata } returns deploymentMetadataRead

    val path = "/api/v1/deployment/metadata"
    assertStatus(HttpStatus.OK, client.status(HttpRequest.POST<Any>(path, null)))
  }
}
