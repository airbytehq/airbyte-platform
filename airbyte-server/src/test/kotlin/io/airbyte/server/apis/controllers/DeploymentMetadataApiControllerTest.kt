/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.model.generated.DeploymentMetadataRead
import io.airbyte.commons.server.handlers.DeploymentMetadataHandler
import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.UUID

class DeploymentMetadataApiControllerTest {
  private lateinit var deploymentMetadataHandler: DeploymentMetadataHandler
  private lateinit var controller: DeploymentMetadataApiController

  @BeforeEach
  fun setup() {
    deploymentMetadataHandler = mockk()
    controller = DeploymentMetadataApiController(deploymentMetadataHandler)
  }

  @Test
  fun testFetchDeploymentMetadata() {
    val expected =
      DeploymentMetadataRead()
        .id(UUID.randomUUID())
        .mode("COMMUNITY")
        .version("0.2.3")

    every { deploymentMetadataHandler.getDeploymentMetadata() } returns expected

    val result = controller.getDeploymentMetadata()
    assertEquals(expected, result)
  }
}
