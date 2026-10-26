/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers

import io.airbyte.commons.version.AirbyteVersion
import io.airbyte.config.Configs.AirbyteEdition
import io.micronaut.context.env.Environment
import io.mockk.every
import io.mockk.mockk
import org.jooq.DSLContext
import org.jooq.Record
import org.jooq.Result
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import java.util.UUID

internal class DeploymentMetadataHandlerTest {
  @ParameterizedTest
  @ValueSource(strings = [Environment.TEST])
  fun testRetrievingDeploymentMetadata(activeEnvironment: String) {
    val deploymentId = UUID.randomUUID()
    val version = "0.1.2"
    val airbyteVersion = AirbyteVersion(version)
    val airbyteEdition = AirbyteEdition.COMMUNITY
    val dslContext = mockk<DSLContext>()
    val environment = mockk<Environment>()
    val result = mockk<Result<Record?>>()

    every { result.getValue(any<Int>(), any<String>()) } returns deploymentId
    every { dslContext.fetch(any<String>()) } returns result
    every { environment.activeNames } returns setOf(activeEnvironment).toMutableSet()

    val handler = DeploymentMetadataHandler(airbyteVersion, airbyteEdition, dslContext)

    val deploymentMetadataRead = handler.getDeploymentMetadata()

    Assertions.assertEquals(deploymentId, deploymentMetadataRead.id)
    Assertions.assertEquals(airbyteEdition.name, deploymentMetadataRead.mode)
    Assertions.assertEquals(version, deploymentMetadataRead.version)
  }
}
