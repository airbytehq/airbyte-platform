/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers

import io.airbyte.commons.version.AirbyteVersion
import io.airbyte.config.Configs.AirbyteEdition
import io.micronaut.context.env.Environment
import org.jooq.DSLContext
import org.jooq.Record
import org.jooq.Result
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import org.mockito.ArgumentMatchers
import org.mockito.Mockito
import java.util.UUID

internal class DeploymentMetadataHandlerTest {
  @ParameterizedTest
  @ValueSource(strings = [Environment.TEST])
  fun testRetrievingDeploymentMetadata(activeEnvironment: String) {
    val deploymentId = UUID.randomUUID()
    val version = "0.1.2"
    val airbyteVersion = AirbyteVersion(version)
    val airbyteEdition = AirbyteEdition.COMMUNITY
    val dslContext = Mockito.mock<DSLContext>(DSLContext::class.java)
    val environment = Mockito.mock<Environment>(Environment::class.java)
    val result: Result<Record?> = Mockito.mock(Result::class.java) as Result<Record?>

    Mockito.`when`<Any?>(result.getValue(ArgumentMatchers.anyInt(), ArgumentMatchers.anyString())).thenReturn(deploymentId)
    Mockito.`when`<Result<Record?>?>(dslContext.fetch(ArgumentMatchers.anyString())).thenReturn(result)
    Mockito.`when`<MutableSet<String?>?>(environment.getActiveNames()).thenReturn(setOf(activeEnvironment).toMutableSet())

    val handler = DeploymentMetadataHandler(airbyteVersion, airbyteEdition, dslContext)

    val deploymentMetadataRead = handler.getDeploymentMetadata()

    Assertions.assertEquals(deploymentId, deploymentMetadataRead.getId())
    Assertions.assertEquals(airbyteEdition.name, deploymentMetadataRead.getMode())
    Assertions.assertEquals(version, deploymentMetadataRead.getVersion())
  }
}
