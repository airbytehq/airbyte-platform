/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.micronaut.runtime

import io.micronaut.context.annotation.Property
import io.micronaut.context.env.Environment
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import jakarta.inject.Inject
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.util.UUID

@MicronautTest(environments = [Environment.TEST])
@Property(name = "micronaut.server.port", value = "-1")
internal class AirbyteContextConfigDefaultTest {
  @Inject
  private lateinit var airbyteContextConfig: AirbyteContextConfig

  @Test
  fun testLoadingValuesFromConfig() {
    assertEquals(DEFAULT_CONTEXT_ATTEMPT_ID, airbyteContextConfig.attemptId)
    assertEquals("", airbyteContextConfig.connectionId)
    assertEquals(DEFAULT_CONTEXT_JOB_ID, airbyteContextConfig.jobId)
    assertEquals("", airbyteContextConfig.workloadId)
    assertEquals("", airbyteContextConfig.workspaceId)
  }
}

@MicronautTest(propertySources = ["classpath:application-context.yml"])
@Property(name = "micronaut.server.port", value = "-1")
internal class AirbyteContextConfigOverridesTest {
  @Inject
  private lateinit var airbyteContextConfig: AirbyteContextConfig

  @Test
  fun testLoadingValuesFromConfig() {
    val connectionId = "2c7c3141-16e0-4558-9033-55b52350d4c7"
    val workspaceId = "3a23cb17-912e-4f03-b2ec-8fe25dff0962"
    assertEquals(3, airbyteContextConfig.attemptId)
    assertEquals(connectionId, airbyteContextConfig.connectionId)
    assertEquals(UUID.fromString(connectionId), airbyteContextConfig.connectionIdAsUUID())
    assertEquals(1234L, airbyteContextConfig.jobId)
    assertEquals("test-workload-id", airbyteContextConfig.workloadId)
    assertEquals(workspaceId, airbyteContextConfig.workspaceId)
    assertEquals(UUID.fromString(workspaceId), airbyteContextConfig.workspaceIdAsUUID())
  }
}
