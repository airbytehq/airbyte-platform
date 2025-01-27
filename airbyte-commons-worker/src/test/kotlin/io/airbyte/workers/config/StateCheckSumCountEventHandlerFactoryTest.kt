/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.config

import io.micronaut.context.annotation.Property
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.time.Instant
import java.util.UUID
import javax.inject.Inject

@MicronautTest
@Property(name = "airbyte.version", value = "1.0.0")
@Property(name = "airbyte.deployment-mode", value = "OSS")
@Property(name = "airbyte.internal-api.base-path", value = "http://localhost/")
@Property(name = "airbyte.internal-api.connect-timeout-seconds", value = "10")
@Property(name = "airbyte.internal-api.read-timeout-seconds", value = "10")
@Property(name = "airbyte.internal-api.auth-header.name", value = "name")
@Property(name = "micronaut.application.name", value = "test-app")
@Property(name = "airbyte.cloud.pubsub.error-reporting.strategy", value = "logging")
internal class StateCheckSumCountEventHandlerFactoryTest {
  @Inject
  lateinit var sumCountEventHandlerFactory: StateCheckSumCountEventHandlerFactory

  @Test
  internal fun `should create bean properly`() {
    val currentTimeInMicroSecond1 = Instant.now().toEpochMilli() * 1000
    Thread.sleep(1)
    val stateCheckSumEventHandler = sumCountEventHandlerFactory.get(UUID.randomUUID(), UUID.randomUUID(), 1, 1)
    val currentTimeInMicroSecond2 = stateCheckSumEventHandler.getCurrentTimeInMicroSecond()
    assertTrue(currentTimeInMicroSecond2 > currentTimeInMicroSecond1)
  }
}
