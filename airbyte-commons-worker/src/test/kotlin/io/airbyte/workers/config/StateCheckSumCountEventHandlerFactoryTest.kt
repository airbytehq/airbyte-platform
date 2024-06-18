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
