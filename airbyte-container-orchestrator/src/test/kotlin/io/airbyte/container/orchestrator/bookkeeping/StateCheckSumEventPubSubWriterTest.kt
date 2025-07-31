/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.container.orchestrator.bookkeeping

import com.google.api.core.ApiFuture
import com.google.cloud.pubsub.v1.Publisher
import com.google.protobuf.ByteString
import com.google.pubsub.v1.PubsubMessage
import io.airbyte.commons.json.Jsons
import io.airbyte.container.orchestrator.worker.model.StateCheckSumCountEvent
import io.airbyte.workers.models.ArchitectureConstants.ORCHESTRATOR
import io.airbyte.workers.models.ArchitectureConstants.PLATFORM_MODE
import io.micronaut.context.annotation.Property
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import io.mockk.Runs
import io.mockk.clearAllMocks
import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import io.mockk.verify
import jakarta.inject.Inject
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.time.Instant
import java.util.Random
import java.util.UUID
import java.util.function.Supplier

@MicronautTest
@Property(name = PLATFORM_MODE, value = ORCHESTRATOR)
@Property(name = "airbyte.attempt-id", value = "1")
@Property(name = "airbyte.internal-api.auth.token-endpoint", value = "https://localhost:8080/test-token-endpoint")
@Property(name = "airbyte.auth.dataplane-client-id", value = "test")
@Property(name = "airbyte.auth.dataplane-client-secret", value = "test")
@Property(name = "airbyte.cloud.pubsub.enabled", value = "true")
@Property(name = "airbyte.cloud.pubsub.topic", value = "dummy-topic")
@Property(name = "airbyte.cloud.pubsub.message-count-batch-size", value = "10")
@Property(name = "airbyte.cloud.pubsub.request-bytes-threshold", value = "10")
@Property(name = "airbyte.cloud.pubsub.message-count-batch-size", value = "10")
@Property(name = "airbyte.cloud.pubsub.publish-delay-threshold-ms", value = "10")
@Property(name = "airbyte.cloud.storage.type", value = "LOCAL")
@Property(name = "airbyte.job-id", value = "1")
@Property(name = "airbyte.workspace-root", value = "/tmp")
@Property(name = "micronaut.http.services.workload-api.url", value = "http://localhost")
class StateCheckSumEventPubSubWriterTest {
  private val publisherSupplier = mockk<Supplier<Publisher>>(relaxed = true)

  private val publisher = mockk<Publisher>(relaxed = true)

  private lateinit var pubSubWriter: StateCheckSumEventPubSubWriter

  @Inject
  lateinit var pubSubWriterMicronautBean: StateCheckSumEventPubSubWriter

  @BeforeEach
  fun setUp() {
    every { publisherSupplier.get() } returns publisher
    pubSubWriter = StateCheckSumEventPubSubWriter(publisherSupplier)
  }

  @AfterEach
  fun tearDown() {
    clearAllMocks()
  }

  @Test
  fun `bean creation should work`() {
    pubSubWriterMicronautBean.publishEvent(listOf(getDummyCheckSumEvent()))
    pubSubWriterMicronautBean.close()
  }

  @Test
  fun `events should be written to pubsub`() {
    val event = getDummyCheckSumEvent()
    val expectedMessage: PubsubMessage =
      PubsubMessage
        .newBuilder()
        .setData(ByteString.copyFromUtf8(Jsons.serialize(event)))
        // Filter in subscription is set to -> attributes.event = "state_checksum_metrics"
        .putAllAttributes(mapOf("event" to "state_checksum_metrics"))
        .build()
    every { publisher.shutdown() } just Runs
    every { publisher.awaitTermination(any(), any()) } returns true
    every { publisher.publish(expectedMessage) } returns mockk<ApiFuture<String>>()

    pubSubWriter.publishEvent(listOf(event, event, event, event, event, event))
    pubSubWriter.close()

    verify(exactly = 6) { publisher.publish(expectedMessage) }
  }

  @Test
  fun `close should shutdown publisher`() {
    every { publisher.shutdown() } just Runs
    every { publisher.awaitTermination(any(), any()) } returns true

    pubSubWriter.close()

    verify(exactly = 1) { publisher.shutdown() }
  }

  private fun getDummyCheckSumEvent(): StateCheckSumCountEvent =
    StateCheckSumCountEvent(
      UUID.randomUUID().toString(),
      Random().nextLong(),
      UUID.randomUUID().toString(),
      "LOCAL_MACHINE",
      "LOCAL",
      "nope@nope.com",
      UUID.randomUUID().toString(),
      Random().nextLong(),
      Random().nextLong(),
      UUID.randomUUID().toString(),
      Random().nextLong().toString(),
      "SOURCE",
      "STREAM",
      "stream",
      null,
      Instant.now().toEpochMilli() * 1000L,
      Random().nextBoolean(),
    )
}
