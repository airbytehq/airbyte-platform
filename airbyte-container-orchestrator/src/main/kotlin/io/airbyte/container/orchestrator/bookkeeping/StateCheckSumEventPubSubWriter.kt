/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.container.orchestrator.bookkeeping

import com.google.cloud.pubsub.v1.Publisher
import com.google.protobuf.ByteString
import com.google.pubsub.v1.PubsubMessage
import io.airbyte.commons.json.Jsons
import io.airbyte.container.orchestrator.worker.model.StateCheckSumCountEvent
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.Requires
import jakarta.inject.Named
import jakarta.inject.Singleton
import java.util.concurrent.TimeUnit
import java.util.function.Supplier

private val logger = KotlinLogging.logger { }

@Singleton
@Requires(property = "airbyte.cloud.pubsub.enabled", value = "true", defaultValue = "false")
@Named("pubSubWriter")
class StateCheckSumEventPubSubWriter(
  @Named("pubSubPublisherSupplier") private val publisherSupplier: Supplier<Publisher>,
) {
  private val publisher: Publisher by lazy { publisherSupplier.get() }

  fun publishEvent(event: List<StateCheckSumCountEvent>) {
    try {
      event.forEach { stateCheckSumEvent -> Jsons.serialize(stateCheckSumEvent)?.let { writeMessageToPubSub(it) } }
    } catch (e: Exception) {
      logger.error(e) { "Swallowed exception during publishing pubsub message" }
    }
  }

  private fun writeMessageToPubSub(message: String) {
    try {
      val data = ByteString.copyFromUtf8(message)
      val pubsubMessage: PubsubMessage =
        PubsubMessage
          .newBuilder()
          .setData(data)
          // Filter in subscription is set to -> attributes.event = "state_checksum_metrics"
          .putAllAttributes(mapOf("event" to "state_checksum_metrics"))
          .build()
      publisher.publish(pubsubMessage)
    } catch (e: Exception) {
      logger.error(e) { "Swallowed exception during writing pubsub message" }
    }
  }

  fun close() {
    try {
      logger.info { "Closing StateCheckSumEventPubSubWriter" }
      publisher.shutdown()
      publisher.awaitTermination(1, TimeUnit.MINUTES)
    } catch (e: Exception) {
      logger.error(e) { "Swallowed exception during closing pubsub class" }
    }
  }
}
