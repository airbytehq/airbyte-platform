/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.container.orchestrator.config

import com.google.api.gax.batching.BatchingSettings
import com.google.cloud.pubsub.v1.Publisher
import com.google.pubsub.v1.TopicName
import io.micronaut.context.annotation.Factory
import io.micronaut.context.annotation.Requires
import io.micronaut.context.annotation.Value
import jakarta.inject.Named
import jakarta.inject.Singleton
import org.threeten.bp.Duration
import java.util.function.Supplier

private const val MAX_BATCH_SIZE = 50L
private const val MAX_BYTES_THRESHOLD = 5000L
private const val MAX_DELAY_THRESHOLD = 100L

@Factory
class StateCheckSumEventPubSubPublisherFactory {
  @Singleton
  @Requires(property = "airbyte.cloud.pubsub.enabled", value = "true", defaultValue = "false")
  @Named("pubSubPublisherSupplier")
  fun pubSubPublisherSupplier(
    @Value("\${airbyte.cloud.pubsub.topic}") topicName: String,
    @Value("\${airbyte.cloud.pubsub.request-bytes-threshold}") requestBytesThreshold: Int,
    @Value("\${airbyte.cloud.pubsub.message-count-batch-size}") messageCountBatchSize: Int,
    @Value("\${airbyte.cloud.pubsub.publish-delay-threshold-ms}") publishDelayThreshold: Int,
  ): Supplier<Publisher> {
    val batchingSettings =
      BatchingSettings
        .newBuilder()
        .setElementCountThreshold(messageCountBatchSize.toLong().coerceAtMost(MAX_BATCH_SIZE))
        .setRequestByteThreshold(requestBytesThreshold.toLong().coerceAtMost(MAX_BYTES_THRESHOLD))
        .setDelayThreshold(Duration.ofMillis(publishDelayThreshold.toLong().coerceAtMost(MAX_DELAY_THRESHOLD)))
        .build()

    return Supplier {
      Publisher.newBuilder(TopicName.parse(topicName)).setBatchingSettings(batchingSettings).build()
    }
  }
}
