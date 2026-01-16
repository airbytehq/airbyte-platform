/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.container.orchestrator.config

import com.google.api.gax.batching.BatchingSettings
import com.google.cloud.pubsub.v1.Publisher
import com.google.pubsub.v1.TopicName
import io.airbyte.micronaut.runtime.AirbyteCloudPubSubConfig
import io.airbyte.micronaut.runtime.CLOUD_PUBSUB_PREFIX
import io.airbyte.micronaut.runtime.DEFAULT_CLOUD_PUBSUB_MESSAGE_COUNT_BATCH_SIZE
import io.airbyte.micronaut.runtime.DEFAULT_CLOUD_PUBSUB_PUBLISH_DELAY_THRESHOLD_MS
import io.airbyte.micronaut.runtime.DEFAULT_CLOUD_PUBSUB_REQUEST_BYTES_THRESHOLD
import io.micronaut.context.annotation.Factory
import io.micronaut.context.annotation.Requires
import jakarta.inject.Named
import jakarta.inject.Singleton
import org.threeten.bp.Duration
import java.util.function.Supplier

@Factory
class StateCheckSumEventPubSubPublisherFactory {
  @Singleton
  @Requires(property = "$CLOUD_PUBSUB_PREFIX.enabled", value = "true", defaultValue = "false")
  @Named("pubSubPublisherSupplier")
  fun pubSubPublisherSupplier(airbyteCloudPubSubConfig: AirbyteCloudPubSubConfig): Supplier<Publisher> {
    val batchingSettings =
      BatchingSettings
        .newBuilder()
        .setElementCountThreshold(airbyteCloudPubSubConfig.messageCountBatchSize.coerceAtMost(DEFAULT_CLOUD_PUBSUB_MESSAGE_COUNT_BATCH_SIZE))
        .setRequestByteThreshold(airbyteCloudPubSubConfig.requestBytesThreshold.coerceAtMost(DEFAULT_CLOUD_PUBSUB_REQUEST_BYTES_THRESHOLD))
        .setDelayThreshold(
          Duration.ofMillis(airbyteCloudPubSubConfig.publishDelayThresholdMs.coerceAtMost(DEFAULT_CLOUD_PUBSUB_PUBLISH_DELAY_THRESHOLD_MS)),
        ).build()

    return Supplier {
      Publisher.newBuilder(TopicName.parse(airbyteCloudPubSubConfig.topic)).setBatchingSettings(batchingSettings).build()
    }
  }
}
