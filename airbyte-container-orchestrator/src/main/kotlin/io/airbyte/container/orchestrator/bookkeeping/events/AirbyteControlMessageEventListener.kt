/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.container.orchestrator.bookkeeping.events

import io.airbyte.commons.converters.ConnectorConfigUpdater
import io.airbyte.container.orchestrator.bookkeeping.AirbyteMessageOrigin
import io.airbyte.container.orchestrator.worker.context.ReplicationContext
import io.airbyte.metrics.MetricAttribute
import io.airbyte.metrics.MetricClient
import io.airbyte.metrics.OssMetricsRegistry
import io.airbyte.metrics.lib.MetricTags
import io.airbyte.protocol.models.v0.AirbyteControlMessage
import io.airbyte.protocol.models.v0.AirbyteMessage
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.event.ApplicationEventListener
import jakarta.inject.Singleton

private val logger = KotlinLogging.logger { }

/**
 * Custom application listener that handles Airbyte Protocol [AirbyteMessage.Type.CONTROL] messages produced
 * by both sources and destinations. It handles the messages synchronously to ensure that all
 * control messages are processed before continuing with replication.
 */
@Singleton
class AirbyteControlMessageEventListener(
  private val connectorConfigUpdater: ConnectorConfigUpdater,
  private val metricClient: MetricClient,
) : ApplicationEventListener<ReplicationAirbyteMessageEvent> {
  override fun onApplicationEvent(event: ReplicationAirbyteMessageEvent): Unit =
    when (event.airbyteMessageOrigin) {
      AirbyteMessageOrigin.DESTINATION -> acceptDstControlMessage(event.airbyteMessage.control, event.replicationContext)
      AirbyteMessageOrigin.SOURCE -> acceptSrcControlMessage(event.airbyteMessage.control, event.replicationContext)
      else -> logger.warn { "Invalid event from ${event.airbyteMessageOrigin} message origin for message: ${event.airbyteMessage}" }
    }

  override fun supports(event: ReplicationAirbyteMessageEvent) = event.airbyteMessage.type == AirbyteMessage.Type.CONTROL

  private fun acceptDstControlMessage(
    msg: AirbyteControlMessage,
    ctx: ReplicationContext,
  ): Unit =
    when (msg.type) {
      AirbyteControlMessage.Type.CONNECTOR_CONFIG ->
        try {
          connectorConfigUpdater.updateDestination(ctx.destinationId, msg.connectorConfig.config)
        } catch (e: Exception) {
          metricClient.count(
            metric = OssMetricsRegistry.CONNECTOR_CONFIG_PERSISTENCE_FAILURE,
            attributes =
              arrayOf(
                MetricAttribute(MetricTags.CONNECTION_ID, ctx.connectionId.toString()),
                MetricAttribute(MetricTags.DESTINATION_ID, ctx.destinationId.toString()),
                MetricAttribute(MetricTags.JOB_ID, ctx.jobId.toString()),
              ),
          )
          logger.error(e) { "Connector config persistence failed for destination ${ctx.destinationId}." }
        }
      else -> logger.debug { "Control message type ${msg.type} not supported." }
    }

  private fun acceptSrcControlMessage(
    msg: AirbyteControlMessage,
    ctx: ReplicationContext,
  ): Unit =
    when (msg.type) {
      AirbyteControlMessage.Type.CONNECTOR_CONFIG ->
        try {
          connectorConfigUpdater.updateSource(ctx.sourceId, msg.connectorConfig.config)
        } catch (e: Exception) {
          metricClient.count(
            metric = OssMetricsRegistry.CONNECTOR_CONFIG_PERSISTENCE_FAILURE,
            attributes =
              arrayOf(
                MetricAttribute(MetricTags.CONNECTION_ID, ctx.connectionId.toString()),
                MetricAttribute(MetricTags.SOURCE_ID, ctx.sourceId.toString()),
                MetricAttribute(MetricTags.JOB_ID, ctx.jobId.toString()),
              ),
          )
          logger.error(e) { "Connector config persistence failed for source ${ctx.sourceId}." }
        }
      else -> logger.debug { "Control message type ${msg.type} not supported." }
    }
}
