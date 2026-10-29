/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.directjobrunner.connect

import io.airbyte.config.WorkloadType
import io.airbyte.directjobrunner.core.DirectJobHandler
import io.airbyte.workers.serde.PayloadDeserializer
import io.airbyte.workers.workload.WorkloadOutputWriter
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Singleton
import java.time.Duration

private val logger = KotlinLogging.logger {}

/** Runs CONNECT workloads: a TCP connection test from this data plane to a user-supplied host and port. */
@Singleton
class ConnectJobHandler(
  private val payloadDeserializer: PayloadDeserializer,
  private val probe: ConnectProbe,
  private val outputWriter: WorkloadOutputWriter,
) : DirectJobHandler {
  override val workloadType: WorkloadType = WorkloadType.CONNECT

  override val runningDeadline: Duration = Duration.ofMinutes(2)

  override fun run(
    workloadId: String,
    inputPayload: String,
  ) {
    val input = payloadDeserializer.toConnectInput(inputPayload)
    val output = probe.probe(input)
    logger.info { "Connect probe for workload $workloadId finished: reachable=${output.reachable} errorCode=${output.errorCode}" }

    outputWriter.writeConnectOutput(workloadId, output)
  }
}
