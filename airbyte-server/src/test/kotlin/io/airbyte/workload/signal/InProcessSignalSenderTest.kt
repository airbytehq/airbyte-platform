/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.signal

import io.airbyte.commons.server.handlers.SignalHandler
import io.airbyte.config.SignalInput
import io.airbyte.metrics.MetricClient
import io.airbyte.protocol.models.Jsons
import io.airbyte.workload.repository.domain.WorkloadType
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

class InProcessSignalSenderTest {
  private lateinit var signalHandler: SignalHandler
  private lateinit var metricClient: MetricClient
  private lateinit var signalSender: InProcessSignalSender

  @BeforeEach
  fun setup() {
    signalHandler = mockk(relaxed = true)
    metricClient = mockk(relaxed = true)
    signalSender = InProcessSignalSender(signalHandler, metricClient)
  }

  @Test
  fun `sendSignal calls the underlying handler with the input`() {
    val workloadType = WorkloadType.CHECK
    val signal = SignalInput(SignalInput.SYNC_WORKFLOW, "my-workflow-id")

    signalSender.sendSignal(workloadType, Jsons.serialize(signal))
    verify { signalHandler.signal(signal) }
  }
}
