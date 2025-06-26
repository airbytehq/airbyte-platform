/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.signal

import io.airbyte.api.client.AirbyteApiClient
import io.airbyte.api.client.generated.SignalApi
import io.airbyte.commons.json.Jsons
import io.airbyte.config.SignalInput
import io.airbyte.metrics.MetricClient
import io.airbyte.workload.repository.domain.WorkloadType
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import io.airbyte.api.client.model.generated.SignalInput as ApiSignalInput

class ApiSignalSenderTest {
  private lateinit var airbyteApi: AirbyteApiClient
  private lateinit var signalApiMock: SignalApi
  private lateinit var metricClient: MetricClient
  private lateinit var signalSender: ApiSignalSender

  @BeforeEach
  fun setup() {
    signalApiMock = mockk(relaxed = true)
    airbyteApi =
      mockk {
        every { signalApi } returns signalApiMock
      }
    metricClient = mockk(relaxed = true)
    signalSender = ApiSignalSender(airbyteApi, metricClient)
  }

  @Test
  fun `sendSignal calls the underlying handler with the input`() {
    val workloadType = WorkloadType.CHECK
    val signal = SignalInput(SignalInput.SYNC_WORKFLOW, "my-workflow-id")

    signalSender.sendSignal(workloadType, Jsons.serialize(signal))
    verify { signalApiMock.signal(ApiSignalInput(signal.workflowType, signal.workflowId)) }
  }
}
