/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.directjobrunner.connect

import io.airbyte.commons.json.Jsons
import io.airbyte.config.ConnectErrorCode
import io.airbyte.config.ConnectInput
import io.airbyte.config.ConnectOutput
import io.airbyte.config.WorkloadType
import io.airbyte.workers.serde.PayloadDeserializer
import io.airbyte.workers.workload.WorkloadOutputWriter
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import io.mockk.verifyOrder
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.util.UUID

class ConnectJobHandlerTest {
  private val probe = mockk<ConnectProbe>()
  private val outputWriter = mockk<WorkloadOutputWriter>(relaxed = true)
  private val handler = ConnectJobHandler(PayloadDeserializer(), probe, outputWriter)
  private val workloadId = "workload-1"
  private val input = ConnectInput("db.example.com", 5432, UUID.randomUUID(), UUID.randomUUID())

  @Test
  fun `handles connect workloads`() {
    assertEquals(WorkloadType.CONNECT, handler.workloadType)
  }

  @Test
  fun `probes the input and writes the output`() {
    val output = ConnectOutput(true, ConnectErrorCode.NONE, "ok", listOf("8.8.8.8"), 5)
    every { probe.probe(input) } returns output

    handler.run(workloadId, Jsons.serialize(input))

    verifyOrder {
      probe.probe(input)
      outputWriter.writeConnectOutput(workloadId, output)
    }
  }

  @Test
  fun `an unreachable host is written as a normal result`() {
    val output = ConnectOutput(false, ConnectErrorCode.CONNECTION_REFUSED, "refused", listOf("8.8.8.8"), 5)
    every { probe.probe(input) } returns output

    handler.run(workloadId, Jsons.serialize(input))

    verify(exactly = 1) { outputWriter.writeConnectOutput(workloadId, output) }
  }

  @Test
  fun `an invalid payload throws without probing`() {
    assertThrows<Exception> { handler.run(workloadId, "not json") }

    verify(exactly = 0) { probe.probe(any()) }
    verify(exactly = 0) { outputWriter.writeConnectOutput(any(), any()) }
  }

  @Test
  fun `an output write failure propagates`() {
    val output = ConnectOutput(true, ConnectErrorCode.NONE, "ok", emptyList(), 5)
    every { probe.probe(input) } returns output
    every { outputWriter.writeConnectOutput(workloadId, output) } throws RuntimeException("storage down")

    assertThrows<RuntimeException> { handler.run(workloadId, Jsons.serialize(input)) }
  }
}
