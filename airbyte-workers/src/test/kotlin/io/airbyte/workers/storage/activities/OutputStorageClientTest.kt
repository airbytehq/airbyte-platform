/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.storage.activities

import io.airbyte.metrics.MetricClient
import io.micrometer.core.instrument.Counter
import io.mockk.every
import io.mockk.impl.annotations.MockK
import io.mockk.junit5.MockKExtension
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertDoesNotThrow
import org.junit.jupiter.api.extension.ExtendWith
import java.util.UUID

@ExtendWith(MockKExtension::class)
class OutputStorageClientTest {
  @MockK
  private lateinit var metricClient: MetricClient

  @MockK
  private lateinit var storageClient: ActivityPayloadStorageClient

  private lateinit var client: OutputStorageClient<TestClass>

  class TestClass(
    value1: String,
    value2: Long,
  )

  @BeforeEach
  fun setup() {
    client = OutputStorageClient(storageClient, metricClient, Fixtures.TEST_PAYLOAD_NAME, TestClass::class.java)

    every { metricClient.count(metric = any(), value = any(), attributes = anyVararg()) } returns mockk<Counter>()
  }

  @Test
  fun `persist writes json to storage`() {
    val obj = TestClass("test", 123)
    client.persist(obj, Fixtures.CONNECTION_ID, Fixtures.JOB_ID, Fixtures.ATTEMPT_NUMBER, arrayOf())

    verify(exactly = 1) { storageClient.writeJSON(any(), obj) }
  }

  @Test
  fun `persist short circuits if input null`() {
    client.persist(null, Fixtures.CONNECTION_ID, Fixtures.JOB_ID, Fixtures.ATTEMPT_NUMBER, arrayOf())

    verify(exactly = 0) { storageClient.writeJSON(any(), any()) }
  }

  @Test
  fun `persist swallows exceptions`() {
    val obj = TestClass("test", 123)

    every { storageClient.writeJSON(any(), any()) } throws Exception("bang")

    assertDoesNotThrow {
      client.persist(
        obj,
        Fixtures.CONNECTION_ID,
        Fixtures.JOB_ID,
        Fixtures.ATTEMPT_NUMBER,
        arrayOf(),
      )
    }
  }

  object Fixtures {
    const val TEST_PAYLOAD_NAME = "test-payload"
    val CONNECTION_ID: UUID = UUID.randomUUID()
    const val JOB_ID = 9987124L
    const val ATTEMPT_NUMBER = 2
  }
}
