package io.airbyte.workers.storage.activities

import io.airbyte.metrics.lib.MetricClient
import io.airbyte.workers.storage.activities.OutputStorageClientTest.Fixtures.ATTEMPT_NUMBER
import io.airbyte.workers.storage.activities.OutputStorageClientTest.Fixtures.CONNECTION_ID
import io.airbyte.workers.storage.activities.OutputStorageClientTest.Fixtures.JOB_ID
import io.airbyte.workers.storage.activities.OutputStorageClientTest.Fixtures.TEST_PAYLOAD_NAME
import io.mockk.every
import io.mockk.impl.annotations.MockK
import io.mockk.junit5.MockKExtension
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

  class TestClass(value1: String, value2: Long)

  @BeforeEach
  fun setup() {
    client = OutputStorageClient(storageClient, metricClient, TEST_PAYLOAD_NAME, TestClass::class.java)

    every { metricClient.count(any(), any(), *anyVararg()) } returns Unit
  }

  @Test
  fun `persist writes json to storage`() {
    val obj = TestClass("test", 123)
    client.persist(obj, CONNECTION_ID, JOB_ID, ATTEMPT_NUMBER, arrayOf())

    verify(exactly = 1) { storageClient.writeJSON(any(), obj) }
  }

  @Test
  fun `persist short circuits if input null`() {
    client.persist(null, CONNECTION_ID, JOB_ID, ATTEMPT_NUMBER, arrayOf())

    verify(exactly = 0) { storageClient.writeJSON(any(), any()) }
  }

  @Test
  fun `persist swallows exceptions`() {
    val obj = TestClass("test", 123)

    every { storageClient.writeJSON(any(), any()) } throws Exception("bang")

    assertDoesNotThrow { client.persist(obj, CONNECTION_ID, JOB_ID, ATTEMPT_NUMBER, arrayOf()) }
  }

  object Fixtures {
    const val TEST_PAYLOAD_NAME = "test-payload"
    val CONNECTION_ID: UUID = UUID.randomUUID()
    const val JOB_ID = 9987124L
    const val ATTEMPT_NUMBER = 2
  }
}
