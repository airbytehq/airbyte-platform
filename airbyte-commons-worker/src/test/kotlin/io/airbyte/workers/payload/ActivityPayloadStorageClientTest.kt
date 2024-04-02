package io.airbyte.workers.payload

import io.airbyte.commons.json.JsonSerde
import io.airbyte.config.StandardSyncOutput
import io.airbyte.metrics.lib.MetricClient
import io.airbyte.workers.models.RefreshSchemaActivityOutput
import io.airbyte.workers.storage.StorageClient
import io.mockk.every
import io.mockk.impl.annotations.MockK
import io.mockk.junit5.MockKExtension
import io.mockk.verify
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith

@ExtendWith(MockKExtension::class)
class ActivityPayloadStorageClientTest {
  @MockK
  private lateinit var metricClient: MetricClient

  @MockK
  private lateinit var storageClientRaw: StorageClient

  @MockK
  private lateinit var serde: JsonSerde

  private lateinit var client: ActivityPayloadStorageClient

  @BeforeEach
  fun setup() {
    client = ActivityPayloadStorageClient(storageClientRaw, serde, metricClient)

    every { metricClient.count(any(), any()) } returns Unit

    every { storageClientRaw.write(any(), any()) } returns Unit

    every { storageClientRaw.read(any()) } returns ""
  }

  @Test
  fun `readJSON reads json and unmarshalls to specified class for a given uri`() {
    val syncOutput = StandardSyncOutput().withAdditionalProperty("some", "unique-value-1")
    val refreshOutput = RefreshSchemaActivityOutput()

    every {
      storageClientRaw.read("sync-output")
    } returns "serialized-sync-output"

    every {
      serde.deserialize("serialized-sync-output", StandardSyncOutput::class.java)
    } returns syncOutput

    val result1 = client.readJSON<StandardSyncOutput>(ActivityPayloadURI("sync-output"))

    Assertions.assertEquals(syncOutput, result1)

    every {
      storageClientRaw.read("refresh-output")
    } returns "serialized-refresh-output"

    every {
      serde.deserialize("serialized-refresh-output", RefreshSchemaActivityOutput::class.java)
    } returns refreshOutput

    val result2 = client.readJSON<RefreshSchemaActivityOutput>(ActivityPayloadURI("refresh-output"))

    Assertions.assertEquals(refreshOutput, result2)
  }

  @Test
  fun `readJSON handles null`() {
    every {
      storageClientRaw.read("sync-output")
    } returns null

    val result = client.readJSON<StandardSyncOutput>(ActivityPayloadURI("sync-output"))

    Assertions.assertNull(result)
  }

  @Test
  fun `writeJSON serializes to json and writes to a given uri`() {
    val syncOutput = StandardSyncOutput().withAdditionalProperty("some", "unique-value-1")

    every {
      serde.serialize(syncOutput)
    } returns "serialized-sync-output"

    client.writeJSON(ActivityPayloadURI("sync-output"), syncOutput)

    verify { storageClientRaw.write("sync-output", "serialized-sync-output") }
  }
}
