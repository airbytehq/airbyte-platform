/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.storage.activities

import io.airbyte.commons.json.JsonSerde
import io.airbyte.commons.storage.StorageClient
import io.airbyte.config.StandardSyncOutput
import io.airbyte.metrics.MetricClient
import io.airbyte.metrics.OssMetricsRegistry
import io.airbyte.workers.models.RefreshSchemaActivityOutput
import io.micrometer.core.instrument.Counter
import io.mockk.every
import io.mockk.impl.annotations.MockK
import io.mockk.junit5.MockKExtension
import io.mockk.mockk
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

  private var comparator = NaiveEqualityComparator<StandardSyncOutput>()

  @BeforeEach
  fun setup() {
    client = ActivityPayloadStorageClient(storageClientRaw, serde, metricClient)

    every { metricClient.count(metric = any(), value = any(), attributes = anyVararg()) } returns mockk<Counter>()

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

  @Test
  fun `validateOutput records a result for a match`() {
    val uri = ActivityPayloadURI("id", "version")
    val syncOutput = StandardSyncOutput().withAdditionalProperty("some", "unique-value-1")

    every { serde.deserialize(any(), StandardSyncOutput::class.java) } returns syncOutput

    client.validateOutput(uri, StandardSyncOutput::class.java, syncOutput, comparator, listOf())

    verify {
      metricClient.count(OssMetricsRegistry.PAYLOAD_VALIDATION_RESULT, 1, *anyVararg())
    }
  }

  @Test
  fun `validateOutput records a result for a mismatch`() {
    val uri = ActivityPayloadURI("id", "version")
    val syncOutput1 = StandardSyncOutput().withAdditionalProperty("some", "unique-value-1")
    val syncOutput2 = StandardSyncOutput().withAdditionalProperty("some", "unique-value-2")

    every { serde.deserialize(any(), StandardSyncOutput::class.java) } returns syncOutput2

    client.validateOutput(uri, StandardSyncOutput::class.java, syncOutput1, comparator, listOf())

    verify {
      metricClient.count(OssMetricsRegistry.PAYLOAD_VALIDATION_RESULT, 1, *anyVararg())
    }
  }

  @Test
  fun `validateOutput records a result for a read miss`() {
    val uri = ActivityPayloadURI("id", "version")
    val syncOutput = StandardSyncOutput().withAdditionalProperty("some", "unique-value-1")

    every { storageClientRaw.read(uri.id) } returns null

    client.validateOutput(uri, StandardSyncOutput::class.java, syncOutput, comparator, listOf())

    verify {
      metricClient.count(OssMetricsRegistry.PAYLOAD_VALIDATION_RESULT, 1, *anyVararg())
    }
  }

  @Test
  fun `validateOutput records read failure for null uri`() {
    val uri = null
    val syncOutput = StandardSyncOutput().withAdditionalProperty("some", "unique-value-1")

    client.validateOutput(uri, StandardSyncOutput::class.java, syncOutput, comparator, listOf())

    verify {
      metricClient.count(OssMetricsRegistry.PAYLOAD_FAILURE_READ, 1, *anyVararg())
    }
  }

  @Test
  fun `validateOutput records read failure on client read exception`() {
    val uri = ActivityPayloadURI("id", "version")
    val syncOutput = StandardSyncOutput().withAdditionalProperty("some", "unique-value-1")

    every { storageClientRaw.read(uri.id) } throws RuntimeException("yikes")

    client.validateOutput(uri, StandardSyncOutput::class.java, syncOutput, comparator, listOf())

    verify {
      metricClient.count(OssMetricsRegistry.PAYLOAD_FAILURE_READ, 1, *anyVararg())
    }
  }
}
