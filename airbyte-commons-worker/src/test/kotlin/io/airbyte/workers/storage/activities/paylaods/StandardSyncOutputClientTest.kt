package io.airbyte.workers.storage.activities.paylaods

import io.airbyte.config.StandardSyncOutput
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.featureflag.ReadReplicationOutputFromObjectStorage
import io.airbyte.featureflag.WriteReplicationOutputToObjectStorage
import io.airbyte.metrics.lib.MetricClient
import io.airbyte.workers.storage.activities.ActivityPayloadStorageClient
import io.airbyte.workers.storage.activities.ActivityPayloadURI
import io.airbyte.workers.storage.activities.paylaods.StandardSyncOutputClientTest.Fixtures.ATTEMPT_NUMBER
import io.airbyte.workers.storage.activities.paylaods.StandardSyncOutputClientTest.Fixtures.CONNECTION_ID
import io.airbyte.workers.storage.activities.paylaods.StandardSyncOutputClientTest.Fixtures.JOB_ID
import io.airbyte.workers.storage.activities.payloads.StandardSyncOutputClient
import io.mockk.every
import io.mockk.impl.annotations.MockK
import io.mockk.junit5.MockKExtension
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import java.util.UUID
import io.airbyte.config.ActivityPayloadURI as OpenApiURI

@ExtendWith(MockKExtension::class)
class StandardSyncOutputClientTest {
  @MockK
  private lateinit var metricClient: MetricClient

  @MockK
  private lateinit var storageClient: ActivityPayloadStorageClient

  @MockK
  private lateinit var featureFlagClient: FeatureFlagClient

  private lateinit var client: StandardSyncOutputClient

  @BeforeEach
  fun setup() {
    client = StandardSyncOutputClient(storageClient, metricClient, featureFlagClient)

    every { metricClient.count(any(), any(), *anyVararg()) } returns Unit
  }

  @Test
  fun `hydrate short circuits and returns null if input null`() {
    val result = client.hydrate(null, CONNECTION_ID, JOB_ID, ATTEMPT_NUMBER)

    assertNull(result)

    verify(exactly = 0) { storageClient.readJSON<StandardSyncOutput>(any(), any()) }
  }

  @Test
  fun `hydrate short circuits returns input if input uri un-convertable`() {
    val input = StandardSyncOutput().withUri(null)
    val result = client.hydrate(input, CONNECTION_ID, JOB_ID, ATTEMPT_NUMBER)

    assertEquals(input, result)

    verify(exactly = 0) { storageClient.readJSON<StandardSyncOutput>(any(), any()) }
  }

  @Test
  fun `hydrate returns input and validates if read FF off`() {
    val validUri = OpenApiURI().withId("id").withVersion("version")
    val input = StandardSyncOutput().withUri(validUri)

    every { featureFlagClient.boolVariation(ReadReplicationOutputFromObjectStorage, any()) } returns false

    every { storageClient.validateOutput<StandardSyncOutput>(any(), any(), any(), any(), any()) } returns input

    val result = client.hydrate(input, CONNECTION_ID, JOB_ID, ATTEMPT_NUMBER)

    assertEquals(input, result)

    verify(exactly = 1) { storageClient.validateOutput<StandardSyncOutput>(any(), any(), any(), any(), any()) }
    verify(exactly = 0) { storageClient.readJSON<StandardSyncOutput>(any(), any()) }
  }

  @Test
  fun `hydrate reads from storage and returns output if read FF on`() {
    val apiUri = OpenApiURI().withId("id").withVersion("version")
    val input = StandardSyncOutput().withUri(apiUri)
    val output = StandardSyncOutput().withUri(apiUri).withAdditionalProperty("hydrated", "value")

    every { featureFlagClient.boolVariation(ReadReplicationOutputFromObjectStorage, any()) } returns true

    every { storageClient.readJSON(any(), StandardSyncOutput::class.java) } returns output

    val result = client.hydrate(input, CONNECTION_ID, JOB_ID, ATTEMPT_NUMBER)

    assertEquals(output, result)
  }

  @Test
  fun `persist short circuits and returns null if input null`() {
    val result = client.persistAndTrim(null, CONNECTION_ID, JOB_ID, ATTEMPT_NUMBER, arrayOf())

    assertNull(result)

    verify(exactly = 0) { storageClient.writeJSON<StandardSyncOutput>(any(), any()) }
  }

  @Test
  fun `persist short circuits and returns input if write FF off`() {
    val input = StandardSyncOutput().withAdditionalProperty("some", "property")

    every { featureFlagClient.boolVariation(WriteReplicationOutputToObjectStorage, any()) } returns false

    val result = client.persistAndTrim(input, CONNECTION_ID, JOB_ID, ATTEMPT_NUMBER, arrayOf())

    assertEquals(input, result)

    verify(exactly = 0) { storageClient.writeJSON<StandardSyncOutput>(any(), any()) }
  }

  @Test
  fun `persist writes the output if write FF on and returns the input with URI if read FF off`() {
    val input = StandardSyncOutput().withAdditionalProperty("some", "property")
    val uri = ActivityPayloadURI.v1(CONNECTION_ID, JOB_ID, ATTEMPT_NUMBER, StandardSyncOutputClient.PAYLOAD_NAME)
    val inputWithUri = StandardSyncOutput().withAdditionalProperty("some", "property").withUri(uri.toOpenApi())

    every { featureFlagClient.boolVariation(WriteReplicationOutputToObjectStorage, any()) } returns true
    every { featureFlagClient.boolVariation(ReadReplicationOutputFromObjectStorage, any()) } returns false

    val result = client.persistAndTrim(input, CONNECTION_ID, JOB_ID, ATTEMPT_NUMBER, arrayOf())

    assertEquals(inputWithUri, result)

    verify(exactly = 1) { storageClient.writeJSON<StandardSyncOutput>(any(), inputWithUri) }
  }

  @Test
  fun `persist writes the output if write FF on and returns a trimmed output if read FF on`() {
    val input = StandardSyncOutput().withAdditionalProperty("some", "property")
    val uri = ActivityPayloadURI.v1(CONNECTION_ID, JOB_ID, ATTEMPT_NUMBER, StandardSyncOutputClient.PAYLOAD_NAME)
    val inputWithUri = StandardSyncOutput().withAdditionalProperty("some", "property").withUri(uri.toOpenApi())
    val trimmed = StandardSyncOutput().withUri(uri.toOpenApi())

    every { featureFlagClient.boolVariation(WriteReplicationOutputToObjectStorage, any()) } returns true
    every { featureFlagClient.boolVariation(ReadReplicationOutputFromObjectStorage, any()) } returns true

    val result = client.persistAndTrim(input, CONNECTION_ID, JOB_ID, ATTEMPT_NUMBER, arrayOf())

    assertEquals(trimmed, result)

    verify(exactly = 1) { storageClient.writeJSON<StandardSyncOutput>(any(), inputWithUri) }
  }

  object Fixtures {
    val CONNECTION_ID: UUID = UUID.randomUUID()
    const val JOB_ID = 9987124L
    const val ATTEMPT_NUMBER = 2
  }
}
