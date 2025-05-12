/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.container.orchestrator.worker

import io.airbyte.commons.resources.MoreResources
import io.airbyte.config.StandardSync
import io.airbyte.persistence.job.models.ReplicationInput
import io.airbyte.protocol.models.Jsons
import io.airbyte.protocol.models.v0.AirbyteMessage
import io.airbyte.protocol.models.v0.AirbyteStream
import io.airbyte.protocol.models.v0.AirbyteStreamNameNamespacePair
import io.airbyte.validation.json.JsonSchemaValidator
import io.airbyte.workers.WorkerUtils
import io.airbyte.workers.testutils.AirbyteMessageUtils
import io.airbyte.workers.testutils.TestConfigHelpers.createReplicationConfig
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.io.IOException
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentMap
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.function.Consumer

private val AIRBYTE_STREAM_NAME_NAMESPACE_PAIR: AirbyteStreamNameNamespacePair = AirbyteStreamNameNamespacePair("user_preferences", "")
private val STREAM_NAME: String = AIRBYTE_STREAM_NAME_NAMESPACE_PAIR.name
private const val FIELD_NAME: String = "favorite_color"
private val VALID_RECORD: AirbyteMessage = AirbyteMessageUtils.createRecordMessage(STREAM_NAME, FIELD_NAME, "blue")
private val INVALID_RECORD_1: AirbyteMessage = AirbyteMessageUtils.createRecordMessage(STREAM_NAME, FIELD_NAME, 3)
private val INVALID_RECORD_2: AirbyteMessage = AirbyteMessageUtils.createRecordMessage(STREAM_NAME, mapOf(FIELD_NAME to true))

internal class RecordSchemaValidatorTest {
  private lateinit var replicationInput: ReplicationInput
  private lateinit var validationErrors: ConcurrentMap<AirbyteStreamNameNamespacePair, Pair<MutableSet<String>, Int>?>
  private lateinit var uncountedValidationErrors: ConcurrentMap<AirbyteStreamNameNamespacePair, MutableSet<String>>

  @BeforeEach
  fun setup() {
    val syncPair: Pair<StandardSync, ReplicationInput> = createReplicationConfig()
    replicationInput = syncPair.second
    validationErrors = ConcurrentHashMap()
    uncountedValidationErrors = ConcurrentHashMap()
  }

  @Test
  @Throws(IOException::class)
  fun testValidateValidSchema() {
    RecordSchemaValidator(
      jsonSchemaValidator = JsonSchemaValidator(),
      schemaValidationExecutorService = Executors.newSingleThreadExecutor(),
      streamNamesToSchemas = WorkerUtils.mapStreamNamesToSchemas(replicationInput.catalog),
    ).use { recordSchemaValidator ->
      recordSchemaValidator.validateSchema(
        message = VALID_RECORD.record,
        AirbyteStreamNameNamespacePair.fromRecordMessage(VALID_RECORD.record),
        validationErrors = validationErrors,
      )
      assertEquals(0, validationErrors.size)
    }
  }

  @Test
  @Throws(IOException::class)
  fun testValidateValidSchemaWithoutCounting() {
    RecordSchemaValidator(
      jsonSchemaValidator = JsonSchemaValidator(),
      schemaValidationExecutorService = Executors.newSingleThreadExecutor(),
      streamNamesToSchemas = WorkerUtils.mapStreamNamesToSchemas(replicationInput.catalog),
    ).use { recordSchemaValidator ->
      recordSchemaValidator.validateSchemaWithoutCounting(
        message = VALID_RECORD.record,
        airbyteStream = AirbyteStreamNameNamespacePair.fromRecordMessage(VALID_RECORD.record),
        validationErrors = uncountedValidationErrors,
      )
      assertEquals(0, uncountedValidationErrors.size)
    }
  }

  @Test
  @Throws(InterruptedException::class)
  fun testValidateInvalidSchema() {
    val executorService = Executors.newSingleThreadExecutor()
    val recordSchemaValidator =
      RecordSchemaValidator(
        jsonSchemaValidator = JsonSchemaValidator(),
        schemaValidationExecutorService = executorService,
        streamNamesToSchemas = WorkerUtils.mapStreamNamesToSchemas(replicationInput.catalog),
      )
    recordSchemaValidator.initializeSchemaValidator()
    val messagesToValidate: MutableList<AirbyteMessage> = mutableListOf(INVALID_RECORD_1, INVALID_RECORD_2, VALID_RECORD)

    messagesToValidate.forEach(
      Consumer { message: AirbyteMessage ->
        recordSchemaValidator.validateSchema(
          message = message.record,
          airbyteStream = AIRBYTE_STREAM_NAME_NAMESPACE_PAIR,
          validationErrors = validationErrors,
        )
      },
    )
    executorService.awaitTermination(3, TimeUnit.SECONDS)
    assertEquals(1, validationErrors.size)
    assertEquals(2, validationErrors[AIRBYTE_STREAM_NAME_NAMESPACE_PAIR]?.second as Int)
  }

  @Test
  @Throws(InterruptedException::class)
  fun testValidateInvalidSchemaWithoutCounting() {
    val executorService = Executors.newSingleThreadExecutor()
    val recordSchemaValidator =
      RecordSchemaValidator(
        jsonSchemaValidator = JsonSchemaValidator(),
        schemaValidationExecutorService = executorService,
        streamNamesToSchemas = WorkerUtils.mapStreamNamesToSchemas(replicationInput.catalog),
      )
    recordSchemaValidator.initializeSchemaValidator()
    val messagesToValidate: MutableList<AirbyteMessage> = mutableListOf(INVALID_RECORD_1, INVALID_RECORD_2, VALID_RECORD)

    messagesToValidate.forEach(
      Consumer { message: AirbyteMessage ->
        recordSchemaValidator.validateSchemaWithoutCounting(
          message = message.record,
          airbyteStream = AIRBYTE_STREAM_NAME_NAMESPACE_PAIR,
          validationErrors = uncountedValidationErrors,
        )
      },
    )

    executorService.awaitTermination(3, TimeUnit.SECONDS)
    assertEquals(1, uncountedValidationErrors.size)
    assertEquals(2, uncountedValidationErrors[AIRBYTE_STREAM_NAME_NAMESPACE_PAIR]!!.size)
  }

  @Test
  @Throws(InterruptedException::class, IOException::class)
  fun testMigrationOfIdPropertyToEscapedVersion() {
    val jsonSchema = MoreResources.readResource("catalog-json-schema-with-id.json")
    val airbyteStream = AirbyteStream().withJsonSchema(Jsons.deserialize(jsonSchema))
    val executorService = Executors.newSingleThreadExecutor()
    val recordSchemaValidator =
      RecordSchemaValidator(
        jsonSchemaValidator = JsonSchemaValidator(),
        schemaValidationExecutorService = executorService,
        streamNamesToSchemas = mutableMapOf(AIRBYTE_STREAM_NAME_NAMESPACE_PAIR to airbyteStream.jsonSchema),
      )
    val messagesToValidate = mutableListOf(AirbyteMessageUtils.createRecordMessage(STREAM_NAME, "id", "5"))

    messagesToValidate.forEach(
      Consumer { message: AirbyteMessage ->
        recordSchemaValidator.validateSchemaWithoutCounting(
          message = message.record,
          airbyteStream = AIRBYTE_STREAM_NAME_NAMESPACE_PAIR,
          validationErrors = uncountedValidationErrors,
        )
      },
    )

    executorService.awaitTermination(3, TimeUnit.SECONDS)
    assertEquals(0, uncountedValidationErrors.size)
  }
}
