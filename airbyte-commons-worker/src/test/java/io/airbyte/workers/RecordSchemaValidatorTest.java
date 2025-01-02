/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers;

import static org.junit.Assert.assertEquals;

import io.airbyte.commons.resources.MoreResources;
import io.airbyte.config.StandardSync;
import io.airbyte.persistence.job.models.ReplicationInput;
import io.airbyte.protocol.models.AirbyteMessage;
import io.airbyte.protocol.models.AirbyteStream;
import io.airbyte.protocol.models.AirbyteStreamNameNamespacePair;
import io.airbyte.protocol.models.Jsons;
import io.airbyte.workers.test_utils.AirbyteMessageUtils;
import io.airbyte.workers.test_utils.TestConfigHelpers;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@SuppressWarnings("PMD.JUnitTestsShouldIncludeAssert")
class RecordSchemaValidatorTest {

  private ReplicationInput replicationInput;
  private static final AirbyteStreamNameNamespacePair AIRBYTE_STREAM_NAME_NAMESPACE_PAIR = new AirbyteStreamNameNamespacePair("user_preferences", "");
  private static final String STREAM_NAME = AIRBYTE_STREAM_NAME_NAMESPACE_PAIR.getName();
  private static final String FIELD_NAME = "favorite_color";
  private static final AirbyteMessage VALID_RECORD = AirbyteMessageUtils.createRecordMessage(STREAM_NAME, FIELD_NAME, "blue");
  private static final AirbyteMessage INVALID_RECORD_1 = AirbyteMessageUtils.createRecordMessage(STREAM_NAME, FIELD_NAME, 3);
  private static final AirbyteMessage INVALID_RECORD_2 = AirbyteMessageUtils.createRecordMessage(STREAM_NAME, Map.of(FIELD_NAME, true));

  private ConcurrentMap<AirbyteStreamNameNamespacePair, ImmutablePair<Set<String>, Integer>> validationErrors;
  private ConcurrentMap<AirbyteStreamNameNamespacePair, Set<String>> uncountedValidationErrors;

  @BeforeEach
  void setup() {
    final ImmutablePair<StandardSync, ReplicationInput> syncPair = TestConfigHelpers.createReplicationConfig();
    replicationInput = syncPair.getValue();
    validationErrors = new ConcurrentHashMap<>();
    uncountedValidationErrors = new ConcurrentHashMap<>();
  }

  @Test
  void testValidateValidSchema() {
    final var recordSchemaValidator = new RecordSchemaValidator(WorkerUtils.mapStreamNamesToSchemas(replicationInput.getCatalog()));
    recordSchemaValidator.validateSchema(VALID_RECORD.getRecord(), AirbyteStreamNameNamespacePair.fromRecordMessage(VALID_RECORD.getRecord()),
        validationErrors);

    assertEquals(0, validationErrors.size());
  }

  @Test
  void testValidateValidSchemaWithoutCounting() {
    final var recordSchemaValidator = new RecordSchemaValidator(WorkerUtils.mapStreamNamesToSchemas(replicationInput.getCatalog()));
    recordSchemaValidator.validateSchemaWithoutCounting(VALID_RECORD.getRecord(),
        AirbyteStreamNameNamespacePair.fromRecordMessage(VALID_RECORD.getRecord()),
        uncountedValidationErrors);

    assertEquals(0, uncountedValidationErrors.size());
  }

  @Test
  void testValidateInvalidSchema() throws InterruptedException {
    final var executorService = Executors.newFixedThreadPool(1);
    final var recordSchemaValidator = new RecordSchemaValidator(WorkerUtils.mapStreamNamesToSchemas(replicationInput.getCatalog()), executorService);
    final List<AirbyteMessage> messagesToValidate = new ArrayList<>(Arrays.asList(INVALID_RECORD_1, INVALID_RECORD_2, VALID_RECORD));

    messagesToValidate.forEach(message -> recordSchemaValidator.validateSchema(
        message.getRecord(),
        AIRBYTE_STREAM_NAME_NAMESPACE_PAIR,
        validationErrors));
    executorService.awaitTermination(3, TimeUnit.SECONDS);
    assertEquals(1, validationErrors.size());
    assertEquals(2, (int) validationErrors.get(AIRBYTE_STREAM_NAME_NAMESPACE_PAIR).getRight());
  }

  @Test
  void testValidateInvalidSchemaWithoutCounting() throws InterruptedException {
    final var executorService = Executors.newFixedThreadPool(1);
    final var recordSchemaValidator = new RecordSchemaValidator(WorkerUtils.mapStreamNamesToSchemas(replicationInput.getCatalog()), executorService);
    final List<AirbyteMessage> messagesToValidate = new ArrayList<>(Arrays.asList(INVALID_RECORD_1, INVALID_RECORD_2, VALID_RECORD));

    messagesToValidate.forEach(message -> recordSchemaValidator.validateSchemaWithoutCounting(
        message.getRecord(),
        AIRBYTE_STREAM_NAME_NAMESPACE_PAIR,
        uncountedValidationErrors));

    executorService.awaitTermination(3, TimeUnit.SECONDS);
    assertEquals(1, uncountedValidationErrors.size());
    assertEquals(2, uncountedValidationErrors.get(AIRBYTE_STREAM_NAME_NAMESPACE_PAIR).size());
  }

  @Test
  void testMigrationOfIdPropertyToEscapedVersion() throws InterruptedException, IOException {
    final String jsonSchema = MoreResources.readResource("catalog-json-schema-with-id.json");
    final AirbyteStream airbyteStream = new AirbyteStream().withJsonSchema(Jsons.deserialize(jsonSchema));
    final var executorService = Executors.newFixedThreadPool(1);
    final var recordSchemaValidator =
        new RecordSchemaValidator(Map.of(AIRBYTE_STREAM_NAME_NAMESPACE_PAIR, airbyteStream.getJsonSchema()), executorService);
    final List<AirbyteMessage> messagesToValidate = List.of(AirbyteMessageUtils.createRecordMessage(STREAM_NAME, "id", "5"));

    messagesToValidate.forEach(message -> recordSchemaValidator.validateSchemaWithoutCounting(
        message.getRecord(),
        AIRBYTE_STREAM_NAME_NAMESPACE_PAIR,
        uncountedValidationErrors));

    executorService.awaitTermination(3, TimeUnit.SECONDS);
    assertEquals(0, uncountedValidationErrors.size());
  }

}
