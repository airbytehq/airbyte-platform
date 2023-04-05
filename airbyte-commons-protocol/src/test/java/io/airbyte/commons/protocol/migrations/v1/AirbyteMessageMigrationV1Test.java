/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.protocol.migrations.v1;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.databind.JsonNode;
import io.airbyte.commons.json.Jsons;
import io.airbyte.protocol.models.AirbyteCatalog;
import io.airbyte.protocol.models.AirbyteMessage;
import io.airbyte.protocol.models.AirbyteMessage.Type;
import io.airbyte.protocol.models.AirbyteRecordMessage;
import io.airbyte.protocol.models.AirbyteStream;
import io.airbyte.protocol.models.ConfiguredAirbyteCatalog;
import io.airbyte.protocol.models.ConfiguredAirbyteStream;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class AirbyteMessageMigrationV1Test {

  private final AirbyteMessageMigrationV1 migration = new AirbyteMessageMigrationV1();

  @Test
  void testVersionMetadata() {
    assertEquals("0.3.0", migration.getPreviousVersion().serialize());
    assertEquals("1.0.0", migration.getCurrentVersion().serialize());
  }

  @Nested
  class CatalogUpgradeTest {

    @Test
    void testBasicUpgrade() {
      // This isn't actually a valid stream schema (since it's not an object)
      // but this test case is mostly about preserving the message structure, so it's not super relevant
      final JsonNode oldSchema = Jsons.deserialize(
          """
          {
            "type": "string"
          }
          """);

      final AirbyteMessage upgradedMessage = migration.upgrade(createCatalogMessage(oldSchema), Optional.empty());

      final AirbyteMessage expectedMessage = Jsons.deserialize(
          """
          {
            "type": "CATALOG",
            "catalog": {
              "streams": [
                {
                  "json_schema": {
                    "type": "string"
                  }
                }
              ]
            }
          }
          """,
          AirbyteMessage.class);
      assertEquals(expectedMessage, upgradedMessage);
    }

    @Test
    void testNullUpgrade() {
      final io.airbyte.protocol.models.v0.AirbyteMessage oldMessage = new io.airbyte.protocol.models.v0.AirbyteMessage()
          .withType(io.airbyte.protocol.models.v0.AirbyteMessage.Type.CATALOG);
      final AirbyteMessage upgradedMessage = migration.upgrade(oldMessage, Optional.empty());
      final AirbyteMessage expectedMessage = new AirbyteMessage().withType(Type.CATALOG);
      assertEquals(expectedMessage, upgradedMessage);
    }

    private io.airbyte.protocol.models.v0.AirbyteMessage createCatalogMessage(final JsonNode schema) {
      return new io.airbyte.protocol.models.v0.AirbyteMessage().withType(io.airbyte.protocol.models.v0.AirbyteMessage.Type.CATALOG)
          .withCatalog(
              new io.airbyte.protocol.models.v0.AirbyteCatalog().withStreams(List.of(new io.airbyte.protocol.models.v0.AirbyteStream().withJsonSchema(
                  schema))));
    }

  }

  @Nested
  class RecordUpgradeTest {

    @Test
    void testBasicUpgrade() {
      final JsonNode oldData = Jsons.deserialize(
          """
          {
            "id": 42
          }
          """);

      final AirbyteMessage upgradedMessage = migration.upgrade(createRecordMessage(oldData), Optional.empty());

      final AirbyteMessage expectedMessage = Jsons.deserialize(
          """
          {
            "type": "RECORD",
            "record": {
              "data": {
                "id": 42
              }
            }
          }
          """,
          AirbyteMessage.class);
      assertEquals(expectedMessage, upgradedMessage);
    }

    @Test
    void testNullUpgrade() {
      final io.airbyte.protocol.models.v0.AirbyteMessage oldMessage = new io.airbyte.protocol.models.v0.AirbyteMessage()
          .withType(io.airbyte.protocol.models.v0.AirbyteMessage.Type.RECORD);
      final AirbyteMessage upgradedMessage = migration.upgrade(oldMessage, Optional.empty());
      final AirbyteMessage expectedMessage = new AirbyteMessage().withType(Type.RECORD);
      assertEquals(expectedMessage, upgradedMessage);
    }

    private io.airbyte.protocol.models.v0.AirbyteMessage createRecordMessage(final JsonNode data) {
      return new io.airbyte.protocol.models.v0.AirbyteMessage().withType(io.airbyte.protocol.models.v0.AirbyteMessage.Type.RECORD)
          .withRecord(new io.airbyte.protocol.models.v0.AirbyteRecordMessage().withData(data));
    }

  }

  @Nested
  class CatalogDowngradeTest {

    @Test
    void testBasicDowngrade() {
      // This isn't actually a valid stream schema (since it's not an object)
      // but this test case is mostly about preserving the message structure, so it's not super relevant
      final JsonNode newSchema = Jsons.deserialize(
          """
          {
            "type": "string"
          }
          """);

      final io.airbyte.protocol.models.v0.AirbyteMessage downgradedMessage = migration.downgrade(createCatalogMessage(newSchema), Optional.empty());

      final io.airbyte.protocol.models.v0.AirbyteMessage expectedMessage = Jsons.deserialize(
          """
          {
            "type": "CATALOG",
            "catalog": {
              "streams": [
                {
                  "json_schema": {
                    "type": "string"
                  }
                }
              ]
            }
          }
          """,
          io.airbyte.protocol.models.v0.AirbyteMessage.class);
      assertEquals(expectedMessage, downgradedMessage);
    }

    @Test
    void testNullDowngrade() {
      final AirbyteMessage oldMessage = new AirbyteMessage().withType(Type.CATALOG);
      final io.airbyte.protocol.models.v0.AirbyteMessage upgradedMessage = migration.downgrade(oldMessage, Optional.empty());
      final io.airbyte.protocol.models.v0.AirbyteMessage expectedMessage = new io.airbyte.protocol.models.v0.AirbyteMessage()
          .withType(io.airbyte.protocol.models.v0.AirbyteMessage.Type.CATALOG);
      assertEquals(expectedMessage, upgradedMessage);
    }

    private AirbyteMessage createCatalogMessage(final JsonNode schema) {
      return new AirbyteMessage().withType(AirbyteMessage.Type.CATALOG)
          .withCatalog(
              new AirbyteCatalog().withStreams(List.of(new AirbyteStream().withJsonSchema(
                  schema))));
    }

  }

  @Nested
  class RecordDowngradeTest {

    private static final String STREAM_NAME = "foo_stream";
    private static final String NAMESPACE_NAME = "foo_namespace";

    @Test
    void testBasicDowngrade() {
      final ConfiguredAirbyteCatalog catalog = createConfiguredAirbyteCatalog(
          """
          {"type": "string"}
          """);
      final JsonNode oldData = Jsons.deserialize(
          """
          42
          """);

      final io.airbyte.protocol.models.v0.AirbyteMessage downgradedMessage = new AirbyteMessageMigrationV1()
          .downgrade(createRecordMessage(oldData), Optional.of(catalog));

      final io.airbyte.protocol.models.v0.AirbyteMessage expectedMessage = Jsons.deserialize(
          """
          {
            "type": "RECORD",
            "record": {
              "stream": "foo_stream",
              "namespace": "foo_namespace",
              "data": 42
            }
          }
          """,
          io.airbyte.protocol.models.v0.AirbyteMessage.class);
      assertEquals(expectedMessage, downgradedMessage);
    }

    @Test
    void testNullDowngrade() {
      final AirbyteMessage oldMessage = new AirbyteMessage().withType(Type.RECORD);
      final io.airbyte.protocol.models.v0.AirbyteMessage upgradedMessage = migration.downgrade(oldMessage, Optional.empty());
      final io.airbyte.protocol.models.v0.AirbyteMessage expectedMessage = new io.airbyte.protocol.models.v0.AirbyteMessage()
          .withType(io.airbyte.protocol.models.v0.AirbyteMessage.Type.RECORD);
      assertEquals(expectedMessage, upgradedMessage);
    }

    private ConfiguredAirbyteCatalog createConfiguredAirbyteCatalog(final String schema) {
      return new ConfiguredAirbyteCatalog()
          .withStreams(List.of(new ConfiguredAirbyteStream().withStream(new io.airbyte.protocol.models.AirbyteStream()
              .withName(STREAM_NAME)
              .withNamespace(NAMESPACE_NAME)
              .withJsonSchema(Jsons.deserialize(schema)))));
    }

    private AirbyteMessage createRecordMessage(final JsonNode data) {
      return new AirbyteMessage().withType(AirbyteMessage.Type.RECORD)
          .withRecord(new AirbyteRecordMessage().withStream(STREAM_NAME).withNamespace(NAMESPACE_NAME).withData(data));
    }

  }

}
