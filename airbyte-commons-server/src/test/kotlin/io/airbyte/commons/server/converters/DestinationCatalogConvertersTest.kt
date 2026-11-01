/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.converters

import io.airbyte.commons.json.Jsons
import io.airbyte.config.DestinationCatalog
import io.airbyte.config.DestinationCatalogWithId
import io.airbyte.config.DestinationOperation
import io.airbyte.config.DestinationSyncMode
import io.airbyte.domain.models.DestinationCatalogId
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import java.util.UUID
import io.airbyte.api.server.generated.models.DestinationSyncMode as ApiDestinationSyncMode

class DestinationCatalogConvertersTest {
  private val catalogId = UUID.randomUUID()
  private val objectName = "test_table"
  private val matchingKeys = listOf(listOf("id"), listOf("email"))
  private val jsonSchema =
    Jsons.jsonNode(
      mapOf(
        "type" to "object",
        "properties" to
          mapOf(
            "id" to mapOf("type" to "string"),
            "email" to mapOf("type" to "string"),
          ),
      ),
    )

  @Nested
  inner class DestinationCatalogWithIdToApi {
    @Test
    fun `should convert DestinationCatalogWithId to DestinationDiscoverRead`() {
      val operation =
        DestinationOperation(
          objectName = objectName,
          syncMode = DestinationSyncMode.APPEND,
          jsonSchema = jsonSchema,
          matchingKeys = matchingKeys,
        )

      val catalog = DestinationCatalog(operations = listOf(operation))
      val catalogWithId =
        DestinationCatalogWithId(
          catalogId = DestinationCatalogId(catalogId),
          catalog = catalog,
        )

      val result = catalogWithId.toApi()

      assertEquals(catalogId, result.catalogId)
      assertEquals(1, result.catalog.operations.size)
      assertEquals(objectName, result.catalog.operations[0].objectName)
      assertEquals(ApiDestinationSyncMode.APPEND, result.catalog.operations[0].syncMode)
      assertEquals(jsonSchema, result.catalog.operations[0].schema)
      assertEquals(matchingKeys, result.catalog.operations[0].matchingKeys)
    }

    @Test
    fun `should convert DestinationCatalogWithId with empty operations`() {
      val catalog = DestinationCatalog(operations = emptyList())
      val catalogWithId =
        DestinationCatalogWithId(
          catalogId = DestinationCatalogId(catalogId),
          catalog = catalog,
        )

      val result = catalogWithId.toApi()

      assertEquals(catalogId, result.catalogId)
      assertEquals(emptyList<Any>(), result.catalog.operations)
    }
  }

  @Nested
  inner class DestinationCatalogModelToApi {
    @Test
    fun `should convert DestinationCatalogModel to ApiDestinationCatalog`() {
      val operation1 =
        DestinationOperation(
          objectName = "table1",
          syncMode = DestinationSyncMode.APPEND_DEDUP,
          jsonSchema = jsonSchema,
          matchingKeys = matchingKeys,
        )

      val operation2 =
        DestinationOperation(
          objectName = "table2",
          syncMode = DestinationSyncMode.OVERWRITE,
          jsonSchema = jsonSchema,
          matchingKeys = emptyList(),
        )

      val catalog = DestinationCatalog(operations = listOf(operation1, operation2))

      val result = catalog.toApi()

      assertEquals(2, result.operations.size)
      assertEquals("table1", result.operations[0].objectName)
      assertEquals(ApiDestinationSyncMode.APPEND_DEDUP, result.operations[0].syncMode)
      assertEquals("table2", result.operations[1].objectName)
      assertEquals(ApiDestinationSyncMode.OVERWRITE, result.operations[1].syncMode)
      assertEquals(emptyList<Any>(), result.operations[1].matchingKeys)
    }

    @Test
    fun `should convert DestinationCatalogModel with empty operations list`() {
      val catalog = DestinationCatalog(operations = emptyList())

      val result = catalog.toApi()

      assertEquals(emptyList<Any>(), result.operations)
    }
  }

  @Nested
  inner class DestinationOperationModelToApi {
    @Test
    fun `should convert DestinationOperationModel`() {
      val operation =
        DestinationOperation(
          objectName = objectName,
          syncMode = DestinationSyncMode.OVERWRITE,
          jsonSchema = jsonSchema,
          matchingKeys = matchingKeys,
        )

      val result = operation.toApi()

      assertEquals(objectName, result.objectName)
      assertEquals(ApiDestinationSyncMode.OVERWRITE, result.syncMode)
      assertEquals(jsonSchema, result.schema)
      assertEquals(matchingKeys, result.matchingKeys)
    }

    @Test
    fun `should convert DestinationOperationModel with empty matching keys`() {
      val operation =
        DestinationOperation(
          objectName = objectName,
          syncMode = DestinationSyncMode.APPEND,
          jsonSchema = jsonSchema,
          matchingKeys = emptyList(),
        )

      val result = operation.toApi()

      assertEquals(objectName, result.objectName)
      assertEquals(ApiDestinationSyncMode.APPEND, result.syncMode)
      assertEquals(jsonSchema, result.schema)
      assertEquals(emptyList<Any>(), result.matchingKeys)
    }

    @Test
    fun `should convert DestinationOperationModel with complex matching keys`() {
      val complexMatchingKeys =
        listOf(
          listOf("id", "timestamp"),
          listOf("email"),
          listOf("user_id", "event_type", "created_at"),
        )

      val operation =
        DestinationOperation(
          objectName = "complex_table",
          syncMode = DestinationSyncMode.APPEND_DEDUP,
          jsonSchema = jsonSchema,
          matchingKeys = complexMatchingKeys,
        )

      val result = operation.toApi()

      assertEquals("complex_table", result.objectName)
      assertEquals(ApiDestinationSyncMode.APPEND_DEDUP, result.syncMode)
      assertEquals(jsonSchema, result.schema)
      assertEquals(complexMatchingKeys, result.matchingKeys)
    }
  }
}
