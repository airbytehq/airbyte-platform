/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.converters

import io.airbyte.commons.json.Jsons
import io.airbyte.config.DestinationCatalog
import io.airbyte.config.DestinationCatalogWithId
import io.airbyte.config.DestinationOperation
import io.airbyte.config.DestinationSyncMode
import io.airbyte.domain.models.DestinationCatalogId
import io.kotest.matchers.shouldBe
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import java.util.UUID
import io.airbyte.api.model.generated.DestinationSyncMode as ApiDestinationSyncMode

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

      result.catalogId shouldBe catalogId
      result.catalog.operations.size shouldBe 1
      result.catalog.operations[0].objectName shouldBe objectName
      result.catalog.operations[0].syncMode shouldBe ApiDestinationSyncMode.APPEND
      result.catalog.operations[0].schema shouldBe jsonSchema
      result.catalog.operations[0].matchingKeys shouldBe matchingKeys
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

      result.catalogId shouldBe catalogId
      result.catalog.operations shouldBe emptyList()
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

      result.operations.size shouldBe 2
      result.operations[0].objectName shouldBe "table1"
      result.operations[0].syncMode shouldBe ApiDestinationSyncMode.APPEND_DEDUP
      result.operations[1].objectName shouldBe "table2"
      result.operations[1].syncMode shouldBe ApiDestinationSyncMode.OVERWRITE
      result.operations[1].matchingKeys shouldBe emptyList()
    }

    @Test
    fun `should convert DestinationCatalogModel with empty operations list`() {
      val catalog = DestinationCatalog(operations = emptyList())

      val result = catalog.toApi()

      result.operations shouldBe emptyList()
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

      result.objectName shouldBe objectName
      result.syncMode shouldBe ApiDestinationSyncMode.OVERWRITE
      result.schema shouldBe jsonSchema
      result.matchingKeys shouldBe matchingKeys
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

      result.objectName shouldBe objectName
      result.syncMode shouldBe ApiDestinationSyncMode.APPEND
      result.schema shouldBe jsonSchema
      result.matchingKeys shouldBe emptyList()
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

      result.objectName shouldBe "complex_table"
      result.syncMode shouldBe ApiDestinationSyncMode.APPEND_DEDUP
      result.schema shouldBe jsonSchema
      result.matchingKeys shouldBe complexMatchingKeys
    }
  }
}
