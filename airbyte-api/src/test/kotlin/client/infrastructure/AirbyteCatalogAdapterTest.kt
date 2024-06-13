/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.api.client.infrastructure

import com.squareup.moshi.adapter
import io.airbyte.api.client.model.generated.SourceDiscoverSchemaRead
import io.airbyte.commons.resources.MoreResources
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertDoesNotThrow
import org.openapitools.client.infrastructure.Serializer

internal class AirbyteCatalogAdapterTest {
  @Test
  @OptIn(ExperimentalStdlibApi::class)
  internal fun testSerdeAirbyteCatalog() {
    val json = MoreResources.readResource("json/responses/source_discover_schema_read_response.json")
    val adapter = Serializer.moshi.adapter<SourceDiscoverSchemaRead>()
    val result = assertDoesNotThrow { adapter.fromJson(json) }
    assertNotNull(result)
    assertEquals(
      json.trim(),
      adapter.indent("  ").toJson(result)
        // Moshi's formatting does not jibe with our code style formatting
        .replace(
          oldValue =
            "\"supportedSyncModes\": [\n" +
              "            \"full_refresh\",\n" +
              "            \"incremental\"\n" +
              "          ],",
          newValue = "\"supportedSyncModes\": [\"full_refresh\", \"incremental\"],",
        ),
    )
  }
}
