/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.api.client.infrastructure

import com.squareup.moshi.adapter
import io.airbyte.api.client.model.generated.DestinationDefinitionReadList
import io.airbyte.api.client.model.generated.SourceDefinitionReadList
import io.airbyte.commons.resources.MoreResources
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertDoesNotThrow
import org.openapitools.client.infrastructure.Serializer
import java.time.LocalDate

internal class AirbyteLocalDateAdapterTest {
  private lateinit var adapter: AirbyteLocalDateAdapter

  @BeforeEach
  internal fun setup() {
    adapter = AirbyteLocalDateAdapter()
  }

  @Test
  internal fun toJson() {
    assertEquals("2024-01-01", adapter.toJson(LocalDate.parse("2024-01-01")))
  }

  @Test
  internal fun fromJson() {
    val localDate = LocalDate.parse("2024-01-01")
    assertEquals(localDate, adapter.fromJson(listOf(2024, 1, 1)))
  }

  @Test
  @OptIn(ExperimentalStdlibApi::class)
  internal fun testSerdeGeneratedOfLocalDateAsArray() {
    val json = MoreResources.readResource("json/responses/destination_definition_read_list_response.json")
    val adapter = Serializer.moshi.adapter<DestinationDefinitionReadList>()
    val result = assertDoesNotThrow { adapter.fromJson(json) }
    assertNotNull(result)
  }

  @Test
  @OptIn(ExperimentalStdlibApi::class)
  internal fun testSerdeGeneratedOfLocalDateAsString() {
    val json = MoreResources.readResource("json/responses/source_definition_read_list_response.json")
    val adapter = Serializer.moshi.adapter<SourceDefinitionReadList>()
    val result = assertDoesNotThrow { adapter.fromJson(json) }
    assertNotNull(result)
  }
}
