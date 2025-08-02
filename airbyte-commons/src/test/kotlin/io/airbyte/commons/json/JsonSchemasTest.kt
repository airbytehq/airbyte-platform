/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.json

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.commons.json.JsonSchemas.FieldNameOrList.Companion.fieldName
import io.airbyte.commons.json.JsonSchemas.FieldNameOrList.Companion.list
import io.airbyte.commons.json.JsonSchemas.allowsAdditionalProperties
import io.airbyte.commons.json.JsonSchemas.mutateTypeToArrayStandard
import io.airbyte.commons.json.Jsons.clone
import io.airbyte.commons.resources.Resources.read
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import org.junit.jupiter.params.provider.ValueSource
import org.mockito.Mockito
import java.io.IOException
import java.util.function.BiConsumer
import java.util.stream.Stream

internal class JsonSchemasTest {
  @Test
  fun testMutateTypeToArrayStandard() {
    val expectedWithoutType = Jsons.deserialize("{\"test\":\"abc\"}")
    val actualWithoutType = clone<JsonNode>(expectedWithoutType)
    mutateTypeToArrayStandard(expectedWithoutType)
    Assertions.assertEquals(expectedWithoutType, actualWithoutType)

    val expectedWithArrayType = Jsons.deserialize("{\"test\":\"abc\", \"type\":[\"object\"]}")
    val actualWithArrayType = clone<JsonNode>(expectedWithArrayType)
    mutateTypeToArrayStandard(actualWithArrayType)
    Assertions.assertEquals(expectedWithoutType, actualWithoutType)

    val expectedWithoutArrayType = Jsons.deserialize("{\"test\":\"abc\", \"type\":[\"object\"]}")
    val actualWithStringType = Jsons.deserialize("{\"test\":\"abc\", \"type\":\"object\"}")
    mutateTypeToArrayStandard(actualWithStringType)
    Assertions.assertEquals(expectedWithoutArrayType, actualWithStringType)
  }

  @Test
  @Throws(IOException::class)
  fun testTraverse() {
    val jsonWithAllTypes = Jsons.deserialize(read("json_schemas/json_with_all_types.json"))
    val mock: BiConsumer<JsonNode, MutableList<JsonSchemas.FieldNameOrList>> =
      Mockito.mock(
        BiConsumer::class.java,
      ) as BiConsumer<JsonNode, MutableList<JsonSchemas.FieldNameOrList>>

    JsonSchemas.traverseJsonSchema(jsonWithAllTypes, mock)
    val inOrder = Mockito.inOrder(mock)
    inOrder
      .verify(mock)
      .accept(jsonWithAllTypes, mutableListOf<JsonSchemas.FieldNameOrList>())
    inOrder.verify(mock).accept(
      jsonWithAllTypes.get(PROPERTIES).get(NAME),
      mutableListOf<JsonSchemas.FieldNameOrList>(
        fieldName(NAME),
      ),
    )
    inOrder.verify(mock).accept(
      jsonWithAllTypes
        .get(PROPERTIES)
        .get(NAME)
        .get(
          PROPERTIES,
        ).get("first"),
      mutableListOf<JsonSchemas.FieldNameOrList>(fieldName(NAME), fieldName("first")),
    )
    inOrder.verify(mock).accept(
      jsonWithAllTypes
        .get(PROPERTIES)
        .get(NAME)
        .get(
          PROPERTIES,
        ).get("last"),
      mutableListOf<JsonSchemas.FieldNameOrList>(fieldName(NAME), fieldName("last")),
    )
    inOrder.verify(mock).accept(
      jsonWithAllTypes.get(PROPERTIES).get(COMPANY),
      mutableListOf<JsonSchemas.FieldNameOrList>(
        fieldName(COMPANY),
      ),
    )
    inOrder.verify(mock).accept(
      jsonWithAllTypes.get(PROPERTIES).get(PETS),
      mutableListOf<JsonSchemas.FieldNameOrList>(
        fieldName(PETS),
      ),
    )
    inOrder.verify(mock).accept(
      jsonWithAllTypes.get(PROPERTIES).get(PETS).get(
        ITEMS,
      ),
      mutableListOf<JsonSchemas.FieldNameOrList>(fieldName(PETS), list()),
    )
    inOrder.verify(mock).accept(
      jsonWithAllTypes
        .get(PROPERTIES)
        .get(PETS)
        .get(
          ITEMS,
        ).get(PROPERTIES)
        .get("type"),
      mutableListOf<JsonSchemas.FieldNameOrList>(fieldName(PETS), list(), fieldName("type")),
    )
    inOrder.verify(mock).accept(
      jsonWithAllTypes
        .get(PROPERTIES)
        .get(PETS)
        .get(
          ITEMS,
        ).get(PROPERTIES)
        .get("number"),
      mutableListOf<JsonSchemas.FieldNameOrList>(fieldName(PETS), list(), fieldName("number")),
    )
    inOrder.verifyNoMoreInteractions()
  }

  @ValueSource(
    strings = [
      "anyOf", "oneOf", "allOf",
    ],
  )
  @ParameterizedTest
  @Throws(IOException::class)
  fun testTraverseComposite(compositeKeyword: String) {
    val jsonSchemaString =
      read("json_schemas/composite_json_schema.json")
        .replace("<composite-placeholder>".toRegex(), compositeKeyword)
    val jsonWithAllTypes = Jsons.deserialize(jsonSchemaString)
    val mock: BiConsumer<JsonNode, MutableList<JsonSchemas.FieldNameOrList>> =
      Mockito.mock(
        BiConsumer::class.java,
      ) as BiConsumer<JsonNode, MutableList<JsonSchemas.FieldNameOrList>>

    JsonSchemas.traverseJsonSchema(jsonWithAllTypes, mock)

    val inOrder = Mockito.inOrder(mock)
    inOrder
      .verify(mock)
      .accept(jsonWithAllTypes, mutableListOf<JsonSchemas.FieldNameOrList>())
    inOrder
      .verify(mock)
      .accept(jsonWithAllTypes.get(compositeKeyword).get(0), mutableListOf<JsonSchemas.FieldNameOrList>())
    inOrder
      .verify(mock)
      .accept(jsonWithAllTypes.get(compositeKeyword).get(1), mutableListOf<JsonSchemas.FieldNameOrList>())
    inOrder.verify(mock).accept(
      jsonWithAllTypes
        .get(compositeKeyword)
        .get(1)
        .get(
          PROPERTIES,
        ).get("prop1"),
      mutableListOf<JsonSchemas.FieldNameOrList>(fieldName("prop1")),
    )
    inOrder
      .verify(mock)
      .accept(jsonWithAllTypes.get(compositeKeyword).get(2), mutableListOf<JsonSchemas.FieldNameOrList>())
    inOrder.verify(mock).accept(
      jsonWithAllTypes.get(compositeKeyword).get(2).get(
        ITEMS,
      ),
      mutableListOf<JsonSchemas.FieldNameOrList>(list()),
    )
    inOrder
      .verify(mock)
      .accept(
        jsonWithAllTypes
          .get(compositeKeyword)
          .get(3)
          .get(compositeKeyword)
          .get(0),
        mutableListOf<JsonSchemas.FieldNameOrList>(),
      )
    inOrder
      .verify(mock)
      .accept(
        jsonWithAllTypes
          .get(compositeKeyword)
          .get(3)
          .get(compositeKeyword)
          .get(1),
        mutableListOf<JsonSchemas.FieldNameOrList>(),
      )
    inOrder.verify(mock).accept(
      jsonWithAllTypes.get(compositeKeyword).get(3).get(compositeKeyword).get(1).get(
        ITEMS,
      ),
      mutableListOf<JsonSchemas.FieldNameOrList>(list()),
    )
    inOrder.verifyNoMoreInteractions()
  }

  @Test
  @Throws(IOException::class)
  fun testTraverseMultiType() {
    val jsonWithAllTypes = Jsons.deserialize(read("json_schemas/json_with_array_type_fields.json"))
    val mock: BiConsumer<JsonNode, MutableList<JsonSchemas.FieldNameOrList>> =
      Mockito.mock(
        BiConsumer::class.java,
      ) as BiConsumer<JsonNode, MutableList<JsonSchemas.FieldNameOrList>>

    JsonSchemas.traverseJsonSchema(jsonWithAllTypes, mock)
    val inOrder = Mockito.inOrder(mock)
    inOrder
      .verify(mock)
      .accept(jsonWithAllTypes, mutableListOf<JsonSchemas.FieldNameOrList>())
    inOrder.verify(mock).accept(
      jsonWithAllTypes.get(PROPERTIES).get(COMPANY),
      mutableListOf<JsonSchemas.FieldNameOrList>(
        fieldName(COMPANY),
      ),
    )
    inOrder.verify(mock).accept(
      jsonWithAllTypes.get(ITEMS),
      mutableListOf<JsonSchemas.FieldNameOrList>(
        list(),
      ),
    )
    inOrder.verify(mock).accept(
      jsonWithAllTypes.get(ITEMS).get(PROPERTIES).get(
        USER,
      ),
      mutableListOf<JsonSchemas.FieldNameOrList>(list(), fieldName(USER)),
    )
    inOrder.verifyNoMoreInteractions()
  }

  @Test
  @Throws(IOException::class)
  fun testTraverseMultiTypeComposite() {
    val compositeKeyword = "anyOf"
    val jsonWithAllTypes = Jsons.deserialize(read("json_schemas/json_with_array_type_fields_with_composites.json"))
    val mock: BiConsumer<JsonNode, MutableList<JsonSchemas.FieldNameOrList>> =
      Mockito.mock(
        BiConsumer::class.java,
      ) as BiConsumer<JsonNode, MutableList<JsonSchemas.FieldNameOrList>>

    JsonSchemas.traverseJsonSchema(jsonWithAllTypes, mock)

    val inOrder = Mockito.inOrder(mock)
    inOrder
      .verify(mock)
      .accept(jsonWithAllTypes, mutableListOf<JsonSchemas.FieldNameOrList>())
    inOrder.verify(mock).accept(
      jsonWithAllTypes
        .get(compositeKeyword)
        .get(0)
        .get(
          PROPERTIES,
        ).get(COMPANY),
      mutableListOf<JsonSchemas.FieldNameOrList>(fieldName(COMPANY)),
    )
    inOrder.verify(mock).accept(
      jsonWithAllTypes
        .get(compositeKeyword)
        .get(1)
        .get(
          PROPERTIES,
        ).get("organization"),
      mutableListOf<JsonSchemas.FieldNameOrList>(fieldName("organization")),
    )
    inOrder.verify(mock).accept(
      jsonWithAllTypes.get(ITEMS),
      mutableListOf<JsonSchemas.FieldNameOrList>(
        list(),
      ),
    )
    inOrder.verify(mock).accept(
      jsonWithAllTypes.get(ITEMS).get(PROPERTIES).get(
        USER,
      ),
      mutableListOf<JsonSchemas.FieldNameOrList>(list(), fieldName("user")),
    )
    inOrder.verifyNoMoreInteractions()
  }

  @Test
  @Throws(IOException::class)
  fun testTraverseArrayTypeWithNoItemsDoNotThrowsException() {
    val jsonWithAllTypes = Jsons.deserialize(read("json_schemas/json_with_array_type_fields_no_items.json"))
    val mock: BiConsumer<JsonNode, MutableList<JsonSchemas.FieldNameOrList>> =
      Mockito.mock(
        BiConsumer::class.java,
      ) as BiConsumer<JsonNode, MutableList<JsonSchemas.FieldNameOrList>>

    JsonSchemas.traverseJsonSchema(jsonWithAllTypes, mock)
  }

  @ParameterizedTest(name = "{2}")
  @MethodSource("allowsAdditionalPropertiesTestCases")
  fun testAllowsAdditionalProperties(
    schemaJson: String,
    expected: Boolean,
    description: String?,
  ) {
    val schema = Jsons.deserialize(schemaJson)
    Assertions.assertEquals(expected, allowsAdditionalProperties(schema))
  }

  companion object {
    private const val UNCHECKED = "unchecked"
    private const val NAME = "name"
    private const val PROPERTIES = "properties"
    private const val PETS = "pets"
    private const val COMPANY = "company"
    private const val ITEMS = "items"
    private const val USER = "user"

    @JvmStatic
    private fun allowsAdditionalPropertiesTestCases(): Stream<Arguments?> =
      Stream.of<Arguments?>( // When additionalProperties is not specified, should default to true
        Arguments.of(
          "{\"type\": \"object\", \"properties\": {\"name\": {\"type\": \"string\"}}}",
          true,
          "missing additionalProperties",
        ), // When additionalProperties is explicitly true
        Arguments.of(
          "{\"type\": \"object\", \"properties\": {\"name\": {\"type\": \"string\"}}, \"additionalProperties\": true}",
          true,
          "additionalProperties: true",
        ), // When additionalProperties is explicitly false
        Arguments.of(
          "{\"type\": \"object\", \"properties\": {\"name\": {\"type\": \"string\"}}, \"additionalProperties\": false}",
          false,
          "additionalProperties: false",
        ), // Test with minimal schema
        Arguments.of("{}", true, "empty schema"), // Test with nested object that has additionalProperties: true (testing root level)
        Arguments.of(
          "{\"type\": \"object\", \"properties\": {\"nested\": {\"type\": \"object\", \"additionalProperties\": true}}}",
          true,
          "nested object with additionalProperties: true",
        ), // Test with nested object that has additionalProperties: false (testing root level)
        Arguments.of(
          "{\"type\": \"object\", \"properties\": {\"nested\": {\"type\": \"object\", \"additionalProperties\": false}}}",
          true,
          "nested object with additionalProperties: false",
        ),
      )
  }
}
