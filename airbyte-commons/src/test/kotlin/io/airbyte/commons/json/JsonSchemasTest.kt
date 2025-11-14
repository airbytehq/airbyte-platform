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
import io.mockk.mockk
import io.mockk.verifySequence
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import org.junit.jupiter.params.provider.ValueSource
import java.util.function.BiConsumer

internal class JsonSchemasTest {
  @Test
  fun testMutateTypeToArrayStandard() {
    val expectedWithoutType = Jsons.deserialize("{\"test\":\"abc\"}")
    val actualWithoutType = clone(expectedWithoutType)
    mutateTypeToArrayStandard(expectedWithoutType)
    Assertions.assertEquals(expectedWithoutType, actualWithoutType)

    val expectedWithArrayType = Jsons.deserialize("{\"test\":\"abc\", \"type\":[\"object\"]}")
    val actualWithArrayType = clone(expectedWithArrayType)
    mutateTypeToArrayStandard(actualWithArrayType)
    Assertions.assertEquals(expectedWithoutType, actualWithoutType)

    val expectedWithoutArrayType = Jsons.deserialize("{\"test\":\"abc\", \"type\":[\"object\"]}")
    val actualWithStringType = Jsons.deserialize("{\"test\":\"abc\", \"type\":\"object\"}")
    mutateTypeToArrayStandard(actualWithStringType)
    Assertions.assertEquals(expectedWithoutArrayType, actualWithStringType)
  }

  @Test
  fun testTraverse() {
    val jsonWithAllTypes = Jsons.deserialize(read("json_schemas/json_with_all_types.json"))
    val mock: BiConsumer<JsonNode, MutableList<JsonSchemas.FieldNameOrList>> = mockk(relaxed = true)

    JsonSchemas.traverseJsonSchema(jsonWithAllTypes, mock)

    verifySequence {
      mock.accept(jsonWithAllTypes, mutableListOf())
      mock.accept(
        jsonWithAllTypes.get(PROPERTIES).get(NAME),
        mutableListOf(
          fieldName(NAME),
        ),
      )
      mock.accept(
        jsonWithAllTypes
          .get(PROPERTIES)
          .get(NAME)
          .get(
            PROPERTIES,
          ).get("first"),
        mutableListOf(fieldName(NAME), fieldName("first")),
      )
      mock.accept(
        jsonWithAllTypes
          .get(PROPERTIES)
          .get(NAME)
          .get(
            PROPERTIES,
          ).get("last"),
        mutableListOf(fieldName(NAME), fieldName("last")),
      )
      mock.accept(
        jsonWithAllTypes.get(PROPERTIES).get(COMPANY),
        mutableListOf(
          fieldName(COMPANY),
        ),
      )
      mock.accept(
        jsonWithAllTypes.get(PROPERTIES).get(PETS),
        mutableListOf(
          fieldName(PETS),
        ),
      )
      mock.accept(
        jsonWithAllTypes.get(PROPERTIES).get(PETS).get(
          ITEMS,
        ),
        mutableListOf(fieldName(PETS), list()),
      )
      mock.accept(
        jsonWithAllTypes
          .get(PROPERTIES)
          .get(PETS)
          .get(
            ITEMS,
          ).get(PROPERTIES)
          .get("type"),
        mutableListOf(fieldName(PETS), list(), fieldName("type")),
      )
      mock.accept(
        jsonWithAllTypes
          .get(PROPERTIES)
          .get(PETS)
          .get(
            ITEMS,
          ).get(PROPERTIES)
          .get("number"),
        mutableListOf(fieldName(PETS), list(), fieldName("number")),
      )
    }
  }

  @ValueSource(
    strings = [
      "anyOf", "oneOf", "allOf",
    ],
  )
  @ParameterizedTest
  fun testTraverseComposite(compositeKeyword: String) {
    val jsonSchemaString =
      read("json_schemas/composite_json_schema.json")
        .replace("<composite-placeholder>".toRegex(), compositeKeyword)
    val jsonWithAllTypes = Jsons.deserialize(jsonSchemaString)
    val mock: BiConsumer<JsonNode, MutableList<JsonSchemas.FieldNameOrList>> = mockk(relaxed = true)

    JsonSchemas.traverseJsonSchema(jsonWithAllTypes, mock)

    verifySequence {
      mock.accept(jsonWithAllTypes, mutableListOf())
      mock.accept(jsonWithAllTypes.get(compositeKeyword).get(0), mutableListOf())
      mock.accept(jsonWithAllTypes.get(compositeKeyword).get(1), mutableListOf())
      mock.accept(
        jsonWithAllTypes
          .get(compositeKeyword)
          .get(1)
          .get(
            PROPERTIES,
          ).get("prop1"),
        mutableListOf(fieldName("prop1")),
      )
      mock.accept(jsonWithAllTypes.get(compositeKeyword).get(2), mutableListOf())
      mock.accept(
        jsonWithAllTypes.get(compositeKeyword).get(2).get(
          ITEMS,
        ),
        mutableListOf(list()),
      )
      mock.accept(jsonWithAllTypes.get(compositeKeyword).get(3), mutableListOf())
      mock.accept(
        jsonWithAllTypes
          .get(compositeKeyword)
          .get(3)
          .get(compositeKeyword)
          .get(0),
        mutableListOf(),
      )
      mock.accept(
        jsonWithAllTypes
          .get(compositeKeyword)
          .get(3)
          .get(compositeKeyword)
          .get(1),
        mutableListOf(),
      )
      mock.accept(
        jsonWithAllTypes.get(compositeKeyword).get(3).get(compositeKeyword).get(1).get(
          ITEMS,
        ),
        mutableListOf(list()),
      )
    }
  }

  @Test
  fun testTraverseMultiType() {
    val jsonWithAllTypes = Jsons.deserialize(read("json_schemas/json_with_array_type_fields.json"))
    val mock: BiConsumer<JsonNode, MutableList<JsonSchemas.FieldNameOrList>> = mockk(relaxed = true)

    JsonSchemas.traverseJsonSchema(jsonWithAllTypes, mock)

    verifySequence {
      mock.accept(jsonWithAllTypes, mutableListOf())
      mock.accept(
        jsonWithAllTypes.get(PROPERTIES).get(COMPANY),
        mutableListOf(
          fieldName(COMPANY),
        ),
      )
      mock.accept(
        jsonWithAllTypes.get(ITEMS),
        mutableListOf(
          list(),
        ),
      )
      mock.accept(
        jsonWithAllTypes.get(ITEMS).get(PROPERTIES).get(
          USER,
        ),
        mutableListOf(list(), fieldName(USER)),
      )
    }
  }

  @Test
  fun testTraverseMultiTypeComposite() {
    val compositeKeyword = "anyOf"
    val jsonWithAllTypes = Jsons.deserialize(read("json_schemas/json_with_array_type_fields_with_composites.json"))
    val mock: BiConsumer<JsonNode, MutableList<JsonSchemas.FieldNameOrList>> = mockk(relaxed = true)

    JsonSchemas.traverseJsonSchema(jsonWithAllTypes, mock)

    verifySequence {
      mock.accept(jsonWithAllTypes, mutableListOf())
      mock.accept(
        jsonWithAllTypes
          .get(compositeKeyword)
          .get(0),
        mutableListOf(),
      )
      mock.accept(
        jsonWithAllTypes
          .get(compositeKeyword)
          .get(0)
          .get(
            PROPERTIES,
          ).get(COMPANY),
        mutableListOf(fieldName(COMPANY)),
      )
      mock.accept(
        jsonWithAllTypes
          .get(compositeKeyword)
          .get(1),
        mutableListOf(),
      )
      mock.accept(
        jsonWithAllTypes
          .get(compositeKeyword)
          .get(1)
          .get(
            PROPERTIES,
          ).get("organization"),
        mutableListOf(fieldName("organization")),
      )
      mock.accept(
        jsonWithAllTypes.get(ITEMS),
        mutableListOf(
          list(),
        ),
      )
      mock.accept(
        jsonWithAllTypes.get(ITEMS).get(PROPERTIES).get(
          USER,
        ),
        mutableListOf(list(), fieldName("user")),
      )
    }
  }

  @Test
  fun testTraverseArrayTypeWithNoItemsDoNotThrowsException() {
    val jsonWithAllTypes = Jsons.deserialize(read("json_schemas/json_with_array_type_fields_no_items.json"))
    val mock: BiConsumer<JsonNode, MutableList<JsonSchemas.FieldNameOrList>> = mockk(relaxed = true)

    JsonSchemas.traverseJsonSchema(jsonWithAllTypes, mock)
  }

  @ParameterizedTest(name = "{2}")
  @MethodSource("allowsAdditionalPropertiesTestCases")
  fun testAllowsAdditionalProperties(
    schemaJson: String,
    expected: Boolean,
  ) {
    val schema = Jsons.deserialize(schemaJson)
    Assertions.assertEquals(expected, allowsAdditionalProperties(schema))
  }

  companion object {
    private const val NAME = "name"
    private const val PROPERTIES = "properties"
    private const val PETS = "pets"
    private const val COMPANY = "company"
    private const val ITEMS = "items"
    private const val USER = "user"

    @JvmStatic
    private fun allowsAdditionalPropertiesTestCases() =
      listOf<Arguments?>( // When additionalProperties is not specified, should default to true
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
        Arguments.of("{}", true, "empty schema"), // Test with a nested object that has additionalProperties: true (testing root level)
        Arguments.of(
          "{\"type\": \"object\", \"properties\": {\"nested\": {\"type\": \"object\", \"additionalProperties\": true}}}",
          true,
          "nested object with additionalProperties: true",
        ), // Test with a nested object that has additionalProperties: false (testing root level)
        Arguments.of(
          "{\"type\": \"object\", \"properties\": {\"nested\": {\"type\": \"object\", \"additionalProperties\": false}}}",
          true,
          "nested object with additionalProperties: false",
        ),
      )
  }
}
