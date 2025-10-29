/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.json

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.core.type.TypeReference
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.BinaryNode
import com.fasterxml.jackson.databind.node.JsonNodeFactory
import com.fasterxml.jackson.databind.node.TextNode
import io.airbyte.commons.json.Jsons.arrayNode
import io.airbyte.commons.json.Jsons.canonicalJsonSerialize
import io.airbyte.commons.json.Jsons.clone
import io.airbyte.commons.json.Jsons.deserializeIfText
import io.airbyte.commons.json.Jsons.deserializeToIntegerMap
import io.airbyte.commons.json.Jsons.deserializeToStringList
import io.airbyte.commons.json.Jsons.deserializeToStringMap
import io.airbyte.commons.json.Jsons.emptyObject
import io.airbyte.commons.json.Jsons.flatten
import io.airbyte.commons.json.Jsons.getEstimatedByteSize
import io.airbyte.commons.json.Jsons.getNodeOrEmptyObject
import io.airbyte.commons.json.Jsons.jsonNode
import io.airbyte.commons.json.Jsons.keys
import io.airbyte.commons.json.Jsons.mergeNodes
import io.airbyte.commons.json.Jsons.`object`
import io.airbyte.commons.json.Jsons.setNestedValue
import io.airbyte.commons.json.Jsons.toBytes
import io.airbyte.commons.json.Jsons.toPrettyString
import io.airbyte.commons.json.Jsons.tryDeserialize
import io.airbyte.commons.json.Jsons.tryObject
import io.airbyte.protocol.models.JsonSchemaType
import io.airbyte.protocol.models.v0.CatalogHelpers
import io.airbyte.protocol.models.v0.Field
import org.assertj.core.util.Maps
import org.junit.jupiter.api.Assertions.assertArrayEquals
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotSame
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test
import java.io.IOException
import java.nio.charset.StandardCharsets
import java.util.Objects
import java.util.Optional

internal class JsonsTest {
  @Test
  fun testSerialize() {
    assertEquals(
      SERIALIZED_JSON,
      Jsons.serialize<ToClass?>(ToClass(ABC, 999, 888L)),
    )

    assertEquals(
      "{\"test\":\"abc\",\"test2\":\"def\"}",
      Jsons.serialize(mapOf(TEST to ABC, TEST2 to DEF)),
    )
  }

  @Test
  fun testSerializeJsonNode() {
    assertEquals(
      SERIALIZED_JSON,
      Jsons.serialize<JsonNode?>(jsonNode<ToClass?>(ToClass(ABC, 999, 888L))),
    )

    assertEquals(
      "{\"test\":\"abc\",\"test2\":\"def\"}",
      Jsons.serialize<JsonNode?>(jsonNode(mapOf(TEST to ABC, TEST2 to DEF))),
    )
    // issue: 5878 add test for binary node serialization, binary data are
    // serialized into base64
    assertEquals(
      "{\"test\":\"dGVzdA==\"}",
      Jsons.serialize<JsonNode?>(
        jsonNode(mapOf(TEST to BinaryNode("test".toByteArray(StandardCharsets.UTF_8)))),
      ),
    )
  }

  @Test
  fun testDeserialize() {
    assertEquals(
      ToClass(ABC, 999, 888L),
      Jsons.deserialize("{\"str\":\"abc\", \"num\": 999, \"numLong\": 888}", ToClass::class.java),
    )
  }

  @Test
  fun testDeserializeToJsonNode() {
    assertEquals(
      SERIALIZED_JSON2,
      Jsons.deserialize(SERIALIZED_JSON2).toString(),
    )

    assertEquals(
      "[{\"str\":\"abc\"},{\"str\":\"abc\"}]",
      Jsons.deserialize("[{\"str\":\"abc\"},{\"str\":\"abc\"}]").toString(),
    )
    // issue: 5878 add test for binary node deserialization, for now should be
    // base64 string
    assertEquals(
      "{\"test\":\"dGVzdA==\"}",
      Jsons.deserialize("{\"test\":\"dGVzdA==\"}").toString(),
    )
  }

  @Test
  fun testTryDeserializeToJsonNode() {
    assertEquals(
      Optional.of<JsonNode?>(Jsons.deserialize(SERIALIZED_JSON2)),
      tryDeserialize(SERIALIZED_JSON2),
    )

    assertEquals(
      Optional.empty<Any?>(),
      tryDeserialize("{\"str\":\"abc\", \"num\": 999, \"test}"),
    )
  }

  @Test
  fun testToJsonNode() {
    assertEquals(
      SERIALIZED_JSON,
      jsonNode<ToClass?>(ToClass(ABC, 999, 888L)).toString(),
    )

    assertEquals(
      "{\"test\":\"abc\",\"test2\":\"def\"}",
      jsonNode(mapOf(TEST to ABC, TEST2 to DEF)).toString(),
    )

    assertEquals(
      "{\"test\":\"abc\",\"test2\":{\"inner\":1}}",
      jsonNode(mapOf(TEST to ABC, TEST2 to mapOf("inner" to 1))).toString(),
    )

    assertEquals(
      jsonNode<ToClass?>(ToClass(ABC, 999, 888L)),
      jsonNode<JsonNode?>(jsonNode<ToClass?>(ToClass(ABC, 999, 888L))),
    )
  }

  @Test
  fun testEmptyObject() {
    assertEquals(Jsons.deserialize("{}"), emptyObject())
  }

  @Test
  fun testArrayNode() {
    assertEquals(Jsons.deserialize("[]"), arrayNode())
  }

  @Test
  fun testToObject() {
    val expected = ToClass(ABC, 999, 888L)
    assertEquals(
      expected,
      `object`(jsonNode<ToClass?>(expected), ToClass::class.java),
    )

    assertEquals(
      listOf(expected),
      `object`(
        jsonNode(listOf(expected)),
        object : TypeReference<List<ToClass?>?>() {},
      ),
    )

    assertEquals(
      ToClass(),
      `object`(Jsons.deserialize("{\"a\":1}"), ToClass::class.java),
    )
  }

  @Test
  fun testTryToObject() {
    val expected = ToClass(ABC, 999, 888L)
    assertEquals(
      Optional.of<ToClass?>(expected),
      tryObject(Jsons.deserialize(SERIALIZED_JSON), ToClass::class.java),
    )

    assertEquals(
      Optional.of<ToClass?>(expected),
      tryObject(Jsons.deserialize(SERIALIZED_JSON), object : TypeReference<ToClass>() {}),
    )

    val emptyExpected = ToClass()
    assertEquals(
      Optional.of<ToClass?>(emptyExpected),
      tryObject(Jsons.deserialize("{\"str1\":\"abc\"}"), ToClass::class.java),
    )

    assertEquals(
      Optional.of<ToClass?>(emptyExpected),
      tryObject(Jsons.deserialize("{\"str1\":\"abc\"}"), object : TypeReference<ToClass>() {}),
    )
  }

  @Test
  fun testClone() {
    val expected = ToClass("abc", 999, 888L)
    val actual = clone<ToClass>(expected)
    assertNotSame(expected, actual)
    assertEquals(expected, actual)
  }

  @Test
  fun testToBytes() {
    val jsonString = "{\"test\":\"abc\",\"type\":[\"object\"]}"
    assertArrayEquals(jsonString.toByteArray(StandardCharsets.UTF_8), toBytes(Jsons.deserialize(jsonString)))
  }

  @Test
  fun testKeys() {
    // test object json node
    val jsonNode = jsonNode(mapOf(TEST to ABC, TEST2 to DEF))
    assertEquals(setOf(TEST, TEST2), keys(jsonNode))

    // test literal jsonNode
    assertEquals(mutableSetOf<Any?>(), keys(jsonNode.get("test")))

    // test nested object json node. should only return top-level keys.
    val nestedJsonNode = jsonNode(mapOf(TEST to ABC, TEST2 to mapOf("test3" to "def")))
    assertEquals(setOf<String?>(TEST, TEST2), keys(nestedJsonNode))

    // test array json node
    val arrayJsonNode = jsonNode(listOf(mapOf(TEST to ABC), mapOf(TEST2 to DEF)))
    assertEquals(mutableSetOf<Any?>(), keys(arrayJsonNode))
  }

  @Test
  fun testToPrettyString() {
    val jsonNode = jsonNode(mapOf(TEST to ABC))
    val expectedOutput = (
      "{\n" +
        "  \"test\" : \"abc\"\n" +
        "}\n"
    )
    assertEquals(expectedOutput, toPrettyString(jsonNode))
  }

  @Test
  fun testGetEstimatedByteSize() {
    val json = Jsons.deserialize("{\"string_key\":\"abc\",\"array_key\":[\"item1\", \"item2\"]}")
    assertEquals(toBytes(json).size, getEstimatedByteSize(json))
  }

  @Test
  fun testDeserializeToStringMap() {
    assertEquals(Maps.newHashMap<String?, String?>("a", "b"), deserializeToStringMap(Jsons.deserialize("{ \"a\": \"b\" }")))
    assertEquals(Maps.newHashMap<String?, Any?>("a", null), deserializeToStringMap(Jsons.deserialize("{ \"a\": null }")))
    assertEquals(mutableMapOf<Any?, Any?>(), deserializeToStringMap(emptyObject()))
  }

  @Test
  fun testDeserializeToIntegerMap() {
    assertEquals(Maps.newHashMap<String?, Int?>("a", 1), deserializeToIntegerMap(Jsons.deserialize("{ \"a\": 1 }")))
    assertEquals(mutableMapOf<Any?, Any?>(), deserializeToIntegerMap(emptyObject()))
  }

  @Test
  fun testDeserializeToStringList() {
    assertEquals(mutableListOf<String?>("a", "b"), deserializeToStringList(Jsons.deserialize("[ \"a\", \"b\" ]")))
    assertThrows(
      IllegalArgumentException::class.java,
      { deserializeToStringList(emptyObject()) },
    )
  }

  @Test
  fun testFlatten__noArrays() {
    val json = Jsons.deserialize("{ \"abc\": { \"def\": \"ghi\" }, \"jkl\": true, \"pqr\": 1 }")
    val expected = mapOf("abc.def" to GHI, JKL to true, PQR to 1)
    assertEquals(expected, flatten(json, false))
  }

  @Test
  fun testFlatten__withArraysNoApplyFlatten() {
    val json =
      Jsons
        .deserialize("{ \"abc\": [{ \"def\": \"ghi\" }, { \"fed\": \"ihg\" }], \"jkl\": true, \"pqr\": 1 }")
    val expected = mapOf(ABC to "[{\"def\":\"ghi\"},{\"fed\":\"ihg\"}]", JKL to true, PQR to 1)
    assertEquals(expected, flatten(json, false))
  }

  @Test
  fun testFlatten__checkBackwardCompatiblity() {
    val json =
      Jsons
        .deserialize("{ \"abc\": [{ \"def\": \"ghi\" }, { \"fed\": \"ihg\" }], \"jkl\": true, \"pqr\": 1 }")
    val expected = mapOf(ABC to "[{\"def\":\"ghi\"},{\"fed\":\"ihg\"}]", JKL to true, PQR to 1)
    assertEquals(expected, flatten(json))
  }

  @Test
  fun testFlatten__withArraysApplyFlatten() {
    val json =
      Jsons
        .deserialize("{ \"abc\": [{ \"def\": \"ghi\" }, { \"fed\": \"ihg\" }], \"jkl\": true, \"pqr\": 1 }")
    val expected = mapOf("abc.[0].def" to "ghi", "abc.[1].fed" to "ihg", JKL to true, PQR to 1)
    assertEquals(expected, flatten(json, true))
  }

  @Test
  fun testFlatten__withArraysApplyFlattenNested() {
    val json =
      Jsons
        .deserialize(
          "{ \"abc\": [{ \"def\": {\"ghi\": [\"xyz\"] }}, { \"fed\": \"ihg\" }], \"jkl\": true, \"pqr\": 1 }",
        )
    val expected = mapOf("abc.[0].def.ghi.[0]" to "xyz", "abc.[1].fed" to "ihg", JKL to true, PQR to 1)
    assertEquals(expected, flatten(json, true))
  }

  @Test
  fun testMergeNodes() {
    val mainNode =
      Jsons
        .deserialize("{ \"abc\": \"testing\", \"def\": \"asdf\"}")
    val updateNode =
      Jsons
        .deserialize("{ \"abc\": \"never-mind\", \"ghi\": {\"more\": \"things\"}}")

    val expected =
      Jsons.deserialize(
        "{ \"abc\": \"never-mind\", \"ghi\": {\"more\": \"things\"}, \"def\": \"asdf\"}",
      )
    assertEquals(expected, mergeNodes(mainNode, updateNode))
  }

  @Test
  fun testSetNestedValue() {
    val node =
      Jsons
        .deserialize("{ \"abc\": \"testing\", \"def\": \"asdf\"}")
    val expected =
      Jsons.deserialize(
        "{ \"abc\": \"testing\", \"def\": \"asdf\", \"nest\": {\"key\": \"value\"}}",
      )

    setNestedValue(node, mutableListOf<String>("nest", "key"), TextNode.valueOf("value"))
    assertEquals(expected, node)
  }

  @Test
  fun testGetNodeOrEmptyObject() {
    val root = Jsons.deserialize("{\"child\": {\"key\": \"value\"}}")
    val result = getNodeOrEmptyObject(root)
    assertEquals(root, result)
  }

  @Test
  fun testGetNodeOrEmptyObjectNested() {
    val root = Jsons.deserialize("{\"nested\": {\"key\": \"value\"}}")
    val result = getNodeOrEmptyObject(root, "nested", "key")
    assertEquals(TextNode.valueOf("value"), result)
  }

  @Test
  fun testGetNodeOrEmptyObjectNonexistentNestedKey() {
    val root = Jsons.deserialize("{\"emptyObjectKey\": {}}")

    val result = getNodeOrEmptyObject(root, "emptyObjectKey", "foo")
    assertEquals(emptyObject(), result)
  }

  @Test
  fun testGetNodeOrEmptyObjectFromNullRootNode() {
    val root = Jsons.deserialize("null")
    val result = getNodeOrEmptyObject(root, "child")
    assertEquals(emptyObject(), result)
  }

  @Test
  fun testGetNodeOrEmptyObjectPathToNullValue() {
    val root = Jsons.deserialize("{\"nullValueKey\": null}")
    val result = getNodeOrEmptyObject(root, "nullValueKey")
    assertEquals(emptyObject(), result)
  }

  private class ToClass {
    @JsonProperty("str")
    var str: String? = null

    @JsonProperty("num")
    var num: Int? = null

    @JsonProperty("numLong")
    var numLong: Long = 0

    constructor()

    constructor(str: String?, num: Int?, numLong: Long) {
      this.str = str
      this.num = num
      this.numLong = numLong
    }

    override fun equals(other: Any?): Boolean {
      if (this === other) {
        return true
      }
      if (other == null || javaClass != other.javaClass) {
        return false
      }
      val toClass = other as ToClass
      return numLong == toClass.numLong &&
        str == toClass.str &&
        num == toClass.num
    }

    override fun hashCode(): Int = Objects.hash(str, num, numLong)
  }

  /**
   * Test that [Jsons.canonicalJsonSerialize] returns a JSON string with keys sorted in
   * alphabetical order.
   */
  @Test
  fun testCanonicalJsonSerialize() {
    val actorCatalog =
      CatalogHelpers.createAirbyteCatalog(
        "clothes",
        Field.of("name", JsonSchemaType.STRING),
        Field.of("size", JsonSchemaType.NUMBER),
        Field.of("color", JsonSchemaType.STRING),
        Field.of("price", JsonSchemaType.NUMBER),
      )

    val actualJson = canonicalJsonSerialize(actorCatalog)

    val expectedJson =
      (
        "{" +
          "\"streams\":[" +
          "{" +
          "\"default_cursor_field\":[]," +
          "\"json_schema\":{" +
          "\"properties\":{" +
          "\"color\":{\"type\":\"string\"}," +
          "\"name\":{\"type\":\"string\"}," +
          "\"price\":{\"type\":\"number\"}," +
          "\"size\":{\"type\":\"number\"}" +
          "}," +
          "\"type\":\"object\"" +
          "}," +
          "\"name\":\"clothes\"," +
          "\"source_defined_primary_key\":[]," +
          "\"supported_sync_modes\":[\"full_refresh\"]" +
          "}" +
          "]" +
          "}"
      )

    // Assert that the result is a JSON string with keys sorted in alphabetical order
    assertEquals(expectedJson, actualJson)
  }

  @Test
  fun testDeserializeIfTextOnTextNode() {
    val textNode = TextNode.valueOf("{\"key1\": \"value1\"}")
    val jsonNode = JsonNodeFactory.instance.objectNode().set<JsonNode?>("key1", TextNode.valueOf("value1"))

    assertEquals(jsonNode, deserializeIfText(textNode))
  }

  @Test
  fun testDeserializeIfTextOnObjectNode() {
    val objectNode = JsonNodeFactory.instance.objectNode().set<JsonNode>("key1", TextNode.valueOf("value1"))

    assertEquals(objectNode, deserializeIfText(objectNode))
  }

  companion object {
    private const val SERIALIZED_JSON = "{\"str\":\"abc\",\"num\":999,\"numLong\":888}"
    private const val SERIALIZED_JSON2 = "{\"str\":\"abc\"}"
    private const val ABC = "abc"
    private const val DEF = "def"
    private const val GHI = "ghi"
    private const val JKL = "jkl"
    private const val PQR = "pqr"
    private const val TEST = "test"
    private const val TEST2 = "test2"
  }
}
