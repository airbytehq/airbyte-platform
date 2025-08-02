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
import com.google.common.base.Charsets
import com.google.common.collect.ImmutableList
import com.google.common.collect.ImmutableMap
import com.google.common.collect.Lists
import com.google.common.collect.Sets
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
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.function.Executable
import java.io.IOException
import java.io.Serializable
import java.nio.charset.StandardCharsets
import java.util.Map
import java.util.Objects
import java.util.Optional

internal class JsonsTest {
  @Test
  fun testSerialize() {
    Assertions.assertEquals(
      SERIALIZED_JSON,
      Jsons.serialize<ToClass?>(ToClass(ABC, 999, 888L)),
    )

    Assertions.assertEquals(
      "{\"test\":\"abc\",\"test2\":\"def\"}",
      Jsons.serialize<ImmutableMap<String?, String?>?>(
        ImmutableMap.of<String?, String?>(
          TEST,
          ABC,
          TEST2,
          DEF,
        ),
      ),
    )
  }

  @Test
  fun testSerializeJsonNode() {
    Assertions.assertEquals(
      SERIALIZED_JSON,
      Jsons.serialize<JsonNode?>(jsonNode<ToClass?>(ToClass(ABC, 999, 888L))),
    )

    Assertions.assertEquals(
      "{\"test\":\"abc\",\"test2\":\"def\"}",
      Jsons.serialize<JsonNode?>(
        jsonNode<ImmutableMap<String?, String?>?>(
          ImmutableMap.of<String?, String?>(
            TEST,
            ABC,
            TEST2,
            DEF,
          ),
        ),
      ),
    )
    // issue: 5878 add test for binary node serialization, binary data are
    // serialized into base64
    Assertions.assertEquals(
      "{\"test\":\"dGVzdA==\"}",
      Jsons.serialize<JsonNode?>(
        jsonNode<ImmutableMap<String?, BinaryNode?>?>(
          ImmutableMap.of<String?, BinaryNode?>(
            TEST,
            BinaryNode("test".toByteArray(StandardCharsets.UTF_8)),
          ),
        ),
      ),
    )
  }

  @Test
  fun testDeserialize() {
    Assertions.assertEquals(
      ToClass(ABC, 999, 888L),
      Jsons.deserialize("{\"str\":\"abc\", \"num\": 999, \"numLong\": 888}", ToClass::class.java),
    )
  }

  @Test
  fun testDeserializeToJsonNode() {
    Assertions.assertEquals(
      SERIALIZED_JSON2,
      Jsons.deserialize(SERIALIZED_JSON2).toString(),
    )

    Assertions.assertEquals(
      "[{\"str\":\"abc\"},{\"str\":\"abc\"}]",
      Jsons.deserialize("[{\"str\":\"abc\"},{\"str\":\"abc\"}]").toString(),
    )
    // issue: 5878 add test for binary node deserialization, for now should be
    // base64 string
    Assertions.assertEquals(
      "{\"test\":\"dGVzdA==\"}",
      Jsons.deserialize("{\"test\":\"dGVzdA==\"}").toString(),
    )
  }

  @Test
  fun testTryDeserializeToJsonNode() {
    Assertions.assertEquals(
      Optional.of<JsonNode?>(Jsons.deserialize(SERIALIZED_JSON2)),
      tryDeserialize(SERIALIZED_JSON2),
    )

    Assertions.assertEquals(
      Optional.empty<Any?>(),
      tryDeserialize("{\"str\":\"abc\", \"num\": 999, \"test}"),
    )
  }

  @Test
  fun testToJsonNode() {
    Assertions.assertEquals(
      SERIALIZED_JSON,
      jsonNode<ToClass?>(ToClass(ABC, 999, 888L)).toString(),
    )

    Assertions.assertEquals(
      "{\"test\":\"abc\",\"test2\":\"def\"}",
      jsonNode<ImmutableMap<String?, String?>?>(
        ImmutableMap.of<String?, String?>(
          TEST,
          ABC,
          TEST2,
          DEF,
        ),
      ).toString(),
    )

    Assertions.assertEquals(
      "{\"test\":\"abc\",\"test2\":{\"inner\":1}}",
      jsonNode<ImmutableMap<String?, Serializable?>?>(
        ImmutableMap.of<String?, Serializable?>(
          TEST,
          ABC,
          TEST2,
          ImmutableMap.of<String?, Int?>("inner", 1),
        ),
      ).toString(),
    )

    Assertions.assertEquals(
      jsonNode<ToClass?>(ToClass(ABC, 999, 888L)),
      jsonNode<JsonNode?>(jsonNode<ToClass?>(ToClass(ABC, 999, 888L))),
    )
  }

  @Test
  fun testEmptyObject() {
    Assertions.assertEquals(Jsons.deserialize("{}"), emptyObject())
  }

  @Test
  fun testArrayNode() {
    Assertions.assertEquals(Jsons.deserialize("[]"), arrayNode())
  }

  @Test
  fun testToObject() {
    val expected = ToClass(ABC, 999, 888L)
    Assertions.assertEquals(
      expected,
      Jsons.`object`(jsonNode<ToClass?>(expected), ToClass::class.java),
    )

    Assertions.assertEquals(
      Lists.newArrayList<ToClass?>(expected),
      `object`<MutableList<ToClass?>?>(
        jsonNode<ArrayList<ToClass?>?>(Lists.newArrayList<ToClass?>(expected)),
        object : TypeReference<MutableList<ToClass?>?>() {},
      ),
    )

    Assertions.assertEquals(
      ToClass(),
      Jsons.`object`(Jsons.deserialize("{\"a\":1}"), ToClass::class.java),
    )
  }

  @Test
  fun testTryToObject() {
    val expected = ToClass(ABC, 999, 888L)
    Assertions.assertEquals(
      Optional.of<ToClass?>(expected),
      Jsons.tryObject(Jsons.deserialize(SERIALIZED_JSON), ToClass::class.java),
    )

    Assertions.assertEquals(
      Optional.of<ToClass?>(expected),
      Jsons.tryObject(Jsons.deserialize(SERIALIZED_JSON), object : TypeReference<ToClass>() {}),
    )

    val emptyExpected = ToClass()
    Assertions.assertEquals(
      Optional.of<ToClass?>(emptyExpected),
      Jsons.tryObject(Jsons.deserialize("{\"str1\":\"abc\"}"), ToClass::class.java),
    )

    Assertions.assertEquals(
      Optional.of<ToClass?>(emptyExpected),
      Jsons.tryObject(Jsons.deserialize("{\"str1\":\"abc\"}"), object : TypeReference<ToClass>() {}),
    )
  }

  @Test
  fun testClone() {
    val expected = ToClass("abc", 999, 888L)
    val actual = clone<ToClass>(expected)
    Assertions.assertNotSame(expected, actual)
    Assertions.assertEquals(expected, actual)
  }

  @Test
  fun testToBytes() {
    val jsonString = "{\"test\":\"abc\",\"type\":[\"object\"]}"
    Assertions.assertArrayEquals(jsonString.toByteArray(Charsets.UTF_8), toBytes(Jsons.deserialize(jsonString)))
  }

  @Test
  fun testKeys() {
    // test object json node
    val jsonNode = jsonNode<ImmutableMap<String?, String?>?>(ImmutableMap.of<String?, String?>(TEST, ABC, TEST2, DEF))
    Assertions.assertEquals(Sets.newHashSet<String?>(TEST, TEST2), keys(jsonNode))

    // test literal jsonNode
    Assertions.assertEquals(mutableSetOf<Any?>(), keys(jsonNode.get("test")))

    // test nested object json node. should only return top-level keys.
    val nestedJsonNode =
      jsonNode<ImmutableMap<String?, Serializable?>?>(
        ImmutableMap.of<String?, Serializable?>(
          TEST,
          ABC,
          TEST2,
          ImmutableMap.of<String?, String?>("test3", "def"),
        ),
      )
    Assertions.assertEquals(Sets.newHashSet<String?>(TEST, TEST2), keys(nestedJsonNode))

    // test array json node
    val arrayJsonNode =
      jsonNode<ImmutableList<ImmutableMap<String?, String?>?>?>(
        ImmutableList.of<ImmutableMap<String?, String?>?>(
          ImmutableMap.of<String?, String?>(
            TEST,
            ABC,
            TEST2,
            DEF,
          ),
        ),
      )
    Assertions.assertEquals(mutableSetOf<Any?>(), keys(arrayJsonNode))
  }

  @Test
  fun testToPrettyString() {
    val jsonNode = jsonNode<ImmutableMap<String?, String?>?>(ImmutableMap.of<String?, String?>(TEST, ABC))
    val expectedOutput = (
      "{\n" +
        "  \"test\" : \"abc\"\n" +
        "}\n"
    )
    Assertions.assertEquals(expectedOutput, toPrettyString(jsonNode))
  }

  @Test
  fun testGetEstimatedByteSize() {
    val json = Jsons.deserialize("{\"string_key\":\"abc\",\"array_key\":[\"item1\", \"item2\"]}")
    Assertions.assertEquals(toBytes(json).size, getEstimatedByteSize(json))
  }

  @Test
  fun testDeserializeToStringMap() {
    Assertions.assertEquals(Maps.newHashMap<String?, String?>("a", "b"), deserializeToStringMap(Jsons.deserialize("{ \"a\": \"b\" }")))
    Assertions.assertEquals(Maps.newHashMap<String?, Any?>("a", null), deserializeToStringMap(Jsons.deserialize("{ \"a\": null }")))
    Assertions.assertEquals(mutableMapOf<Any?, Any?>(), deserializeToStringMap(emptyObject()))
  }

  @Test
  fun testDeserializeToIntegerMap() {
    Assertions.assertEquals(Maps.newHashMap<String?, Int?>("a", 1), deserializeToIntegerMap(Jsons.deserialize("{ \"a\": 1 }")))
    Assertions.assertEquals(mutableMapOf<Any?, Any?>(), deserializeToIntegerMap(emptyObject()))
  }

  @Test
  fun testDeserializeToStringList() {
    Assertions.assertEquals(mutableListOf<String?>("a", "b"), deserializeToStringList(Jsons.deserialize("[ \"a\", \"b\" ]")))
    Assertions.assertThrows<IllegalArgumentException?>(
      IllegalArgumentException::class.java,
      Executable { deserializeToStringList(emptyObject()) },
    )
  }

  @Test
  fun testFlatten__noArrays() {
    val json = Jsons.deserialize("{ \"abc\": { \"def\": \"ghi\" }, \"jkl\": true, \"pqr\": 1 }")
    val expected =
      Map.of<String?, Any?>(
        "abc.def",
        GHI,
        JKL,
        true,
        PQR,
        1,
      )
    Assertions.assertEquals(expected, flatten(json, false))
  }

  @Test
  fun testFlatten__withArraysNoApplyFlatten() {
    val json =
      Jsons
        .deserialize("{ \"abc\": [{ \"def\": \"ghi\" }, { \"fed\": \"ihg\" }], \"jkl\": true, \"pqr\": 1 }")
    val expected =
      Map.of<String?, Any?>(
        ABC,
        "[{\"def\":\"ghi\"},{\"fed\":\"ihg\"}]",
        JKL,
        true,
        PQR,
        1,
      )
    Assertions.assertEquals(expected, flatten(json, false))
  }

  @Test
  fun testFlatten__checkBackwardCompatiblity() {
    val json =
      Jsons
        .deserialize("{ \"abc\": [{ \"def\": \"ghi\" }, { \"fed\": \"ihg\" }], \"jkl\": true, \"pqr\": 1 }")
    val expected =
      Map.of<String?, Any?>(
        ABC,
        "[{\"def\":\"ghi\"},{\"fed\":\"ihg\"}]",
        JKL,
        true,
        PQR,
        1,
      )
    Assertions.assertEquals(expected, flatten(json))
  }

  @Test
  fun testFlatten__withArraysApplyFlatten() {
    val json =
      Jsons
        .deserialize("{ \"abc\": [{ \"def\": \"ghi\" }, { \"fed\": \"ihg\" }], \"jkl\": true, \"pqr\": 1 }")
    val expected =
      Map.of<String?, Any?>(
        "abc.[0].def",
        "ghi",
        "abc.[1].fed",
        "ihg",
        JKL,
        true,
        PQR,
        1,
      )
    Assertions.assertEquals(expected, flatten(json, true))
  }

  @Test
  fun testFlatten__withArraysApplyFlattenNested() {
    val json =
      Jsons
        .deserialize(
          "{ \"abc\": [{ \"def\": {\"ghi\": [\"xyz\"] }}, { \"fed\": \"ihg\" }], \"jkl\": true, \"pqr\": 1 }",
        )
    val expected =
      Map.of<String?, Any?>(
        "abc.[0].def.ghi.[0]",
        "xyz",
        "abc.[1].fed",
        "ihg",
        JKL,
        true,
        PQR,
        1,
      )
    Assertions.assertEquals(expected, flatten(json, true))
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
    Assertions.assertEquals(expected, mergeNodes(mainNode, updateNode))
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
    Assertions.assertEquals(expected, node)
  }

  @Test
  fun testGetNodeOrEmptyObject() {
    val root = Jsons.deserialize("{\"child\": {\"key\": \"value\"}}")
    val result = getNodeOrEmptyObject(root)
    Assertions.assertEquals(root, result)
  }

  @Test
  fun testGetNodeOrEmptyObjectNested() {
    val root = Jsons.deserialize("{\"nested\": {\"key\": \"value\"}}")
    val result = getNodeOrEmptyObject(root, "nested", "key")
    Assertions.assertEquals(TextNode.valueOf("value"), result)
  }

  @Test
  fun testGetNodeOrEmptyObjectNonexistentNestedKey() {
    val root = Jsons.deserialize("{\"emptyObjectKey\": {}}")

    val result = getNodeOrEmptyObject(root, "emptyObjectKey", "foo")
    Assertions.assertEquals(emptyObject(), result)
  }

  @Test
  fun testGetNodeOrEmptyObjectFromNullRootNode() {
    val root = Jsons.deserialize("null")
    val result = getNodeOrEmptyObject(root, "child")
    Assertions.assertEquals(emptyObject(), result)
  }

  @Test
  fun testGetNodeOrEmptyObjectPathToNullValue() {
    val root = Jsons.deserialize("{\"nullValueKey\": null}")
    val result = getNodeOrEmptyObject(root, "nullValueKey")
    Assertions.assertEquals(emptyObject(), result)
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

    override fun equals(o: Any?): Boolean {
      if (this === o) {
        return true
      }
      if (o == null || javaClass != o.javaClass) {
        return false
      }
      val toClass = o as ToClass
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
  @Throws(IOException::class)
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
    Assertions.assertEquals(expectedJson, actualJson)
  }

  @Test
  fun testDeserializeIfTextOnTextNode() {
    val textNode = TextNode.valueOf("{\"key1\": \"value1\"}")
    val jsonNode = JsonNodeFactory.instance.objectNode().set<JsonNode?>("key1", TextNode.valueOf("value1"))

    Assertions.assertEquals(jsonNode, deserializeIfText(textNode))
  }

  @Test
  fun testDeserializeIfTextOnObjectNode() {
    val objectNode = JsonNodeFactory.instance.objectNode().set<JsonNode>("key1", TextNode.valueOf("value1"))

    Assertions.assertEquals(objectNode, deserializeIfText(objectNode))
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
