/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.json

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ArrayNode
import com.jayway.jsonpath.PathNotFoundException
import io.airbyte.commons.json.JsonPaths.getExpandedPaths
import io.airbyte.commons.json.JsonPaths.getSingleValue
import io.airbyte.commons.json.JsonPaths.replaceAt
import io.airbyte.commons.json.JsonPaths.replaceAtJsonNodeLoud
import io.airbyte.commons.json.JsonPaths.replaceAtString
import io.airbyte.commons.json.Jsons.clone
import io.airbyte.commons.json.Jsons.jsonNode
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.function.Executable
import java.util.function.BiFunction
import java.util.function.Function

internal class JsonPathsTest {
  @Test
  fun testGetSingleValue() {
    Assertions.assertThrows<IllegalArgumentException?>(
      IllegalArgumentException::class.java,
      Executable { getSingleValue(JSON_NODE, LIST_ALL_QUERY) },
    )
    Assertions.assertEquals(1, getSingleValue(JSON_NODE, LIST_ONE_QUERY).map<Int?>(Function { obj: JsonNode? -> obj!!.asInt() }).orElse(null))
    Assertions.assertEquals(
      10,
      getSingleValue(JSON_NODE, NESTED_FIELD_QUERY).map<Int?>(Function { obj: JsonNode? -> obj!!.asInt() }).orElse(null),
    )
    Assertions.assertEquals(JSON_NODE.get("two"), getSingleValue(JSON_NODE, JSON_OBJECT_QUERY).orElse(null))
    Assertions.assertNull(getSingleValue(JSON_NODE, EMPTY_RETURN_QUERY).orElse(null))
  }

  @Test
  fun testReplaceAtString() {
    assertOriginalObjectNotModified(
      JSON_NODE,
      Runnable {
        val expected = clone<JsonNode>(JSON_NODE)
        (expected.get(ONE) as ArrayNode).set(1, REPLACEMENT_STRING)

        val actual = replaceAtString(JSON_NODE, LIST_ONE_QUERY, REPLACEMENT_STRING)
        Assertions.assertEquals(expected, actual)
      },
    )
  }

  @Test
  fun testReplaceAtStringEmptyReturnNoOp() {
    assertOriginalObjectNotModified(
      JSON_NODE,
      Runnable {
        val expected = clone<JsonNode>(JSON_NODE)
        val actual = replaceAtString(JSON_NODE, EMPTY_RETURN_QUERY, REPLACEMENT_STRING)
        Assertions.assertEquals(expected, actual)
      },
    )
  }

  @Test
  fun testReplaceAtJsonNodeLoud() {
    assertOriginalObjectNotModified(
      JSON_NODE,
      Runnable {
        val expected = clone<JsonNode>(JSON_NODE)
        (expected.get(ONE) as ArrayNode).set(1, REPLACEMENT_JSON)

        val actual = replaceAtJsonNodeLoud(JSON_NODE, LIST_ONE_QUERY, REPLACEMENT_JSON)
        Assertions.assertEquals(expected, actual)
      },
    )
  }

  @Test
  fun testReplaceAtJsonNodeLoudEmptyPathThrows() {
    assertOriginalObjectNotModified(
      JSON_NODE,
      Runnable {
        Assertions.assertThrows<PathNotFoundException?>(
          PathNotFoundException::class.java,
          Executable { replaceAtJsonNodeLoud(JSON_NODE, EMPTY_RETURN_QUERY, REPLACEMENT_JSON) },
        )
      },
    )
  }

  @Test
  fun testReplaceAtJsonNodeLoudMultipleReplace() {
    assertOriginalObjectNotModified(
      JSON_NODE,
      Runnable {
        val expected = clone<JsonNode>(JSON_NODE)
        (expected.get(ONE) as ArrayNode).set(0, REPLACEMENT_JSON)
        (expected.get(ONE) as ArrayNode).set(1, REPLACEMENT_JSON)
        (expected.get(ONE) as ArrayNode).set(2, REPLACEMENT_JSON)

        val actual = replaceAtJsonNodeLoud(JSON_NODE, LIST_ALL_QUERY, REPLACEMENT_JSON)
        Assertions.assertEquals(expected, actual)
      },
    )
  }

  // todo (cgardens) - this behavior is a little unintuitive, but based on the docs, there's not an
  // obvious workaround. in this case, i would expect this to silently do nothing instead of throwing.
  // for now just documenting it with a test. to avoid this, use the non-loud version of this method.
  @Test
  fun testReplaceAtJsonNodeLoudMultipleReplaceSplatInEmptyArrayThrows() {
    val expected = clone<JsonNode>(JSON_NODE)
    (expected.get(ONE) as ArrayNode).removeAll()

    assertOriginalObjectNotModified(
      expected,
      Runnable {
        Assertions.assertThrows<PathNotFoundException?>(
          PathNotFoundException::class.java,
          Executable { replaceAtJsonNodeLoud(expected, "$.one[*]", REPLACEMENT_JSON) },
        )
      },
    )
  }

  @Test
  fun testReplaceAt() {
    assertOriginalObjectNotModified(
      JSON_NODE,
      Runnable {
        val expected = clone<JsonNode>(JSON_NODE)
        (expected.get(ONE) as ArrayNode).set(1, "1-$['one'][1]")

        val actual =
          replaceAt(JSON_NODE, LIST_ONE_QUERY, BiFunction { node: JsonNode?, path: String? -> jsonNode<String?>(node.toString() + "-" + path) })
        Assertions.assertEquals(expected, actual)
      },
    )
  }

  @Test
  fun testReplaceAtMultiple() {
    assertOriginalObjectNotModified(
      JSON_NODE,
      Runnable {
        val expected = clone<JsonNode>(JSON_NODE)
        (expected.get(ONE) as ArrayNode).set(0, "0-$['one'][0]")
        (expected.get(ONE) as ArrayNode).set(1, "1-$['one'][1]")
        (expected.get(ONE) as ArrayNode).set(2, "2-$['one'][2]")

        val actual =
          replaceAt(JSON_NODE, LIST_ALL_QUERY, BiFunction { node: JsonNode?, path: String? -> jsonNode<String?>(node.toString() + "-" + path) })
        Assertions.assertEquals(expected, actual)
      },
    )
  }

  @Test
  fun testReplaceAtEmptyReturnNoOp() {
    assertOriginalObjectNotModified(
      JSON_NODE,
      Runnable {
        val expected = clone<JsonNode>(JSON_NODE)
        val actual =
          replaceAt(
            JSON_NODE,
            EMPTY_RETURN_QUERY,
            BiFunction { node: JsonNode?, path: String? -> jsonNode<String?>(node.toString() + "-" + path) },
          )
        Assertions.assertEquals(expected, actual)
      },
    )
  }

  @Test
  fun testGetExpandedPaths() {
    val testJson =
      """
      {
        "rotating_keys": [
          {"key1": "hunter1"},
          {"key2": "hunter2"},
          "str"
        ]
      }
      
      """.trimIndent()
    val testNode = Jsons.deserialize(testJson)

    // Test using a dot-notation template that should yield a result for key1.
    val resultKey1: MutableList<String?> = getExpandedPaths(testNode, "$.rotating_keys[*].key1").toMutableList()
    Assertions.assertEquals(1, resultKey1.size, "Expected one matching path for key1")
    Assertions.assertEquals("$.rotating_keys[0].key1", resultKey1.get(0))

    // Test using a bracket-notation template that should be normalized and yield a result for key2.
    val resultKey2: MutableList<String?> = getExpandedPaths(testNode, "$['rotating_keys'][*].key2").toMutableList()
    Assertions.assertEquals(1, resultKey2.size, "Expected one matching path for key2")
    Assertions.assertEquals("$.rotating_keys[1].key2", resultKey2.get(0))
  }

  @Test
  fun testGetExpandedPathsComplex() {
    val complexJson =
      """
      {
        "outer": {
          "inner": {
            "rotating_keys": [
              {"key1": "hunter1"},
              {"key2": "hunter2"},
              {"key3": "non-secret"},
              "string-value"
            ]
          },
          "another": [
            {
              "rotating_keys": [
                {"key1": "value1"},
                {"key2": "value2"}
              ]
            },
            {
              "rotating_keys": [
                {"key1": "value3"},
                {"key2": "value4"}
              ]
            }
          ]
        }
      }
      
      """.trimIndent()
    val complexNode = Jsons.deserialize(complexJson)

    // Test for inner.rotating_keys: template for key1
    val resultInnerKey1: MutableList<String?> = getExpandedPaths(complexNode, "$.outer.inner.rotating_keys[*].key1").toMutableList()
    // Only the first element in inner.rotating_keys has key1
    Assertions.assertEquals(1, resultInnerKey1.size, "Expected one matching path for inner key1")
    Assertions.assertEquals("$.outer.inner.rotating_keys[0].key1", resultInnerKey1.first())

    // Test for another array: template for key2 in each rotating_keys array
    val resultAnotherKey2: MutableList<String?> = getExpandedPaths(complexNode, "$.outer.another[*].rotating_keys[*].key2").toMutableList()
    // In both elements of the 'another' array, key2 is at index 1 in their rotating_keys arrays
    // The expected paths, when sorted, should be:
    // "$.outer.another[0].rotating_keys[1].key2" and "$.outer.another[1].rotating_keys[1].key2"
    Assertions.assertEquals(2, resultAnotherKey2.size, "Expected two matching paths for another key2")
    Assertions.assertEquals("$.outer.another[0].rotating_keys[1].key2", resultAnotherKey2.get(0))
    Assertions.assertEquals("$.outer.another[1].rotating_keys[1].key2", resultAnotherKey2.get(1))
  }

  companion object {
    private val JSON =
      """
      {
        "one": [0,1,2],
        "two": { "nested": 10}
      }
      """.trimIndent()
    private val JSON_NODE = Jsons.deserialize(JSON)
    private const val LIST_ALL_QUERY = "$.one[*]"
    private const val LIST_ONE_QUERY = "$.one[1]"
    private const val NESTED_FIELD_QUERY = "$.two.nested"
    private const val JSON_OBJECT_QUERY = "$.two"
    private const val EMPTY_RETURN_QUERY = "$.three"
    private const val REPLACEMENT_STRING = "replaced"
    private val REPLACEMENT_JSON = Jsons.deserialize("{ \"replacement\": \"replaced\" }")
    private const val ONE = "one"

    /**
     * For all replacement functions, they should NOT mutate in place. Helper assertion to verify that
     * invariant.
     *
     * @param json - json object used for testing
     * @param runnable - the rest of the test code that does the replacement
     */
    private fun assertOriginalObjectNotModified(
      json: JsonNode,
      runnable: Runnable,
    ) {
      val originalJsonNode = clone<JsonNode>(json)
      runnable.run()
      // verify the original object was not mutated.
      Assertions.assertEquals(originalJsonNode, json)
    }
  }
}
