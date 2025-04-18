/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.json;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.jayway.jsonpath.PathNotFoundException;
import java.util.List;
import org.junit.jupiter.api.Test;

class JsonPathsTest {

  private static final String JSON = """
                                     {
                                       "one": [0,1,2],
                                       "two": { "nested": 10}
                                     }""";
  private static final JsonNode JSON_NODE = Jsons.deserialize(JSON);
  private static final String LIST_ALL_QUERY = "$.one[*]";
  private static final String LIST_ONE_QUERY = "$.one[1]";
  private static final String NESTED_FIELD_QUERY = "$.two.nested";
  private static final String JSON_OBJECT_QUERY = "$.two";
  private static final String EMPTY_RETURN_QUERY = "$.three";
  private static final String REPLACEMENT_STRING = "replaced";
  private static final JsonNode REPLACEMENT_JSON = Jsons.deserialize("{ \"replacement\": \"replaced\" }");
  private static final String ONE = "one";

  @Test
  void testGetSingleValue() {
    assertThrows(IllegalArgumentException.class, () -> JsonPaths.getSingleValue(JSON_NODE, LIST_ALL_QUERY));
    assertEquals(1, JsonPaths.getSingleValue(JSON_NODE, LIST_ONE_QUERY).map(JsonNode::asInt).orElse(null));
    assertEquals(10, JsonPaths.getSingleValue(JSON_NODE, NESTED_FIELD_QUERY).map(JsonNode::asInt).orElse(null));
    assertEquals(JSON_NODE.get("two"), JsonPaths.getSingleValue(JSON_NODE, JSON_OBJECT_QUERY).orElse(null));
    assertNull(JsonPaths.getSingleValue(JSON_NODE, EMPTY_RETURN_QUERY).orElse(null));
  }

  @Test
  void testReplaceAtString() {
    assertOriginalObjectNotModified(JSON_NODE, () -> {
      final JsonNode expected = Jsons.clone(JSON_NODE);
      ((ArrayNode) expected.get(ONE)).set(1, REPLACEMENT_STRING);

      final JsonNode actual = JsonPaths.replaceAtString(JSON_NODE, LIST_ONE_QUERY, REPLACEMENT_STRING);
      assertEquals(expected, actual);
    });
  }

  @Test
  void testReplaceAtStringEmptyReturnNoOp() {
    assertOriginalObjectNotModified(JSON_NODE, () -> {
      final JsonNode expected = Jsons.clone(JSON_NODE);

      final JsonNode actual = JsonPaths.replaceAtString(JSON_NODE, EMPTY_RETURN_QUERY, REPLACEMENT_STRING);
      assertEquals(expected, actual);
    });
  }

  @Test
  void testReplaceAtJsonNodeLoud() {
    assertOriginalObjectNotModified(JSON_NODE, () -> {
      final JsonNode expected = Jsons.clone(JSON_NODE);
      ((ArrayNode) expected.get(ONE)).set(1, REPLACEMENT_JSON);

      final JsonNode actual = JsonPaths.replaceAtJsonNodeLoud(JSON_NODE, LIST_ONE_QUERY, REPLACEMENT_JSON);
      assertEquals(expected, actual);
    });
  }

  @SuppressWarnings("CodeBlock2Expr")
  @Test
  void testReplaceAtJsonNodeLoudEmptyPathThrows() {
    assertOriginalObjectNotModified(JSON_NODE, () -> {
      assertThrows(PathNotFoundException.class, () -> JsonPaths.replaceAtJsonNodeLoud(JSON_NODE, EMPTY_RETURN_QUERY, REPLACEMENT_JSON));
    });
  }

  @Test
  void testReplaceAtJsonNodeLoudMultipleReplace() {
    assertOriginalObjectNotModified(JSON_NODE, () -> {
      final JsonNode expected = Jsons.clone(JSON_NODE);
      ((ArrayNode) expected.get(ONE)).set(0, REPLACEMENT_JSON);
      ((ArrayNode) expected.get(ONE)).set(1, REPLACEMENT_JSON);
      ((ArrayNode) expected.get(ONE)).set(2, REPLACEMENT_JSON);

      final JsonNode actual = JsonPaths.replaceAtJsonNodeLoud(JSON_NODE, LIST_ALL_QUERY, REPLACEMENT_JSON);
      assertEquals(expected, actual);
    });
  }

  // todo (cgardens) - this behavior is a little unintuitive, but based on the docs, there's not an
  // obvious workaround. in this case, i would expect this to silently do nothing instead of throwing.
  // for now just documenting it with a test. to avoid this, use the non-loud version of this method.
  @SuppressWarnings("CodeBlock2Expr")
  @Test
  void testReplaceAtJsonNodeLoudMultipleReplaceSplatInEmptyArrayThrows() {
    final JsonNode expected = Jsons.clone(JSON_NODE);
    ((ArrayNode) expected.get(ONE)).removeAll();

    assertOriginalObjectNotModified(expected, () -> {
      assertThrows(PathNotFoundException.class, () -> JsonPaths.replaceAtJsonNodeLoud(expected, "$.one[*]", REPLACEMENT_JSON));
    });
  }

  @Test
  void testReplaceAt() {
    assertOriginalObjectNotModified(JSON_NODE, () -> {
      final JsonNode expected = Jsons.clone(JSON_NODE);
      ((ArrayNode) expected.get(ONE)).set(1, "1-$['one'][1]");

      final JsonNode actual = JsonPaths.replaceAt(JSON_NODE, LIST_ONE_QUERY, (node, path) -> Jsons.jsonNode(node + "-" + path));
      assertEquals(expected, actual);
    });
  }

  @Test
  void testReplaceAtMultiple() {
    assertOriginalObjectNotModified(JSON_NODE, () -> {
      final JsonNode expected = Jsons.clone(JSON_NODE);
      ((ArrayNode) expected.get(ONE)).set(0, "0-$['one'][0]");
      ((ArrayNode) expected.get(ONE)).set(1, "1-$['one'][1]");
      ((ArrayNode) expected.get(ONE)).set(2, "2-$['one'][2]");

      final JsonNode actual = JsonPaths.replaceAt(JSON_NODE, LIST_ALL_QUERY, (node, path) -> Jsons.jsonNode(node + "-" + path));
      assertEquals(expected, actual);
    });
  }

  @Test
  void testReplaceAtEmptyReturnNoOp() {
    assertOriginalObjectNotModified(JSON_NODE, () -> {
      final JsonNode expected = Jsons.clone(JSON_NODE);

      final JsonNode actual = JsonPaths.replaceAt(JSON_NODE, EMPTY_RETURN_QUERY, (node, path) -> Jsons.jsonNode(node + "-" + path));
      assertEquals(expected, actual);
    });
  }

  @Test
  void testGetExpandedPaths() {
    String testJson = """
                      {
                        "rotating_keys": [
                          {"key1": "hunter1"},
                          {"key2": "hunter2"},
                          "str"
                        ]
                      }
                      """;
    JsonNode testNode = Jsons.deserialize(testJson);

    // Test using a dot-notation template that should yield a result for key1.
    List<String> resultKey1 = JsonPaths.getExpandedPaths(testNode, "$.rotating_keys[*].key1");
    assertEquals(1, resultKey1.size(), "Expected one matching path for key1");
    assertEquals("$.rotating_keys[0].key1", resultKey1.get(0));

    // Test using a bracket-notation template that should be normalized and yield a result for key2.
    List<String> resultKey2 = JsonPaths.getExpandedPaths(testNode, "$['rotating_keys'][*].key2");
    assertEquals(1, resultKey2.size(), "Expected one matching path for key2");
    assertEquals("$.rotating_keys[1].key2", resultKey2.get(0));
  }

  @Test
  void testGetExpandedPathsComplex() {
    String complexJson = """
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
                         """;
    JsonNode complexNode = Jsons.deserialize(complexJson);

    // Test for inner.rotating_keys: template for key1
    List<String> resultInnerKey1 = JsonPaths.getExpandedPaths(complexNode, "$.outer.inner.rotating_keys[*].key1");
    // Only the first element in inner.rotating_keys has key1
    assertEquals(1, resultInnerKey1.size(), "Expected one matching path for inner key1");
    assertEquals("$.outer.inner.rotating_keys[0].key1", resultInnerKey1.getFirst());

    // Test for another array: template for key2 in each rotating_keys array
    List<String> resultAnotherKey2 = JsonPaths.getExpandedPaths(complexNode, "$.outer.another[*].rotating_keys[*].key2");
    // In both elements of the 'another' array, key2 is at index 1 in their rotating_keys arrays
    // The expected paths, when sorted, should be:
    // "$.outer.another[0].rotating_keys[1].key2" and "$.outer.another[1].rotating_keys[1].key2"
    assertEquals(2, resultAnotherKey2.size(), "Expected two matching paths for another key2");
    assertEquals("$.outer.another[0].rotating_keys[1].key2", resultAnotherKey2.get(0));
    assertEquals("$.outer.another[1].rotating_keys[1].key2", resultAnotherKey2.get(1));
  }

  /**
   * For all replacement functions, they should NOT mutate in place. Helper assertion to verify that
   * invariant.
   *
   * @param json - json object used for testing
   * @param runnable - the rest of the test code that does the replacement
   */
  private static void assertOriginalObjectNotModified(final JsonNode json, final Runnable runnable) {
    final JsonNode originalJsonNode = Jsons.clone(json);
    runnable.run();
    // verify the original object was not mutated.
    assertEquals(originalJsonNode, json);
  }

}
