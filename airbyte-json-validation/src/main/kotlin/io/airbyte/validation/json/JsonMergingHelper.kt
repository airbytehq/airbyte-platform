/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.validation.json

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode

class JsonMergingHelper {
  /**
   * Combines all properties from two JSON nodes into a new JSON node.
   * Recursively merges the JSON structures, with the second node taking precedence.
   */
  fun combineProperties(
    firstNode: JsonNode?,
    secondNode: JsonNode?,
  ): JsonNode {
    val objectMapper = ObjectMapper()
    val combinedNode = objectMapper.createObjectNode()

    // Helper function to merge two JSON objects recursively
    fun mergeObjects(
      target: JsonNode,
      source: JsonNode,
    ) {
      // Ensure both nodes are objects
      if (!target.isObject || !source.isObject) {
        throw IllegalArgumentException("Both nodes must be objects to merge")
      }

      val targetObject = target as ObjectNode

      source.fields().forEach { (key, value) ->
        if (targetObject.has(key)) {
          val targetValue = targetObject.get(key)
          if (targetValue.isObject && value.isObject) {
            // Both are objects, merge them recursively
            mergeObjects(targetValue, value)
          } else if (targetValue.isObject != value.isObject) {
            // One is an object, the other isn't - this is a type conflict
            throw IllegalArgumentException(
              "Type mismatch for property '$key': Cannot merge object with non-object",
            )
          } else {
            // Neither is an object, just override
            targetObject.set<JsonNode>(key, value)
          }
        } else {
          // Key doesn't exist in target, just set it
          targetObject.set<JsonNode>(key, value)
        }
      }
    }

    firstNode?.let { mergeObjects(combinedNode, it) }
    secondNode?.let { mergeObjects(combinedNode, it) }

    return combinedNode
  }
}
