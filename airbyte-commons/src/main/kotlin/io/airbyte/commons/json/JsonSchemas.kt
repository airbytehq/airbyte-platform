/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.json

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ObjectNode
import com.google.common.base.Preconditions
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.lang.invoke.MethodHandles
import java.util.Optional
import java.util.function.BiConsumer
import java.util.function.BiFunction
import java.util.function.Consumer
import java.util.function.Predicate

// todo (cgardens) - we need the ability to identify jsonschemas that Airbyte considers invalid for
// a connector (e.g. "not" keyword).

/**
 * Shared code for interacting with JSONSchema.
 */
object JsonSchemas {
  private val log: Logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass())

  private const val JSON_SCHEMA_ENUM_KEY = "enum"
  private const val JSON_SCHEMA_TYPE_KEY = "type"
  private const val JSON_SCHEMA_PROPERTIES_KEY = "properties"
  private const val JSON_SCHEMA_ITEMS_KEY = "items"
  private const val JSON_SCHEMA_ADDITIONAL_PROPERTIES_KEY = "additionalProperties"

  // all JSONSchema types.
  private const val ARRAY_TYPE = "array"
  private const val OBJECT_TYPE = "object"
  private const val STRING_TYPE = "string"
  private const val ONE_OF_TYPE = "oneOf"
  private const val ALL_OF_TYPE = "allOf"
  private const val ANY_OF_TYPE = "anyOf"

  private val COMPOSITE_KEYWORDS = setOf(ONE_OF_TYPE, ALL_OF_TYPE, ANY_OF_TYPE)

  /**
   * JsonSchema supports to ways of declaring type. `type: "string"` and `type: ["null", "string"]`.
   * This method will mutate a JsonNode with a type field so that the output type is the array
   * version.
   *
   * @param jsonNode - a json object with children that contain types.
   */
  @JvmStatic
  fun mutateTypeToArrayStandard(jsonNode: JsonNode) {
    if (jsonNode.get(JSON_SCHEMA_TYPE_KEY) != null && !jsonNode.get(JSON_SCHEMA_TYPE_KEY).isArray) {
      val type = jsonNode.get(JSON_SCHEMA_TYPE_KEY)
      (jsonNode as ObjectNode).putArray(JSON_SCHEMA_TYPE_KEY).add(type)
    }
  }

  /**
   * Traverse a JsonSchema object. The provided consumer will be called at each node with the node and
   * the path to the node.
   *
   * @param jsonSchema - JsonSchema object to traverse
   * @param consumer - accepts the current node and the path to that node.
   */
  @JvmStatic
  fun traverseJsonSchema(
    jsonSchema: JsonNode,
    consumer: BiConsumer<JsonNode, MutableList<FieldNameOrList>>,
  ) {
    traverseJsonSchemaInternal(jsonSchema, ArrayList(), consumer)
  }

  /**
   * Traverse a JsonSchema object. At each node, optionally map a value.
   *
   * @param jsonSchema - JsonSchema object to traverse
   * @param mapper - accepts the current node and the path to that node. if it returns an empty
   * optional, nothing will be collected, otherwise, whatever is returned will be collected and
   * returned by the final collection.
   * @param <T> - type of objects being collected
   * @return - collection of all items that were collected during the traversal. Returns values in
   * preoorder traversal order.
   </T> */
  private fun <T> traverseJsonSchemaWithFilteredCollector(
    jsonSchema: JsonNode,
    mapper: BiFunction<JsonNode, MutableList<FieldNameOrList>, Optional<T>>,
  ): List<T> {
    val collector: MutableList<T> = ArrayList()
    traverseJsonSchema(
      jsonSchema,
    ) { node: JsonNode, path: MutableList<FieldNameOrList> ->
      mapper
        .apply(node, path)
        .ifPresent(Consumer { e: T -> collector.add(e) })
    }
    return collector.toList() // make list unmodifiable
  }

  /**
   * Traverses a JsonSchema object. It returns the path to each node that meet the provided condition.
   * The paths are return in JsonPath format. The traversal is depth-first search preoorder and values
   * are returned in that order.
   *
   * @param obj - JsonSchema object to traverse
   * @param predicate - predicate to determine if the path for a node should be collected.
   * @return - collection of all paths that were collected during the traversal.
   */
  fun collectPathsThatMeetCondition(
    obj: JsonNode,
    predicate: Predicate<JsonNode>,
  ): List<List<FieldNameOrList>> {
    return traverseJsonSchemaWithFilteredCollector(
      obj,
      BiFunction { node: JsonNode, path: List<FieldNameOrList> ->
        if (predicate.test(node)) {
          return@BiFunction Optional.of<List<FieldNameOrList>>(path)
        } else {
          return@BiFunction Optional.empty<List<FieldNameOrList>>()
        }
      },
    )
  }

  /**
   * Recursive, depth-first implementation of JsonSchemas#traverseJsonSchema(...). Takes path as
   * argument so that the path can be passed to the consumer.
   *
   * @param jsonSchemaNode - jsonschema object to traverse.
   * @param consumer - consumer to be called at each node. it accepts the current node and the path to
   * the node from the root of the object passed at the root level invocation
   */
  private fun traverseJsonSchemaInternal(
    jsonSchemaNode: JsonNode,
    path: MutableList<FieldNameOrList>,
    consumer: BiConsumer<JsonNode, MutableList<FieldNameOrList>>,
  ) {
    require(jsonSchemaNode.isObject()) {
      String.format(
        "json schema nodes should always be object nodes. path: %s actual: %s",
        path,
        jsonSchemaNode,
      )
    }
    consumer.accept(jsonSchemaNode, path)
    // if type is missing assume object. not official JsonSchema, but it seems to be a common
    // compromise.
    val nodeTypes = getTypeOrObject(jsonSchemaNode)

    for (nodeType in nodeTypes) {
      when (nodeType) {
        ARRAY_TYPE -> {
          val newPath: MutableList<FieldNameOrList> = ArrayList(path.toList())
          newPath.add(FieldNameOrList.Companion.list())
          if (jsonSchemaNode.has(JSON_SCHEMA_ITEMS_KEY)) {
            // hit every node.
            traverseJsonSchemaInternal(jsonSchemaNode.get(JSON_SCHEMA_ITEMS_KEY), newPath, consumer)
          } else {
            log.warn("The array is missing an items field. The traversal is silently stopped. Current schema: " + jsonSchemaNode)
          }
        }

        OBJECT_TYPE -> {
          val comboKeyWordOptional = getKeywordIfComposite(jsonSchemaNode)
          if (jsonSchemaNode.has(JSON_SCHEMA_PROPERTIES_KEY)) {
            val it: Iterator<Map.Entry<String, JsonNode>> =
              jsonSchemaNode
                .get(
                  JSON_SCHEMA_PROPERTIES_KEY,
                ).fields()
            while (it.hasNext()) {
              val child = it.next()
              val newPath: MutableList<FieldNameOrList> = ArrayList(path.toList())
              newPath.add(FieldNameOrList.Companion.fieldName(child.key))
              traverseJsonSchemaInternal(child.value, newPath, consumer)
            }
          } else if (comboKeyWordOptional.isPresent) {
            for (arrayItem in jsonSchemaNode.get(comboKeyWordOptional.get())) {
              traverseJsonSchemaInternal(arrayItem, path, consumer)
            }
          } else {
            log.warn(
              "The object is a properties key or a combo keyword. The traversal is silently stopped. Current schema: " + jsonSchemaNode,
            )
          }
        }

        else -> {}
      }
    }
  }

  /**
   * If the object uses JSONSchema composite functionality (e.g. oneOf, anyOf, allOf), detect it and
   * return which one it is using.
   *
   * @param node - object to detect use of composite functionality.
   * @return the composite functionality being used, if not using composite functionality, empty.
   */
  private fun getKeywordIfComposite(node: JsonNode): Optional<String> {
    for (keyWord in COMPOSITE_KEYWORDS) {
      if (node.has(keyWord)) {
        return Optional.ofNullable<String>(keyWord)
      }
    }
    return Optional.empty<String>()
  }

  /**
   * Same logic as [.getType] except when no type is found, it defaults to type:
   * Object.
   *
   * @param jsonSchema - JSONSchema object
   * @return type of the node.
   */
  fun getTypeOrObject(jsonSchema: JsonNode): List<String> = getType(jsonSchema).ifEmpty { listOf(OBJECT_TYPE) }

  /**
   * Get the type of JSONSchema node. Uses JSONSchema types. Only returns the type of the "top-level"
   * node. e.g. if more nodes are nested underneath because it is an object or an array, only the top
   * level type is returned.
   *
   * @param jsonSchema - JSONSchema object
   * @return type of the node.
   */
  fun getType(jsonSchema: JsonNode): List<String> {
    if (jsonSchema.has(JSON_SCHEMA_TYPE_KEY)) {
      if (jsonSchema.get(JSON_SCHEMA_TYPE_KEY).isArray) {
        return jsonSchema
          .get(JSON_SCHEMA_TYPE_KEY)
          .iterator()
          .asSequence()
          .map { obj: JsonNode -> obj.asText() }
          .toList()
      } else {
        return listOf(jsonSchema.get(JSON_SCHEMA_TYPE_KEY).asText())
      }
    }
    if (jsonSchema.has(JSON_SCHEMA_ENUM_KEY)) {
      return listOf(STRING_TYPE)
    }
    return listOf()
  }

  /**
   * Checks if a JSON schema allows additional properties. According to JSON Schema specification: -
   * If additionalProperties is missing, additional properties are allowed (default: true) - If
   * additionalProperties is true, additional properties are allowed - If additionalProperties is
   * false, additional properties are not allowed - If additionalProperties is an object (schema),
   * additional properties are allowed but must conform to the schema
   *
   * @param schema the JSON schema to check
   * @return true if the schema allows additional properties, false otherwise
   */
  @JvmStatic
  fun allowsAdditionalProperties(schema: JsonNode): Boolean {
    val additionalPropertiesNode = schema.get(JSON_SCHEMA_ADDITIONAL_PROPERTIES_KEY)

    // If additionalProperties is not specified, default is true (allows additional properties)
    if (additionalPropertiesNode == null) {
      return true
    }

    // If it's a boolean, return its value
    if (additionalPropertiesNode.isBoolean()) {
      return additionalPropertiesNode.asBoolean()
    }

    // If it's an object (schema), additional properties are allowed but must conform to the schema
    if (additionalPropertiesNode.isObject()) {
      return true
    }

    // Default to true for any other case
    return true
  }

  /**
   * Provides a basic scheme for describing the path into a JSON object. Each element in the path is
   * either a field name or a list. If field name isn't set, we assume it is a list.
   *
   *
   * This class is helpful in the case where fields can be any UTF-8 string, so the only simple way to
   * keep track of the different parts of a path without going crazy with escape characters is to keep
   * it in a list with list set aside as a special case.
   *
   *
   * We prefer using this scheme instead of JSONPath in the tree traversal because, it is easier to
   * decompose a path in this scheme than it is in JSONPath. Some callers of the traversal logic want
   * to isolate parts of the path easily without the need for complex regex (that would be required if
   * we used JSONPath).
   */
  data class FieldNameOrList(
    private val fieldName: String?,
  ) {
    fun getFieldName(): String {
      Preconditions.checkState(fieldName != null, "cannot return field name, is list node")
      return fieldName!!
    }

    fun isList(): Boolean = fieldName == null

    companion object {
      @JvmStatic
      fun fieldName(fieldName: String): FieldNameOrList = FieldNameOrList(fieldName)

      @JvmStatic
      fun list(): FieldNameOrList = FieldNameOrList(null)
    }
  }
}
