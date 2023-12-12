/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.secrets

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ArrayNode
import com.fasterxml.jackson.databind.node.ObjectNode
import com.google.common.annotations.VisibleForTesting
import com.google.common.base.Preconditions
import io.airbyte.commons.constants.AirbyteSecretConstants
import io.airbyte.commons.json.JsonPaths
import io.airbyte.commons.json.JsonSchemas
import io.airbyte.commons.json.Jsons
import io.airbyte.commons.util.MoreIterators
import io.airbyte.validation.json.JsonSchemaValidator
import io.github.oshai.kotlinlogging.KotlinLogging
import java.util.Optional
import java.util.stream.Collectors

private val logger = KotlinLogging.logger {}

class JsonSecretsProcessor(val copySecrets: Boolean = false) {
  companion object {
    protected val VALIDATOR = JsonSchemaValidator()
    const val PROPERTIES_FIELD = "properties"
    const val TYPE_FIELD = "type"
    const val ARRAY_TYPE_FIELD = "array"
    const val ITEMS_FIELD = "items"
    const val ONE_OF_FIELD = "oneOf"

    /**
     * Given a JSONSchema object and an object that conforms to that schema, obfuscate all fields in the
     * object that are a secret.
     *
     * @param json - json object that conforms to the schema
     * @param schema - jsonschema object
     * @return json object with all secrets masked.
     */
    fun maskAllSecrets(
      json: JsonNode,
      schema: JsonNode?,
    ): JsonNode {
      val pathsWithSecrets =
        JsonSchemas.collectPathsThatMeetCondition(
          schema,
        ) { node: JsonNode ->
          MoreIterators.toList(node.fields())
            .stream()
            .anyMatch { (key): Map.Entry<String, JsonNode> -> AirbyteSecretConstants.AIRBYTE_SECRET_FIELD == key }
        }
          .stream()
          .map { jsonSchemaPath: List<JsonSchemas.FieldNameOrList?>? ->
            JsonPaths.mapJsonSchemaPathToJsonPath(
              jsonSchemaPath,
            )
          }
          .collect(Collectors.toSet())
      var copy = Jsons.clone(json)
      for (path in pathsWithSecrets) {
        copy = JsonPaths.replaceAtString(copy, path, AirbyteSecretConstants.SECRETS_MASK)
      }
      return copy
    }

    fun isSecret(obj: JsonNode): Boolean {
      return obj.isObject && obj.has(AirbyteSecretConstants.AIRBYTE_SECRET_FIELD) && obj[AirbyteSecretConstants.AIRBYTE_SECRET_FIELD].asBoolean()
    }

    private fun findJsonCombinationNode(node: JsonNode): Optional<String> {
      for (combinationNode in listOf("allOf", "anyOf", "oneOf")) {
        if (node.has(combinationNode) && node[combinationNode].isArray) {
          return Optional.of(combinationNode)
        }
      }
      return Optional.empty()
    }

    @VisibleForTesting
    fun isValidJsonSchema(schema: JsonNode): Boolean {
      return schema.isObject &&
        (
          schema.has(PROPERTIES_FIELD) &&
            schema[PROPERTIES_FIELD].isObject ||
            schema.has(ONE_OF_FIELD) &&
            schema[ONE_OF_FIELD].isArray
        )
    }
  }

  /**
   * Returns a copy of the input object wherein any fields annotated with "airbyte_secret" in the
   * input schema are masked.
   *
   *
   * This method masks secrets both at the top level of the configuration object and in nested
   * properties in a oneOf.
   *
   * @param schema Schema containing secret annotations
   * @param obj Object containing potentially secret fields
   */
  fun prepareSecretsForOutput(
    obj: JsonNode,
    schema: JsonNode,
  ): JsonNode {
    // todo (cgardens) this is not safe. should throw.
    // if schema is an object and has a properties field
    if (!isValidJsonSchema(schema)) {
      logger.error { "The schema is not valid, the secret can't be hidden" }
      return obj
    }
    return maskAllSecrets(obj, schema)
  }

  /**
   * Returns a copy of the destination object in which any secret fields (as denoted by the input
   * schema) found in the source object are added.
   *
   *
   * This method absorbs secrets both at the top level of the configuration object and in nested properties in a oneOf.
   *
   * @param src The object potentially containing secrets
   * @param dst The object to absorb secrets into
   * @param schema Schema of objects
   * @return dst object with secrets absorbed from src object
   */
  fun copySecrets(
    src: JsonNode,
    dst: JsonNode,
    schema: JsonNode,
  ): JsonNode {
    // todo (cgardens) - figure out how to reused JsonSchemas and JsonPaths for this traversal as well.
    if (copySecrets) {
      // todo (cgardens) this is not safe. should throw.
      if (!isValidJsonSchema(schema)) {
        return dst
      }
      Preconditions.checkArgument(dst.isObject)
      Preconditions.checkArgument(src.isObject)
      val dstCopy = dst.deepCopy<ObjectNode>()
      return copySecretsRecursive(src, dstCopy, schema)
    }
    return src
  }

  // This function is modifying dstCopy in place.
  private fun copySecretsRecursive(
    src: JsonNode,
    dstCopy: JsonNode,
    schema: JsonNode,
  ): JsonNode {
    // todo (cgardens) this is not safe. should throw.
    if (!isValidJsonSchema(schema)) {
      return dstCopy
    }
    Preconditions.checkArgument(dstCopy.isObject)
    Preconditions.checkArgument(src.isObject)
    val combinationKey = findJsonCombinationNode(schema)
    if (combinationKey.isPresent) {
      val arrayNode = schema[combinationKey.get()] as ArrayNode
      for (i in 0 until arrayNode.size()) {
        val childSchema = arrayNode[i]
                /*
                 * when traversing a oneOf or anyOf if multiple schema in the oneOf or anyOf have the SAME key, but
                 * a different type, then, without this test, we can try to apply the wrong schema to the object
                 * resulting in errors because of type mismatches.
                 */
        if (VALIDATOR.test(childSchema, dstCopy)) {
          // Absorb field values if any of the combination option is declaring it as secrets
          copySecretsRecursive(src, dstCopy, childSchema)
        }
      }
    } else {
      val properties = schema[PROPERTIES_FIELD] as ObjectNode
      for (key in Jsons.keys(properties)) {
        // If the source or destination doesn't have this key then we have nothing to copy, so we should
        // skip to the next key.
        if (!src.has(key) || !dstCopy.has(key)) {
          continue
        }
        val fieldSchema = properties[key]
        // We only copy the original secret if the destination object isn't attempting to overwrite it
        // I.e. if the destination object's value is set to the mask, then we can copy the original secret
        if (isSecret(fieldSchema) && AirbyteSecretConstants.SECRETS_MASK == dstCopy[key].asText()) {
          (dstCopy as ObjectNode).set<JsonNode>(key, src[key])
        } else {
          // Otherwise, this is just a plain old json node; recurse into it. If it's not actually an object,
          // the recursive call will exit immediately.
          val copiedField = copySecretsRecursive(src[key], dstCopy[key], fieldSchema)
          (dstCopy as ObjectNode).set<JsonNode>(key, copiedField)
        }
      }
    }
    return dstCopy
  }
}
