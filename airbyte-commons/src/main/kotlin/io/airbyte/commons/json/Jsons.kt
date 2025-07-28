/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.json

import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.core.StreamReadConstraints
import com.fasterxml.jackson.core.type.TypeReference
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter
import com.fasterxml.jackson.core.util.Separators
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.ObjectWriter
import com.fasterxml.jackson.databind.node.ArrayNode
import com.fasterxml.jackson.databind.node.JsonNodeFactory
import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.databind.node.TextNode
import com.google.common.base.Charsets
import com.google.common.base.Preconditions
import io.airbyte.commons.jackson.MoreMappers.initMapper
import java.io.File
import java.io.IOException
import java.util.Collections
import java.util.Optional
import java.util.Spliterator
import java.util.Spliterators
import java.util.function.BiConsumer
import java.util.function.Function
import java.util.function.Predicate
import java.util.stream.Collectors
import java.util.stream.StreamSupport
import kotlin.Any
import kotlin.Boolean
import kotlin.ByteArray
import kotlin.Exception
import kotlin.Int
import kotlin.RuntimeException
import kotlin.Throwable
import kotlin.Throws
import kotlin.plus

/**
 * Shared code for operating on JSON in java.
 */
object Jsons {
  private val STREAM_READ_CONSTRAINTS: StreamReadConstraints =
    StreamReadConstraints
      .builder()
      .maxStringLength(Int.Companion.MAX_VALUE)
      .build()

  // Object Mapper is thread-safe
  private val OBJECT_MAPPER: ObjectMapper = initMapper()

  init {
    OBJECT_MAPPER.factory.setStreamReadConstraints(STREAM_READ_CONSTRAINTS)
  }

  /**
   * Exact ObjectMapper preserves float information by using the Java Big Decimal type.
   */
  private val OBJECT_MAPPER_EXACT: ObjectMapper = initMapper()

  init {
    OBJECT_MAPPER_EXACT.enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
    OBJECT_MAPPER_EXACT.factory.setStreamReadConstraints(STREAM_READ_CONSTRAINTS)
  }

  private val OBJECT_WRITER: ObjectWriter = OBJECT_MAPPER.writer(JsonPrettyPrinter())

  /**
   * Serialize an object to a JSON string.
   *
   * @param obj to serialize
   * @param <T> type of object
   * @return object as JSON string
   */
  @JvmStatic
  fun <T> serialize(obj: T?): String {
    try {
      return OBJECT_MAPPER.writeValueAsString(obj)
    } catch (e: JsonProcessingException) {
      throw RuntimeException(e)
    }
  }

  /**
   * Deserialize a JSON string to an object with a type.
   *
   * @param jsonString to deserialize
   * @param klass of object
   * @param <T> type of object
   * @return deserialized string as type declare in klass
   */
  @JvmStatic
  fun <T> deserialize(
    jsonString: String,
    klass: Class<T>,
  ): T {
    try {
      return OBJECT_MAPPER.readValue<T>(jsonString, klass)
    } catch (e: IOException) {
      throw RuntimeException(e)
    }
  }

  /**
   * Deserialize a JSON string to an object with a type.
   *
   * @param jsonString to deserialize
   * @param valueTypeRef of object
   * @param <T> type of object
   * @return deserialized string as type declare in valueTypeRef
   */
  @JvmStatic
  fun <T> deserialize(
    jsonString: String,
    valueTypeRef: TypeReference<T>,
  ): T {
    try {
      return OBJECT_MAPPER.readValue<T>(jsonString, valueTypeRef)
    } catch (e: IOException) {
      throw RuntimeException(e)
    }
  }

  /**
   * Deserialize a JSON file to an object with a type.
   *
   * @param file containing JSON to deserialize
   * @param klass of object
   * @param <T> type of object
   * @return deserialized string as type declare in klass
   */
  @JvmStatic
  fun <T> deserialize(
    file: File,
    klass: Class<T>,
  ): T {
    try {
      return OBJECT_MAPPER.readValue<T>(file, klass)
    } catch (e: IOException) {
      throw RuntimeException(e)
    }
  }

  /**
   * Deserialize a JSON file to an object with a type.
   *
   * @param file containing JSON to deserialize
   * @param valueTypeRef of object
   * @param <T> type of object
   * @return deserialized string as type declare in klass
   */
  @JvmStatic
  fun <T> deserialize(
    file: File,
    valueTypeRef: TypeReference<T>,
  ): T {
    try {
      return OBJECT_MAPPER.readValue<T>(file, valueTypeRef)
    } catch (e: IOException) {
      throw RuntimeException(e)
    }
  }

  /**
   * Deserialize a JSON string to a [JsonNode].
   *
   * @param jsonString to deserialize
   * @return JSON as JsonNode
   */
  @JvmStatic
  fun deserialize(jsonString: String): JsonNode {
    try {
      return OBJECT_MAPPER.readTree(jsonString)
    } catch (e: IOException) {
      throw RuntimeException(e)
    }
  }

  /**
   * Deserialize a JSON string to a [JsonNode]. If not possible, return empty optional.
   *
   * @param jsonString to deserialize
   * @return JSON as JsonNode wrapped in an Optional. If deserialization fails, returns an empty
   * optional.
   */
  @JvmStatic
  fun tryDeserialize(jsonString: String): Optional<JsonNode> {
    try {
      return Optional.of(OBJECT_MAPPER.readTree(jsonString))
    } catch (e: Throwable) {
      return Optional.empty<JsonNode>()
    }
  }

  /**
   * Deserialize a string to a JSON object using the exact ObjectMapper.
   *
   * @param jsonString to deserialize.
   * @param klass to deserialize to.
   * @param <T> type of input object.
   * @return optional as type T.
   */
  @JvmStatic
  fun <T : Any> tryDeserializeExact(
    jsonString: String,
    klass: Class<T>,
  ): Optional<T> {
    try {
      return Optional.of<T>(OBJECT_MAPPER_EXACT.readValue<T>(jsonString, klass))
    } catch (e: Throwable) {
      return Optional.empty<T>()
    }
  }

  /**
   * Convert an object to [JsonNode].
   *
   * @param obj to convert
   * @param <T> type of input object
   * @return object as JsonNode
   */
  @JvmStatic
  fun <T> jsonNode(obj: T?): JsonNode = OBJECT_MAPPER.valueToTree(obj)

  /**
   * Create an empty JSON object as JsonNode.
   *
   * @return empty JSON object as JsonNode
   */
  @JvmStatic
  fun emptyObject(): JsonNode = jsonNode<MutableMap<Any?, Any?>>(mutableMapOf())

  /**
   * Create an empty Jackson array node.
   *
   * @return an empty Jackson array node
   */
  @JvmStatic
  fun arrayNode(): ArrayNode = OBJECT_MAPPER.createArrayNode()

  /**
   * Convert a java object into a typed JSON object.
   *
   * @param obj to convert
   * @param klass to convert to
   * @param <T> type of class
   * @return object converted to class. if the input is null it returns a null JsonNode.
   */
  @JvmStatic
  fun <T> convertValue(
    obj: Any?,
    klass: Class<T>,
  ): T = OBJECT_MAPPER.convertValue<T>(obj, klass)

  /**
   * Convert a JsonNode object into a typed JSON object.
   *
   * @param jsonNode to convert
   * @param klass to convert to
   * @param <T> type of class
   * @return object converted to class
   */
  @JvmStatic
  fun <T> `object`(
    jsonNode: JsonNode,
    klass: Class<T>,
  ): T = OBJECT_MAPPER.convertValue<T>(jsonNode, klass)

  /**
   * Convert a JsonNode object into a typed JSON object.
   *
   * @param jsonNode to convert
   * @param typeReference to convert to
   * @param <T> type of class
   * @return object converted to class
   */
  @JvmStatic
  fun <T> `object`(
    jsonNode: JsonNode,
    typeReference: TypeReference<T>,
  ): T = OBJECT_MAPPER.convertValue<T>(jsonNode, typeReference)

  /**
   * Convert a JsonNode object into a typed JSON object. Return empty optional if it fails.
   *
   * @param jsonNode to convert
   * @param klass to convert to
   * @param <T> type of class
   * @return object converted to class wrapped in an optional. if it fails, empty optional.
   */
  @JvmStatic
  fun <T : Any> tryObject(
    jsonNode: JsonNode?,
    klass: Class<T>,
  ): Optional<T> {
    try {
      return Optional.of<T>(OBJECT_MAPPER.convertValue<T>(jsonNode, klass))
    } catch (e: Exception) {
      return Optional.empty<T>()
    }
  }

  /**
   * Convert a JsonNode object into a typed JSON object. Return empty optional if it fails.
   *
   * @param jsonNode to convert
   * @param typeReference to convert to
   * @param <T> type of class
   * @return object converted to class wrapped in an optional. if it fails, empty optional.
   */
  @JvmStatic
  fun <T : Any> tryObject(
    jsonNode: JsonNode?,
    typeReference: TypeReference<T>,
  ): Optional<T> {
    try {
      return Optional.of<T>(OBJECT_MAPPER.convertValue<T>(jsonNode, typeReference))
    } catch (e: Exception) {
      return Optional.empty<T>()
    }
  }

  /**
   * Deep clone a JSON-compatible object (i.e. JsonNode or a class generated by json2pojo.
   *
   * @param obj to clone
   * @param <T> type of object
   * @return cloned object
   */
  @JvmStatic
  fun <T : Any> clone(obj: T): T = deserialize(serialize(obj), obj.javaClass)

  /**
   * Convert a [JsonNode] object to a byte array.
   *
   * @param jsonNode to convert
   * @return json node as byte array
   */
  @JvmStatic
  fun toBytes(jsonNode: JsonNode?): ByteArray = serialize<JsonNode?>(jsonNode).toByteArray(Charsets.UTF_8)

  /**
   * Use string length as an estimation for byte size, because all ASCII characters are one byte long
   * in UTF-8, and ASCII characters cover most of the use cases. To be more precise, we can convert
   * the string to byte[] and use the length of the byte[]. However, this conversion is expensive in
   * memory consumption. Given that the byte size of the serialized JSON is already an estimation of
   * the actual size of the JSON object, using a cheap operation seems an acceptable compromise.
   */
  @JvmStatic
  fun getEstimatedByteSize(jsonNode: JsonNode?): Int = serialize<JsonNode?>(jsonNode).length

  /**
   * Get top-level keys of a [JsonNode].
   *
   * @param jsonNode whose keys to extract
   * @return top-level keys of the object
   */
  @JvmStatic
  fun keys(jsonNode: JsonNode): MutableSet<String> {
    if (jsonNode.isObject) {
      return `object`(
        jsonNode,
        object : TypeReference<MutableMap<String, Any?>>() {},
      )?.keys ?: mutableSetOf()
    } else {
      return HashSet()
    }
  }

  /**
   * Get children of top-level [JsonNode].
   *
   * @param jsonNode whose children to extract
   * @return children of top-level object
   */
  @JvmStatic
  fun children(jsonNode: JsonNode): MutableList<JsonNode> =
    StreamSupport
      .stream(Spliterators.spliteratorUnknownSize(jsonNode.elements(), Spliterator.ORDERED), false)
      .toList()

  /**
   * Convert a JSON object to a string with each key / value on one line.
   *
   * @param jsonNode to convert
   * @return pretty string representation of the JSON.
   */
  @JvmStatic
  fun toPrettyString(jsonNode: JsonNode?): String {
    try {
      return OBJECT_WRITER.writeValueAsString(jsonNode) + "\n"
    } catch (e: JsonProcessingException) {
      throw RuntimeException(e)
    }
  }

  /**
   * Get the [JsonNode] at a location in a [JsonNode] object.
   *
   * @param json to navigate
   * @param keys keys to follow to a value
   * @return value at location specified by keys
   */
  @JvmStatic
  fun navigateTo(
    json: JsonNode?,
    keys: List<String>,
  ): JsonNode? {
    var json = json
    for (key in keys) {
      if (json == null) {
        return null
      }
      json = json.get(key)
    }
    return json
  }

  /**
   * Replace a value in a [JsonNode] with a [JsonNode]. This mutates the input object!
   *
   * @param json to mutate
   * @param keys path to mutation location
   * @param replacement new value to be place at location
   */
  @JvmStatic
  fun replaceNestedValue(
    json: JsonNode?,
    keys: List<String>,
    replacement: JsonNode?,
  ) {
    replaceNested(json, keys) { node: ObjectNode, finalKey: String -> node.put(finalKey, replacement) }
  }

  /**
   * Replace a value in a [JsonNode] with a string using a function that takes in the current
   * value and outputs a new value. This mutates the input object!
   *
   * @param json to mutate
   * @param keys path to mutation location
   * @param typedReplacement function that takes in the old value and outputs the new string value
   */
  private fun replaceNested(
    json: JsonNode?,
    keys: List<String>,
    typedReplacement: BiConsumer<ObjectNode, String>,
  ) {
    Preconditions.checkArgument(!keys.isEmpty(), "Must pass at least one key")
    val nodeContainingFinalKey = navigateTo(json, keys.subList(0, keys.size - 1))
    typedReplacement.accept(nodeContainingFinalKey as ObjectNode, keys.get(keys.size - 1))
  }

  /**
   * Get the [JsonNode] at a location in a [JsonNode] object. Empty optional if no value
   * at the specified location.
   *
   * @param json object to navigate
   * @param keys keys to follow to a value
   * @return value at location specified by keys wrapped in an optional. if no value there, empty
   * optional.
   */
  @JvmStatic
  fun getOptional(
    json: JsonNode?,
    keys: List<String>,
  ): Optional<JsonNode> {
    var json = json
    for (key in keys) {
      if (json == null) {
        return Optional.empty<JsonNode>()
      }

      json = json.get(key)
    }

    return Optional.ofNullable<JsonNode>(json)
  }

  /**
   * Get the [JsonNode] at a location in a [JsonNode] object. Empty object node if no
   * value at the specified location or the value at the specified location is null.
   *
   * @param json object to navigate
   * @param keys keys to follow to a value
   * @return value at location specified by keys wrapped in an optional. if no value there, empty
   * node.
   */
  @JvmStatic
  fun getNodeOrEmptyObject(
    json: JsonNode?,
    vararg keys: String,
  ): JsonNode {
    val defaultValue = emptyObject()
    val valueOptional = getOptional(json, listOf(*keys))
    return valueOptional.filter(Predicate { node: JsonNode -> !node.isNull }).orElse(defaultValue)
  }

  /**
   * Flattens an ObjectNode, or dumps it into a {null: value} map if it's not an object. When
   * applyFlattenToArray is true, each element in the array will be one entry in the returned map.
   * This behavior is used in the Redshift SUPER type. When it is false, the whole array will be one
   * entry. This is used in the JobTracker.
   */
  @JvmStatic
  @JvmOverloads
  fun flatten(
    node: JsonNode,
    applyFlattenToArray: Boolean = false,
  ): Map<String?, Any?> {
    if (node.isObject) {
      val output: MutableMap<String?, Any?> = HashMap()
      val it = node.fields()
      while (it.hasNext()) {
        val entry = it.next()
        val field = entry.key
        val value = entry.value
        mergeMaps(output, field, flatten(value, applyFlattenToArray).toMutableMap())
      }
      return output
    } else if (node.isArray && applyFlattenToArray) {
      val output: MutableMap<String?, Any?> = HashMap()
      val arrayLen = node.size()
      for (i in 0..<arrayLen) {
        val field = String.format("[%d]", i)
        val value = node.get(i)
        mergeMaps(output, field, flatten(value, applyFlattenToArray).toMutableMap())
      }
      return output
    } else {
      val value: Any?
      if (node.isBoolean) {
        value = node.asBoolean()
      } else if (node.isLong) {
        value = node.asLong()
      } else if (node.isInt) {
        value = node.asInt()
      } else if (node.isDouble) {
        value = node.asDouble()
      } else if (node.isValueNode && !node.isNull) {
        value = node.asText()
      } else {
        // Fallback handling for e.g. arrays
        value = node.toString()
      }
      return Collections.singletonMap<String?, Any?>(null, value)
    }
  }

  /**
   * Prepend all keys in subMap with prefix, then merge that map into originalMap.
   *
   * If subMap contains a null key, then instead it is replaced with prefix. I.e. {null: value} is
   * treated as {prefix: value} when merging into originalMap.
   */
  @JvmStatic
  fun mergeMaps(
    originalMap: MutableMap<String?, Any?>,
    prefix: String?,
    subMap: Map<String?, Any?>,
  ) {
    originalMap.putAll(
      subMap.entries.stream().collect(
        Collectors.toMap(
          Function { e: Map.Entry<String?, Any?>? ->
            val key = e!!.key
            if (key != null) {
              return@Function prefix + "." + key
            } else {
              return@Function prefix
            }
          },
          Function { obj: Map.Entry<String?, Any?>? -> obj!!.value },
        ),
      ),
    )
  }

  /**
   * Convert a [JsonNode] as a string-to-string map.
   *
   * @param json to convert
   * @return json as string-to-string map. null if input is null.
   */
  @JvmStatic
  fun deserializeToStringMap(json: JsonNode): MutableMap<String, String?> =
    OBJECT_MAPPER.convertValue(json, object : TypeReference<MutableMap<String, String?>>() {})

  /**
   * Convert a [JsonNode] as a string-to-string map.
   *
   * @param json to convert
   * @return json as string-to-string map. null if input is null.
   */
  @JvmStatic
  fun deserializeToMap(json: JsonNode?): MutableMap<String, Any?>? =
    OBJECT_MAPPER.convertValue<MutableMap<String, Any?>?>(json, object : TypeReference<MutableMap<String, Any?>?>() {})

  /**
   * Convert a [JsonNode] as a string-to-integer map.
   *
   * @param json to convert
   * @return json as string-to-integer map.
   */
  @JvmStatic
  fun deserializeToIntegerMap(json: JsonNode): MutableMap<String, Int?> =
    OBJECT_MAPPER.convertValue(json, object : TypeReference<MutableMap<String, Int?>>() {})

  /**
   * Convert a [JsonNode] as a list of string.
   *
   * @param json to convert
   * @return json as list of string
   */
  @JvmStatic
  fun deserializeToStringList(json: JsonNode): MutableList<String> =
    OBJECT_MAPPER.convertValue(json, object : TypeReference<MutableList<String>>() {})

  /**
   * Merge updateNode into mainNode. Stolen from
   * https://stackoverflow.com/questions/9895041/merging-two-json-documents-using-jackson
   */
  @JvmStatic
  fun mergeNodes(
    mainNode: JsonNode,
    updateNode: JsonNode,
  ): JsonNode {
    val fieldNames = updateNode.fieldNames()
    while (fieldNames.hasNext()) {
      val fieldName = fieldNames.next()
      val jsonNode = mainNode.get(fieldName)
      // if field exists and is an embedded object
      if (jsonNode != null && jsonNode.isObject) {
        mergeNodes(jsonNode, updateNode.get(fieldName))
      } else {
        if (mainNode is ObjectNode) {
          // Overwrite field
          val value = updateNode.get(fieldName)
          mainNode.replace(fieldName, value)
        }
      }
    }

    return mainNode
  }

  /**
   * Converts a JsonNode containing a list of strings into a single string with each element separated
   * by the specified separator.
   *
   * @param node the JsonNode containing the list of strings to be joined
   * @param separator the string to be used as the separator between each element
   * @return a single string with each element from the list separated by the specified separator
   */
  @JvmStatic
  fun stringListToJoinedString(
    node: JsonNode,
    separator: String,
  ): String = deserializeToStringList(node).joinToString(separator)

  /**
   * Sets a value in a JSON node for a given key.
   *
   * @param <T> the type of the object to set in the JSON node
   * @param mainNode the main JSON node where the key-value pair will be set
   * @param key the key for which the value will be set
   * @param obj the value to set for the given key
   */
  @JvmStatic
  fun <T> setNode(
    mainNode: JsonNode,
    key: String,
    obj: T?,
  ) {
    (mainNode as ObjectNode).set<JsonNode?>(key, jsonNode<T?>(obj))
  }

  /**
   * Sets a nested node to the passed value. Creates nodes on the way if necessary.
   */
  private fun setNested(
    json: JsonNode,
    keys: List<String>,
    typedValue: BiConsumer<ObjectNode, String>,
  ) {
    Preconditions.checkArgument(!keys.isEmpty(), "Must pass at least one key")
    val nodeContainingFinalKey = navigateToAndCreate(json, keys.subList(0, keys.size - 1))
    typedValue.accept(nodeContainingFinalKey as ObjectNode, keys.get(keys.size - 1))
  }

  /**
   * Navigates to a node based on provided nested keys. Creates necessary parent nodes.
   */
  @JvmStatic
  fun navigateToAndCreate(
    baseNode: JsonNode,
    keys: List<String>,
  ): JsonNode {
    var node: JsonNode? = baseNode
    for (key in keys) {
      val currentNode = node as ObjectNode?
      node = node?.get(key)
      if (node == null || node.isNull) {
        node = emptyObject()
        currentNode!!.set<JsonNode?>(key, node)
      }
    }
    return node!!
  }

  /**
   * Set nested value and create parent keys on the way. Copied from our other replaceNestedValue.
   *
   * @param json node
   * @param keys list of keys that you want to nest into
   * @param value node value to set
   */
  @JvmStatic
  fun setNestedValue(
    json: JsonNode,
    keys: List<String>,
    value: JsonNode?,
  ) {
    setNested(json, keys) { node: ObjectNode, finalKey: String -> node.set<JsonNode?>(finalKey, value) }
  }

  /**
   * Serializes an object to a JSON string with keys sorted in alphabetical order.
   *
   * @param obj the object to serialize
   * @return the JSON string
   * @throws IOException if there is an error serializing the object
   */
  @JvmStatic
  @Throws(IOException::class)
  fun canonicalJsonSerialize(obj: Any?): String {
    val mapper = ObjectMapper()
    val objectNode = mapper.valueToTree<ObjectNode>(obj)
    val sortedObjectNode = sortProperties(objectNode) as ObjectNode
    return mapper.writer().writeValueAsString(sortedObjectNode)
  }

  private fun sortProperties(jsonNode: JsonNode): JsonNode =
    when {
      jsonNode.isObject -> {
        val objectNode = jsonNode as ObjectNode
        val sortedObjectNode = JsonNodeFactory.instance.objectNode()

        val sortedFields =
          objectNode
            .fields()
            .asSequence()
            .toList()
            .sortedBy { it.key }

        for ((key, value) in sortedFields) {
          sortedObjectNode.set<JsonNode>(key, sortProperties(value))
        }

        sortedObjectNode
      }

      jsonNode.isArray -> {
        val arrayNode = jsonNode as ArrayNode
        val sortedArrayNode = JsonNodeFactory.instance.arrayNode()
        for (node in arrayNode) {
          sortedArrayNode.add(sortProperties(node))
        }
        sortedArrayNode
      }

      else -> jsonNode
    }

  /**
   * If the supplied object is a TextNode, attempt to deserialize it and return the result. Otherwise,
   * return the object as-is.
   *
   * @param jsonNode the object to deserialize if in text form
   * @return the deserialized JSON object
   * @throws RuntimeException if the jsonNode is text that cannot be deserialized
   */
  @JvmStatic
  fun deserializeIfText(jsonNode: JsonNode): JsonNode {
    if (jsonNode is TextNode) {
      return deserialize(jsonNode.asText())
    }
    return jsonNode
  }

  /**
   * By the Jackson DefaultPrettyPrinter prints objects with an extra space as follows: {"name" :
   * "airbyte"}. We prefer {"name": "airbyte"}.
   */
  private class JsonPrettyPrinter : DefaultPrettyPrinter() {
    // this method has to be overridden because in the superclass it checks that it is an instance of
    // DefaultPrettyPrinter (which is no longer the case in this inherited class).
    override fun createInstance(): DefaultPrettyPrinter = DefaultPrettyPrinter(this)

    // override the method that inserts the extra space.
    override fun withSeparators(separators: Separators): DefaultPrettyPrinter {
      _separators = separators
      _objectFieldValueSeparatorWithSpaces = separators.objectFieldValueSeparator.toString() + " "
      return this
    }
  }
}
