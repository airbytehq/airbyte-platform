/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.json;

import static java.util.Collections.singletonMap;
import static java.util.stream.Collectors.toMap;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.StreamReadConstraints;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter;
import com.fasterxml.jackson.core.util.Separators;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import io.airbyte.commons.jackson.MoreMappers;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.BiConsumer;
import java.util.stream.StreamSupport;

/**
 * Shared code for operating on JSON in java.
 */
@SuppressWarnings({"PMD.AvoidReassigningParameters", "PMD.AvoidCatchingThrowable"})
public class Jsons {

  private static final StreamReadConstraints STREAM_READ_CONSTRAINTS = StreamReadConstraints
      .builder()
      .maxStringLength(Integer.MAX_VALUE)
      .build();

  // Object Mapper is thread-safe
  private static final ObjectMapper OBJECT_MAPPER = MoreMappers.initMapper();

  static {
    OBJECT_MAPPER.getFactory().setStreamReadConstraints(STREAM_READ_CONSTRAINTS);
  }

  /**
   * Exact ObjectMapper preserves float information by using the Java Big Decimal type.
   */
  private static final ObjectMapper OBJECT_MAPPER_EXACT;

  static {
    OBJECT_MAPPER_EXACT = MoreMappers.initMapper();
    OBJECT_MAPPER_EXACT.enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
    OBJECT_MAPPER_EXACT.getFactory().setStreamReadConstraints(STREAM_READ_CONSTRAINTS);
  }

  private static final ObjectWriter OBJECT_WRITER = OBJECT_MAPPER.writer(new JsonPrettyPrinter());

  /**
   * Serialize an object to a JSON string.
   *
   * @param object to serialize
   * @param <T> type of object
   * @return object as JSON string
   */
  public static <T> String serialize(final T object) {
    try {
      return OBJECT_MAPPER.writeValueAsString(object);
    } catch (final JsonProcessingException e) {
      throw new RuntimeException(e);
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
  public static <T> T deserialize(final String jsonString, final Class<T> klass) {
    try {
      return OBJECT_MAPPER.readValue(jsonString, klass);
    } catch (final IOException e) {
      throw new RuntimeException(e);
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
  public static <T> T deserialize(final String jsonString, final TypeReference<T> valueTypeRef) {
    try {
      return OBJECT_MAPPER.readValue(jsonString, valueTypeRef);
    } catch (final IOException e) {
      throw new RuntimeException(e);
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
  public static <T> T deserialize(final File file, final Class<T> klass) {
    try {
      return OBJECT_MAPPER.readValue(file, klass);
    } catch (final IOException e) {
      throw new RuntimeException(e);
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
  public static <T> T deserialize(final File file, final TypeReference<T> valueTypeRef) {
    try {
      return OBJECT_MAPPER.readValue(file, valueTypeRef);
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Deserialize a JSON string to a {@link JsonNode}.
   *
   * @param jsonString to deserialize
   * @return JSON as JsonNode
   */
  public static JsonNode deserialize(final String jsonString) {
    try {
      return OBJECT_MAPPER.readTree(jsonString);
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Deserialize a JSON string to a {@link JsonNode}. If not possible, return empty optional.
   *
   * @param jsonString to deserialize
   * @return JSON as JsonNode wrapped in an Optional. If deserialization fails, returns an empty
   *         optional.
   */
  public static Optional<JsonNode> tryDeserialize(final String jsonString) {
    try {
      return Optional.of(OBJECT_MAPPER.readTree(jsonString));
    } catch (final Throwable e) {
      return Optional.empty();
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
  public static <T> Optional<T> tryDeserializeExact(final String jsonString, final Class<T> klass) {
    try {
      return Optional.of(OBJECT_MAPPER_EXACT.readValue(jsonString, klass));
    } catch (final Throwable e) {
      return Optional.empty();
    }
  }

  /**
   * Convert an object to {@link JsonNode}.
   *
   * @param object to convert
   * @param <T> type of input object
   * @return object as JsonNode
   */
  public static <T> JsonNode jsonNode(final T object) {
    return OBJECT_MAPPER.valueToTree(object);
  }

  /**
   * Create an empty JSON object as JsonNode.
   *
   * @return empty JSON object as JsonNode
   */
  public static JsonNode emptyObject() {
    return jsonNode(Collections.emptyMap());
  }

  /**
   * Create an empty Jackson array node.
   *
   * @return an empty Jackson array node
   */
  public static ArrayNode arrayNode() {
    return OBJECT_MAPPER.createArrayNode();
  }

  /**
   * Convert a java object into a typed JSON object.
   *
   * @param object to convert
   * @param klass to convert to
   * @param <T> type of class
   * @return object converted to class
   */
  public static <T> T convertValue(final Object object, final Class<T> klass) {
    return OBJECT_MAPPER.convertValue(object, klass);
  }

  /**
   * Convert a JsonNode object into a typed JSON object.
   *
   * @param jsonNode to convert
   * @param klass to convert to
   * @param <T> type of class
   * @return object converted to class
   */
  public static <T> T object(final JsonNode jsonNode, final Class<T> klass) {
    return OBJECT_MAPPER.convertValue(jsonNode, klass);
  }

  /**
   * Convert a JsonNode object into a typed JSON object.
   *
   * @param jsonNode to convert
   * @param typeReference to convert to
   * @param <T> type of class
   * @return object converted to class
   */
  public static <T> T object(final JsonNode jsonNode, final TypeReference<T> typeReference) {
    return OBJECT_MAPPER.convertValue(jsonNode, typeReference);
  }

  /**
   * Convert a JsonNode object into a typed JSON object. Return empty optional if it fails.
   *
   * @param jsonNode to convert
   * @param klass to convert to
   * @param <T> type of class
   * @return object converted to class wrapped in an optional. if it fails, empty optional.
   */
  public static <T> Optional<T> tryObject(final JsonNode jsonNode, final Class<T> klass) {
    try {
      return Optional.of(OBJECT_MAPPER.convertValue(jsonNode, klass));
    } catch (final Exception e) {
      return Optional.empty();
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
  public static <T> Optional<T> tryObject(final JsonNode jsonNode, final TypeReference<T> typeReference) {
    try {
      return Optional.of(OBJECT_MAPPER.convertValue(jsonNode, typeReference));
    } catch (final Exception e) {
      return Optional.empty();
    }
  }

  /**
   * Deep clone a JSON-compatible object (i.e. JsonNode or a class generated by json2pojo.
   *
   * @param object to clone
   * @param <T> type of object
   * @return cloned object
   */
  @SuppressWarnings("unchecked")
  public static <T> T clone(final T object) {
    return (T) deserialize(serialize(object), object.getClass());
  }

  /**
   * Convert a {@link JsonNode} object to a byte array.
   *
   * @param jsonNode to convert
   * @return json node as byte array
   */
  public static byte[] toBytes(final JsonNode jsonNode) {
    return serialize(jsonNode).getBytes(Charsets.UTF_8);
  }

  /**
   * Use string length as an estimation for byte size, because all ASCII characters are one byte long
   * in UTF-8, and ASCII characters cover most of the use cases. To be more precise, we can convert
   * the string to byte[] and use the length of the byte[]. However, this conversion is expensive in
   * memory consumption. Given that the byte size of the serialized JSON is already an estimation of
   * the actual size of the JSON object, using a cheap operation seems an acceptable compromise.
   */
  public static int getEstimatedByteSize(final JsonNode jsonNode) {
    return serialize(jsonNode).length();
  }

  /**
   * Get top-level keys of a {@link JsonNode}.
   *
   * @param jsonNode whose keys to extract
   * @return top-level keys of the object
   */
  public static Set<String> keys(final JsonNode jsonNode) {
    if (jsonNode.isObject()) {
      return Jsons.object(jsonNode, new TypeReference<Map<String, Object>>() {}).keySet();
    } else {
      return new HashSet<>();
    }
  }

  /**
   * Get children of top-level {@link JsonNode}.
   *
   * @param jsonNode whose children to extract
   * @return children of top-level object
   */
  public static List<JsonNode> children(final JsonNode jsonNode) {
    return StreamSupport.stream(Spliterators.spliteratorUnknownSize(jsonNode.elements(), Spliterator.ORDERED), false)
        .toList();
  }

  /**
   * Convert a JSON object to a string with each key / value on one line.
   *
   * @param jsonNode to convert
   * @return pretty string representation of the JSON.
   */
  public static String toPrettyString(final JsonNode jsonNode) {
    try {
      return OBJECT_WRITER.writeValueAsString(jsonNode) + "\n";
    } catch (final JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Get the {@link JsonNode} at a location in a {@link JsonNode} object.
   *
   * @param json to navigate
   * @param keys keys to follow to a value
   * @return value at location specified by keys
   */
  public static JsonNode navigateTo(JsonNode json, final List<String> keys) {
    for (final String key : keys) {
      if (json == null) {
        return null;
      }
      json = json.get(key);
    }
    return json;
  }

  /**
   * Replace a value in a {@link JsonNode} with a {@link JsonNode}. This mutates the input object!
   *
   * @param json to mutate
   * @param keys path to mutation location
   * @param replacement new value to be place at location
   */
  public static void replaceNestedValue(final JsonNode json, final List<String> keys, final JsonNode replacement) {
    replaceNested(json, keys, (node, finalKey) -> node.put(finalKey, replacement));
  }

  /**
   * Replace a value in a {@link JsonNode} with a string. This mutates the input object!
   *
   * @param json to mutate
   * @param keys path to mutation location
   * @param replacement new value to be place at location
   */
  public static void replaceNestedString(final JsonNode json, final List<String> keys, final String replacement) {
    replaceNested(json, keys, (node, finalKey) -> node.put(finalKey, replacement));
  }

  /**
   * Replace a value in a {@link JsonNode} with an int. This mutates the input object!
   *
   * @param json to mutate
   * @param keys path to mutation location
   * @param replacement new value to be place at location
   */
  public static void replaceNestedInt(final JsonNode json, final List<String> keys, final int replacement) {
    replaceNested(json, keys, (node, finalKey) -> node.put(finalKey, replacement));
  }

  /**
   * Replace a value in a {@link JsonNode} with a string using a function that takes in the current
   * value and outputs a new value. This mutates the input object!
   *
   * @param json to mutate
   * @param keys path to mutation location
   * @param typedReplacement function that takes in the old value and outputs the new string value
   */
  private static void replaceNested(final JsonNode json, final List<String> keys, final BiConsumer<ObjectNode, String> typedReplacement) {
    Preconditions.checkArgument(!keys.isEmpty(), "Must pass at least one key");
    final JsonNode nodeContainingFinalKey = navigateTo(json, keys.subList(0, keys.size() - 1));
    typedReplacement.accept((ObjectNode) nodeContainingFinalKey, keys.get(keys.size() - 1));
  }

  /**
   * Get the {@link JsonNode} at a location in a {@link JsonNode} object. Empty optional if no value
   * at the specified location.
   *
   * @param json object to navigate
   * @param keys keys to follow to a value
   * @return value at location specified by keys wrapped in an optional. if no value there, empty
   *         optional.
   */
  public static Optional<JsonNode> getOptional(final JsonNode json, final String... keys) {
    return getOptional(json, Arrays.asList(keys));
  }

  /**
   * Get the {@link JsonNode} at a location in a {@link JsonNode} object. Empty optional if no value
   * at the specified location.
   *
   * @param json object to navigate
   * @param keys keys to follow to a value
   * @return value at location specified by keys wrapped in an optional. if no value there, empty
   *         optional.
   */
  public static Optional<JsonNode> getOptional(JsonNode json, final List<String> keys) {
    for (final String key : keys) {
      if (json == null) {
        return Optional.empty();
      }

      json = json.get(key);
    }

    return Optional.ofNullable(json);
  }

  /**
   * Get the {@link JsonNode} at a location in a {@link JsonNode} object. Empty object node if no
   * value at the specified location or the value at the specified location is null.
   *
   * @param json object to navigate
   * @param keys keys to follow to a value
   * @return value at location specified by keys wrapped in an optional. if no value there, empty
   *         node.
   */
  public static JsonNode getNodeOrEmptyObject(final JsonNode json, final String... keys) {
    final JsonNode defaultValue = emptyObject();
    final Optional<JsonNode> valueOptional = getOptional(json, Arrays.asList(keys));
    return valueOptional.filter(node -> !node.isNull()).orElse(defaultValue);
  }

  /**
   * Get the {@link String} at a location in a {@link JsonNode} object. Returns null, if no value at
   * the specified location.
   *
   * @param json object to navigate
   * @param keys keys to follow to a value
   * @return string value at location specified by keys wrapped in an optional. if no value there,
   *         empty optional.
   */
  public static String getStringOrNull(final JsonNode json, final String... keys) {
    return getStringOrNull(json, Arrays.asList(keys));
  }

  /**
   * Get the {@link String} at a location in a {@link JsonNode} object. Returns null, if no value at
   * the specified location.
   *
   * @param json object to navigate
   * @param keys keys to follow to a value
   * @return string value at location specified by keys wrapped in an optional. if no value there,
   *         empty optional.
   */
  public static String getStringOrNull(final JsonNode json, final List<String> keys) {
    final Optional<JsonNode> optional = getOptional(json, keys);
    return optional.map(JsonNode::asText).orElse(null);
  }

  /**
   * Get the int at a location in a {@link JsonNode} object. Returns 0 if no value at the specified
   * location.
   *
   * @param json object to navigate
   * @param keys keys to follow to a value
   * @return value at location specified by keys wrapped in an optional. if no value there or it is
   *         not an int, returns 0.
   */
  public static int getIntOrZero(final JsonNode json, final String... keys) {
    return getIntOrZero(json, Arrays.asList(keys));
  }

  /**
   * Get the int at a location in a {@link JsonNode} object. Returns 0 if no value at the specified
   * location.
   *
   * @param json object to navigate
   * @param keys keys to follow to a value
   * @return value at location specified by keys wrapped in an optional. if no value there or it is
   *         not an int, returns 0.
   */
  public static int getIntOrZero(final JsonNode json, final List<String> keys) {
    final Optional<JsonNode> optional = getOptional(json, keys);
    return optional.map(JsonNode::asInt).orElse(0);
  }

  /**
   * Flattens an ObjectNode, or dumps it into a {null: value} map if it's not an object. When
   * applyFlattenToArray is true, each element in the array will be one entry in the returned map.
   * This behavior is used in the Redshift SUPER type. When it is false, the whole array will be one
   * entry. This is used in the JobTracker.
   */
  @SuppressWarnings("PMD.ForLoopCanBeForeach")
  public static Map<String, Object> flatten(final JsonNode node, final Boolean applyFlattenToArray) {
    if (node.isObject()) {
      final Map<String, Object> output = new HashMap<>();
      for (final Iterator<Entry<String, JsonNode>> it = node.fields(); it.hasNext();) {
        final Entry<String, JsonNode> entry = it.next();
        final String field = entry.getKey();
        final JsonNode value = entry.getValue();
        mergeMaps(output, field, flatten(value, applyFlattenToArray));
      }
      return output;
    } else if (node.isArray() && applyFlattenToArray) {
      final Map<String, Object> output = new HashMap<>();
      final int arrayLen = node.size();
      for (int i = 0; i < arrayLen; i++) {
        final String field = String.format("[%d]", i);
        final JsonNode value = node.get(i);
        mergeMaps(output, field, flatten(value, applyFlattenToArray));
      }
      return output;
    } else {
      final Object value;
      if (node.isBoolean()) {
        value = node.asBoolean();
      } else if (node.isLong()) {
        value = node.asLong();
      } else if (node.isInt()) {
        value = node.asInt();
      } else if (node.isDouble()) {
        value = node.asDouble();
      } else if (node.isValueNode() && !node.isNull()) {
        value = node.asText();
      } else {
        // Fallback handling for e.g. arrays
        value = node.toString();
      }
      return singletonMap(null, value);
    }
  }

  /**
   * Flattens an ObjectNode, or dumps it into a {null: value} map if it's not an object. New usage of
   * this function is best to explicitly declare the intended array mode. This version is provided for
   * backward compatibility.
   */
  public static Map<String, Object> flatten(final JsonNode node) {
    return flatten(node, false);
  }

  /**
   * Prepend all keys in subMap with prefix, then merge that map into originalMap.
   * <p>
   * If subMap contains a null key, then instead it is replaced with prefix. I.e. {null: value} is
   * treated as {prefix: value} when merging into originalMap.
   */
  public static void mergeMaps(final Map<String, Object> originalMap, final String prefix, final Map<String, Object> subMap) {
    originalMap.putAll(subMap.entrySet().stream().collect(toMap(
        e -> {
          final String key = e.getKey();
          if (key != null) {
            return prefix + "." + key;
          } else {
            return prefix;
          }
        },
        Entry::getValue)));
  }

  /**
   * Convert a {@link JsonNode} as a string-to-string map.
   *
   * @param json to convert
   * @return json as string-to-string map
   */
  public static Map<String, String> deserializeToStringMap(final JsonNode json) {
    return OBJECT_MAPPER.convertValue(json, new TypeReference<>() {});
  }

  /**
   * Convert a {@link JsonNode} as a string-to-string map.
   *
   * @param json to convert
   * @return json as string-to-string map
   */
  public static Map<String, Object> deserializeToMap(final JsonNode json) {
    return OBJECT_MAPPER.convertValue(json, new TypeReference<>() {});
  }

  /**
   * Convert a {@link JsonNode} as a string-to-integer map.
   *
   * @param json to convert
   * @return json as string-to-integer map
   */
  public static Map<String, Integer> deserializeToIntegerMap(final JsonNode json) {
    return OBJECT_MAPPER.convertValue(json, new TypeReference<>() {});
  }

  /**
   * Convert a {@link JsonNode} as a list of string.
   *
   * @param json to convert
   * @return json as list of string
   */
  public static List<String> deserializeToStringList(final JsonNode json) {
    return OBJECT_MAPPER.convertValue(json, new TypeReference<>() {});
  }

  /**
   * By the Jackson DefaultPrettyPrinter prints objects with an extra space as follows: {"name" :
   * "airbyte"}. We prefer {"name": "airbyte"}.
   */
  private static class JsonPrettyPrinter extends DefaultPrettyPrinter {

    // this method has to be overridden because in the superclass it checks that it is an instance of
    // DefaultPrettyPrinter (which is no longer the case in this inherited class).
    @Override
    public DefaultPrettyPrinter createInstance() {
      return new DefaultPrettyPrinter(this);
    }

    // override the method that inserts the extra space.
    @Override
    public DefaultPrettyPrinter withSeparators(final Separators separators) {
      _separators = separators;
      _objectFieldValueSeparatorWithSpaces = separators.getObjectFieldValueSeparator() + " ";
      return this;
    }

  }

  /**
   * Merge updateNode into mainNode Stolen from
   * https://stackoverflow.com/questions/9895041/merging-two-json-documents-using-jackson
   */
  public static JsonNode mergeNodes(final JsonNode mainNode, final JsonNode updateNode) {

    final Iterator<String> fieldNames = updateNode.fieldNames();
    while (fieldNames.hasNext()) {

      final String fieldName = fieldNames.next();
      final JsonNode jsonNode = mainNode.get(fieldName);
      // if field exists and is an embedded object
      if (jsonNode != null && jsonNode.isObject()) {
        mergeNodes(jsonNode, updateNode.get(fieldName));
      } else {
        if (mainNode instanceof ObjectNode) {
          // Overwrite field
          final JsonNode value = updateNode.get(fieldName);
          ((ObjectNode) mainNode).replace(fieldName, value);
        }
      }

    }

    return mainNode;
  }

  /**
   * Sets a nested node to the passed value. Creates nodes on the way if necessary.
   */
  private static void setNested(final JsonNode json, final List<String> keys, final BiConsumer<ObjectNode, String> typedValue) {
    Preconditions.checkArgument(!keys.isEmpty(), "Must pass at least one key");
    final JsonNode nodeContainingFinalKey = navigateToAndCreate(json, keys.subList(0, keys.size() - 1));
    typedValue.accept((ObjectNode) nodeContainingFinalKey, keys.get(keys.size() - 1));
  }

  /**
   * Navigates to a node based on provided nested keys. Creates necessary parent nodes.
   */
  public static JsonNode navigateToAndCreate(JsonNode node, final List<String> keys) {
    for (final String key : keys) {
      final ObjectNode currentNode = (ObjectNode) node;
      node = node.get(key);
      if (node == null || node.isNull()) {
        node = emptyObject();
        currentNode.set(key, node);
      }
    }
    return node;
  }

  /**
   * Set nested value and create parent keys on the way. Copied from our other replaceNestedValue.
   *
   * @param json node
   * @param keys list of keys that you want to nest into
   * @param value node value to set
   */
  public static void setNestedValue(final JsonNode json, final List<String> keys, final JsonNode value) {
    setNested(json, keys, (node, finalKey) -> node.set(finalKey, value));
  }

  /**
   * Serializes an object to a JSON string with keys sorted in alphabetical order.
   *
   * @param object the object to serialize
   * @return the JSON string
   * @throws IOException if there is an error serializing the object
   */
  public static String canonicalJsonSerialize(final Object object) throws IOException {
    final ObjectMapper mapper = new ObjectMapper();
    final ObjectNode objectNode = mapper.valueToTree(object);
    final ObjectNode sortedObjectNode = (ObjectNode) sortProperties(objectNode);
    return mapper.writer().writeValueAsString(sortedObjectNode);
  }

  private static JsonNode sortProperties(final JsonNode jsonNode) {
    if (jsonNode.isObject()) {
      final ObjectNode objectNode = (ObjectNode) jsonNode;
      final ObjectNode sortedObjectNode = JsonNodeFactory.instance.objectNode();

      StreamSupport.stream(Spliterators.spliteratorUnknownSize(objectNode.fields(), Spliterator.ORDERED), false)
          .sorted(Map.Entry.comparingByKey())
          .forEachOrdered(entry -> sortedObjectNode.set(entry.getKey(), sortProperties(entry.getValue())));

      return sortedObjectNode;
    } else if (jsonNode.isArray()) {
      final ArrayNode arrayNode = (ArrayNode) jsonNode;
      final ArrayNode sortedArrayNode = JsonNodeFactory.instance.arrayNode();
      arrayNode.forEach(node -> sortedArrayNode.add(sortProperties(node)));

      return sortedArrayNode;
    } else {
      return jsonNode;
    }
  }

  /**
   * If the supplied object is a TextNode, attempt to deserialize it and return the result. Otherwise,
   * return the object as-is.
   *
   * @param jsonNode the object to deserialize if in text form
   * @return the deserialized JSON object
   * @throws RuntimeException if the jsonNode is text that cannot be deserialized
   */
  public static JsonNode deserializeIfText(final JsonNode jsonNode) {
    if (jsonNode instanceof TextNode) {
      return Jsons.deserialize(jsonNode.asText());
    }
    return jsonNode;
  }

}
