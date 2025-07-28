/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.yaml

import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.core.type.TypeReference
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import java.io.File
import java.io.IOException

/**
 * Shared code for interacting with Yaml and Yaml-representation of Jackson [JsonNode].
 */
object Yamls {
  private val YAML_FACTORY = YAMLFactory()
  private val OBJECT_MAPPER = initYamlMapper(YAML_FACTORY)

  private val YAML_FACTORY_WITHOUT_QUOTES: YAMLFactory = YAMLFactory().enable(YAMLGenerator.Feature.MINIMIZE_QUOTES)
  private val OBJECT_MAPPER_WITHOUT_QUOTES = initYamlMapper(YAML_FACTORY_WITHOUT_QUOTES)

  /**
   * Serialize object to YAML string. String values WILL be wrapped in double quotes.
   *
   * @param object - object to serialize
   * @return YAML string version of object
   */
  @JvmStatic
  fun <T> serialize(`object`: T): String {
    try {
      return OBJECT_MAPPER.writeValueAsString(`object`)
    } catch (e: JsonProcessingException) {
      throw RuntimeException(e)
    }
  }

  /**
   * Serialize object to YAML string. String values will NOT be wrapped in double quotes.
   *
   * @param object - object to serialize
   * @return YAML string version of object
   */
  @JvmStatic
  fun serializeWithoutQuotes(`object`: Any?): String {
    try {
      return OBJECT_MAPPER_WITHOUT_QUOTES.writeValueAsString(`object`)
    } catch (e: JsonProcessingException) {
      throw RuntimeException(e)
    }
  }

  /**
   * Deserialize a Yaml string to an object with a type.
   *
   * @param yamlString to deserialize
   * @param klass of object
   * @param <T> type of object
   * @return deserialized string as type declare in klass
   </T> */
  @JvmStatic
  fun <T> deserialize(
    yamlString: String?,
    klass: Class<T>?,
  ): T {
    try {
      return OBJECT_MAPPER.readValue(yamlString, klass)
    } catch (e: IOException) {
      throw RuntimeException(e)
    }
  }

  /**
   * Deserialize a Yaml string to an object with a type.
   *
   * @param yamlString to deserialize
   * @param typeReference of object
   * @param <T> type of object
   * @return deserialized string as type declare in valueTypeRef
   </T> */
  fun <T> deserialize(
    yamlString: String?,
    typeReference: TypeReference<T>?,
  ): T {
    try {
      return OBJECT_MAPPER.readValue(yamlString, typeReference)
    } catch (e: IOException) {
      throw RuntimeException(e)
    }
  }

  /**
   * Deserialize a JSON string to a [JsonNode].
   *
   * @param yamlString to deserialize
   * @return JSON as JsonNode
   */
  @JvmStatic
  fun deserialize(yamlString: String?): JsonNode {
    try {
      return OBJECT_MAPPER.readTree(yamlString)
    } catch (e: IOException) {
      throw RuntimeException(e)
    }
  }

  /**
   * Deserialize a file containing yaml to a [JsonNode].
   *
   * @param file to deserialize
   * @return JSON as JsonNode
   */
  fun deserialize(file: File?): JsonNode {
    try {
      return OBJECT_MAPPER.readTree(file)
    } catch (e: IOException) {
      throw RuntimeException(e)
    }
  }

  /**
   * Init default yaml [ObjectMapper].
   *
   * @param factory yaml object mapper factory
   * @return object mapper
   */
  private fun initYamlMapper(factory: YAMLFactory): ObjectMapper = ObjectMapper(factory).registerModule(JavaTimeModule())
}
