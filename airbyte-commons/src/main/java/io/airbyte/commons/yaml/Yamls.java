/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.yaml;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.io.File;
import java.io.IOException;

/**
 * Shared code for interacting with Yaml and Yaml-representation of Jackson {@link JsonNode}.
 */
@SuppressWarnings("PMD.AvoidBranchingStatementAsLastInLoop")
public class Yamls {

  private static final YAMLFactory YAML_FACTORY = new YAMLFactory();
  private static final ObjectMapper OBJECT_MAPPER = initYamlMapper(YAML_FACTORY);

  private static final YAMLFactory YAML_FACTORY_WITHOUT_QUOTES = new YAMLFactory().enable(YAMLGenerator.Feature.MINIMIZE_QUOTES);
  private static final ObjectMapper OBJECT_MAPPER_WITHOUT_QUOTES = initYamlMapper(YAML_FACTORY_WITHOUT_QUOTES);

  /**
   * Serialize object to YAML string. String values WILL be wrapped in double quotes.
   *
   * @param object - object to serialize
   * @return YAML string version of object
   */
  public static <T> String serialize(final T object) {
    try {
      return OBJECT_MAPPER.writeValueAsString(object);
    } catch (final JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Serialize object to YAML string. String values will NOT be wrapped in double quotes.
   *
   * @param object - object to serialize
   * @return YAML string version of object
   */
  public static String serializeWithoutQuotes(final Object object) {
    try {
      return OBJECT_MAPPER_WITHOUT_QUOTES.writeValueAsString(object);
    } catch (final JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Deserialize a Yaml string to an object with a type.
   *
   * @param yamlString to deserialize
   * @param klass of object
   * @param <T> type of object
   * @return deserialized string as type declare in klass
   */
  public static <T> T deserialize(final String yamlString, final Class<T> klass) {
    try {
      return OBJECT_MAPPER.readValue(yamlString, klass);
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Deserialize a Yaml string to an object with a type.
   *
   * @param yamlString to deserialize
   * @param typeReference of object
   * @param <T> type of object
   * @return deserialized string as type declare in valueTypeRef
   */
  public static <T> T deserialize(final String yamlString, final TypeReference<T> typeReference) {
    try {
      return OBJECT_MAPPER.readValue(yamlString, typeReference);
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Deserialize a JSON string to a {@link JsonNode}.
   *
   * @param yamlString to deserialize
   * @return JSON as JsonNode
   */
  public static JsonNode deserialize(final String yamlString) {
    try {
      return OBJECT_MAPPER.readTree(yamlString);
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Deserialize a file containing yaml to a {@link JsonNode}.
   *
   * @param file to deserialize
   * @return JSON as JsonNode
   */
  public static JsonNode deserialize(final File file) {
    try {
      return OBJECT_MAPPER.readTree(file);
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Init default yaml {@link ObjectMapper}.
   *
   * @param factory yaml object mapper factory
   * @return object mapper
   */
  private static ObjectMapper initYamlMapper(final YAMLFactory factory) {
    return new ObjectMapper(factory).registerModule(new JavaTimeModule());
  }

}
