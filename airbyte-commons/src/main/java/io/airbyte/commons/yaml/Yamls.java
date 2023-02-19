/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.yaml;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SequenceWriter;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import com.fasterxml.jackson.dataformat.yaml.YAMLParser;
import com.google.common.collect.AbstractIterator;
import io.airbyte.commons.jackson.MoreMappers;
import io.airbyte.commons.lang.CloseableConsumer;
import io.airbyte.commons.lang.Exceptions;
import io.airbyte.commons.util.AutoCloseableIterator;
import io.airbyte.commons.util.AutoCloseableIterators;
import java.io.IOException;
import java.io.InputStream;
import java.io.Writer;
import java.util.Iterator;

/**
 * Shared code for interacting with Yaml and Yaml-representation of Jackson {@link JsonNode}.
 */
@SuppressWarnings("PMD.AvoidBranchingStatementAsLastInLoop")
public class Yamls {

  private static final YAMLFactory YAML_FACTORY = new YAMLFactory();
  private static final ObjectMapper OBJECT_MAPPER = MoreMappers.initYamlMapper(YAML_FACTORY);

  private static final YAMLFactory YAML_FACTORY_WITHOUT_QUOTES = new YAMLFactory().enable(YAMLGenerator.Feature.MINIMIZE_QUOTES);
  private static final ObjectMapper OBJECT_MAPPER_WITHOUT_QUOTES = MoreMappers.initYamlMapper(YAML_FACTORY_WITHOUT_QUOTES);

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
   * Deserialize an {@link InputStream} from a list of Yaml.
   *
   * @param stream whose contents to deserialize
   * @return stream as an {@link AutoCloseableIterator}
   */
  public static AutoCloseableIterator<JsonNode> deserializeArray(final InputStream stream) {
    try {
      final YAMLParser parser = YAML_FACTORY.createParser(stream);

      // Check the first token
      if (parser.nextToken() != JsonToken.START_ARRAY) {
        throw new IllegalStateException("Expected content to be an array");
      }

      final Iterator<JsonNode> iterator = new AbstractIterator<>() {

        @Override
        protected JsonNode computeNext() {
          try {
            while (parser.nextToken() != JsonToken.END_ARRAY) {
              return parser.readValueAsTree();
            }
          } catch (final IOException e) {
            throw new RuntimeException(e);
          }
          return endOfData();
        }

      };

      return AutoCloseableIterators.fromIterator(iterator, parser::close);

    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }

  // todo (cgardens) - share this with Jsons if ever needed.

  /**
   * Creates a consumer that writes list items to the writer in a streaming fashion.
   *
   * @param writer writer to write to
   * @param <T> type of items being written
   * @return consumer that is able to write element to a list element by element. must be closed!
   */
  public static <T> CloseableConsumer<T> listWriter(final Writer writer) {
    return new YamlConsumer<>(writer, OBJECT_MAPPER);
  }

  /**
   * Yaml Consumer for reading large yaml lists incrementally (so that they don't fully need to be in
   * memory all at once).
   *
   * @param <T> type value being consumed
   */
  public static class YamlConsumer<T> implements CloseableConsumer<T> {

    private final SequenceWriter sequenceWriter;

    public YamlConsumer(final Writer writer, final ObjectMapper objectMapper) {
      this.sequenceWriter = Exceptions.toRuntime(() -> objectMapper.writer().writeValuesAsArray(writer));

    }

    @Override
    public void accept(final T t) {
      try {
        sequenceWriter.write(t);
      } catch (final IOException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public void close() throws Exception {
      // closing the SequenceWriter closes the Writer that it wraps.
      sequenceWriter.close();
    }

  }

}
