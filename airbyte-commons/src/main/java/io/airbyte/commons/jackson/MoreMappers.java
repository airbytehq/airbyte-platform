/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.jackson;

import com.fasterxml.jackson.core.JsonGenerator.Feature;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fasterxml.jackson.module.kotlin.KotlinModule;

/**
 * The {@link JavaTimeModule} allows mappers to accommodate different varieties of serialised date
 * time strings.
 * <p>
 * All jackson mapper creation should use the following methods for instantiation.
 */
public class MoreMappers {

  /**
   * Init default {@link ObjectMapper}.
   *
   * @return object mapper
   */
  public static ObjectMapper initMapper() {
    return configure(new ObjectMapper());
  }

  /**
   * Configures the {@link ObjectMapper}.
   *
   * @param objectMapper An {@link ObjectMapper}.
   * @return The configured {@link ObjectMapper} if not {@code null}.
   */
  public static ObjectMapper configure(final ObjectMapper objectMapper) {
    if (objectMapper != null) {
      objectMapper
          .enable(MapperFeature.ACCEPT_CASE_INSENSITIVE_ENUMS)
          .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
          .configure(Feature.WRITE_BIGDECIMAL_AS_PLAIN, true)
          .registerModule(new KotlinModule.Builder().build())
          .registerModule(new JavaTimeModule())
          .registerModule(new Jdk8Module());
    }
    return objectMapper;
  }

}
