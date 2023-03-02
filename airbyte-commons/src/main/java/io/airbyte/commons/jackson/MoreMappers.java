/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.jackson;

import com.fasterxml.jackson.core.JsonGenerator.Feature;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

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
    final ObjectMapper result = new ObjectMapper().registerModule(new JavaTimeModule());
    result.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    result.configure(Feature.WRITE_BIGDECIMAL_AS_PLAIN, true);
    return result;
  }

}
