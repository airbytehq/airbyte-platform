/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.jackson

import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.MapperFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.KotlinModule

/**
 * The [JavaTimeModule] allows mappers to accommodate different varieties of serialised date
 * time strings.
 *
 *
 * All jackson mapper creation should use the following methods for instantiation.
 */
object MoreMappers {
  /**
   * Init default [ObjectMapper].
   *
   * @return object mapper
   */
  @JvmStatic
  fun initMapper(): ObjectMapper = configure(ObjectMapper())

  /**
   * Configures the [ObjectMapper].
   *
   * @param objectMapper An [ObjectMapper].
   * @return The configured [ObjectMapper] if not `null`.
   */
  fun configure(objectMapper: ObjectMapper): ObjectMapper {
    objectMapper
      .enable(MapperFeature.ACCEPT_CASE_INSENSITIVE_ENUMS)
      .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
      .configure(JsonGenerator.Feature.WRITE_BIGDECIMAL_AS_PLAIN, true)
      .registerModule(KotlinModule.Builder().build())
      .registerModule(JavaTimeModule())
      ?.registerModule(Jdk8Module())
    return objectMapper
  }
}
