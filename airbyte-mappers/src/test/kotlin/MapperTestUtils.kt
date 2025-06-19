/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

import com.fasterxml.jackson.core.JsonGenerator.Feature
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter
import com.fasterxml.jackson.core.util.Separators
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.MapperFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.ObjectWriter
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.KotlinModule

val TEST_OBJECT_MAPPER: ObjectMapper =
  ObjectMapper()
    .enable(MapperFeature.ACCEPT_CASE_INSENSITIVE_ENUMS)
    .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    .configure(Feature.WRITE_BIGDECIMAL_AS_PLAIN, true)
    .registerModule(KotlinModule.Builder().build())
    .registerModule(JavaTimeModule())
    .registerModule(Jdk8Module())

val TEST_PRETTY_OBJECT_WRITER: ObjectWriter = TEST_OBJECT_MAPPER.writer(JsonPrettyPrinter())

fun toPrettyJsonString(value: Any): String = "${TEST_PRETTY_OBJECT_WRITER.writeValueAsString(value)}\n"

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
    _objectFieldValueSeparatorWithSpaces = separators.getObjectFieldValueSeparator().toString() + " "
    return this
  }
}
