/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.ksp.config

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource
import org.yaml.snakeyaml.Yaml

internal class SymbolProcessingUtilsTest {
  private val yaml = Yaml()

  @ParameterizedTest
  @CsvSource(
    value = [
      "boolean,true,true",
      "double,5.0,5.0",
      "duration,PT1M,PT1M",
      "float,1.0F,1.0",
      "float,1.0f,1.0",
      "int,7,7",
      "integer,8,8",
      "list,emptyList(),[]",
      "list,'listOf(1,2,3)','1,2,3'",
      "long,10L,10",
      "long,10l,10",
      "path,/some/path,/some/path",
      "uuid,8119dc85-7aee-4524-82fa-631a4894bfb9,8119dc85-7aee-4524-82fa-631a4894bfb9",
      "unknown,some value,some value",
    ],
  )
  fun testStringToTypeConversion(
    type: String,
    value: String,
    expectedValue: Any,
  ) {
    assertEquals(expectedValue, yaml.dump(convertStringToType(type = type, value = value)).trim())
  }
}
