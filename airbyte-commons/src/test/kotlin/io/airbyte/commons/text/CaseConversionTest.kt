/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.text

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource

internal class CaseConversionTest {
  @ParameterizedTest
  @CsvSource(
    value = [
      "some-test-data,SomeTestData",
      "some-testdata,SomeTestdata",
      "someTestData,SomeTestData",
      "some--test-data,Some-TestData",
      "-some-test-data-,SomeTestData-",
      "some,Some",
      "'',''",
    ],
  )
  fun `should convert lower hyphen case to upper camel case`(
    value: String,
    expected: String,
  ) {
    assertEquals(expected, value.lowerHyphenToUpperCamel())
  }

  @ParameterizedTest
  @CsvSource(
    value = [
      "someTestData,some-test-data",
      "someTestdata,some-testdata",
      "some-test-data,some-test-data",
      "some,some",
      "'',''",
    ],
  )
  fun `should convert lower camel case to lower hyphen case`(
    value: String,
    expected: String,
  ) {
    assertEquals(expected, value.lowerCamelToLowerHyphen())
  }

  @ParameterizedTest
  @CsvSource(
    value = [
      "some_test_data,someTestData",
      "some_testdata,someTestData",
      "someTestData,someTestData",
      "some,some",
      "'',''",
    ],
  )
  fun `should convert lower underscore case to lower camel case`() {
    val lowerUnderscore = "some_test_data"
    assertEquals("someTestData", lowerUnderscore.lowerUnderscoreToLowerCamel())
  }
}
