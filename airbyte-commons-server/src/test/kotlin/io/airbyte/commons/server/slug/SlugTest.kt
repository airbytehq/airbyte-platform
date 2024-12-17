package io.airbyte.commons.server.slug

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource

class SlugTest {
  @ParameterizedTest
  @CsvSource(
    "airbyte, airbyte",
    "Cole's Test, cole-s-test",
    "&&#$*)&hajskfjhj uadh .; h8849r7, hajskfjhj-uadh-h8849r7",
    "'', ''",
    "ßtest √ß√√ test, sstest-ss-test",
  )
  fun `verify slug`(
    input: String,
    expected: String,
  ) {
    assertEquals(expected, slugify(input))
  }
}
