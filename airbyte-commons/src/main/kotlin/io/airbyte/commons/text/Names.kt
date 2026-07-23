/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.text

import java.text.Normalizer

/**
 * Shared code for interacting with names in strings. Usually used for SQL strings. e.g. adding
 * proper quoting.
 */
object Names {
  private const val NON_ALPHANUMERIC_AND_UNDERSCORE_PATTERN = "[^\\p{Alnum}_]"

  /**
   * Converts any UTF8 string to a string with only alphanumeric and _ characters without preserving
   * accent characters.
   *
   * @param s string to convert
   * @return cleaned string
   */
  @JvmStatic
  fun toAlphanumericAndUnderscore(s: String): String =
    Normalizer
      .normalize(s, Normalizer.Form.NFKD)
      .replace("\\p{M}".toRegex(), "") // P{M} matches a code point that is not a combining mark (unicode)
      .replace("\\s+".toRegex(), "_")
      .replace(NON_ALPHANUMERIC_AND_UNDERSCORE_PATTERN.toRegex(), "_")

  /**
   * Wrap a string in single quotes.
   *
   * @param value to wrap
   * @return value wrapped in single quotes.
   */
  @JvmStatic
  fun singleQuote(value: String): String = internalQuote(value, '\'')

  private fun internalQuote(
    value: String,
    quoteChar: Char,
  ): String {
    requireNotNull(value)

    val startsWithChar = value[0] == quoteChar
    val endsWithChar = value[value.length - 1] == quoteChar

    check(startsWithChar == endsWithChar) { "Invalid value: $value" }

    return if (startsWithChar) {
      value
    } else {
      "$quoteChar$value$quoteChar"
    }
  }
}
