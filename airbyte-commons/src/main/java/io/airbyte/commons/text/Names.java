/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.text;

import com.google.common.base.Preconditions;
import java.text.Normalizer;

/**
 * Shared code for interacting with names in strings. Usually used for SQL strings. e.g. adding
 * proper quoting.
 */
public class Names {

  private static final String NON_ALPHANUMERIC_AND_UNDERSCORE_PATTERN = "[^\\p{Alnum}_]";

  /**
   * Converts any UTF8 string to a string with only alphanumeric and _ characters without preserving
   * accent characters.
   *
   * @param s string to convert
   * @return cleaned string
   */
  public static String toAlphanumericAndUnderscore(final String s) {
    return Normalizer.normalize(s, Normalizer.Form.NFKD)
        .replaceAll("\\p{M}", "") // P{M} matches a code point that is not a combining mark (unicode)
        .replaceAll("\\s+", "_")
        .replaceAll(NON_ALPHANUMERIC_AND_UNDERSCORE_PATTERN, "_");
  }

  /**
   * Wrap a string in single quotes.
   *
   * @param value to wrap
   * @return value wrapped in single quotes.
   */
  public static String singleQuote(final String value) {
    return internalQuote(value, '\'');
  }

  private static String internalQuote(final String value, final char quoteChar) {
    Preconditions.checkNotNull(value);

    final boolean startsWithChar = value.charAt(0) == quoteChar;
    final boolean endsWithChar = value.charAt(value.length() - 1) == quoteChar;

    Preconditions.checkState(startsWithChar == endsWithChar, "Invalid value: %s", value);

    if (startsWithChar) {
      return value;
    } else {
      return String.format("%c%s%c", quoteChar, value, quoteChar);
    }
  }

}
