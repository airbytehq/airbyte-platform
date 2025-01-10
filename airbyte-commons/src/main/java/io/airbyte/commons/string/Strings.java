/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.string;

import com.google.common.collect.Streams;
import java.util.stream.Collectors;
import org.apache.commons.lang3.RandomStringUtils;

/**
 * Shared code for interacting with {@link String}.
 */
public class Strings {

  /**
   * Join the contents of an iterable into a string with a separator.
   *
   * @param iterable to join
   * @param separator for each value
   * @return string representation of the iterable
   */
  public static String join(final Iterable<?> iterable, final CharSequence separator) {
    return Streams.stream(iterable)
        .map(Object::toString)
        .collect(Collectors.joining(separator));
  }

  /**
   * Add a random suffix to a string.
   *
   * @param base string to add suffix to.
   * @param separator between input string and random suffix
   * @param suffixLength length of random suffix
   * @return generated string
   */
  public static String addRandomSuffix(final String base, final String separator, final int suffixLength) {
    return base + separator + RandomStringUtils.randomAlphabetic(suffixLength).toLowerCase();
  }

}
