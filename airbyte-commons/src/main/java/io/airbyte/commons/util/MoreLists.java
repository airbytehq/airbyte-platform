/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.util;

import com.google.common.base.Preconditions;
import java.util.List;
import java.util.stream.Stream;

/**
 * Shared code for common operations on lists.
 */
public class MoreLists {

  /**
   * Concatenate multiple lists into one list.
   *
   * @param lists to concatenate
   * @param <T> type
   * @return a new concatenated list
   */
  @SafeVarargs
  public static <T> List<T> concat(final List<T>... lists) {
    return Stream.of(lists).flatMap(List::stream).toList();
  }

  /**
   * Get the value at an index or null if the index is out of bounds.
   *
   * @param list list to extract from
   * @param index index to extra
   * @param <T> type of the value in the list
   * @return extract value at index or null if index is out of bounds.
   */
  public static <T> T getOrNull(final List<T> list, final int index) {
    Preconditions.checkNotNull(list);
    if (list.size() > index) {
      return list.get(index);
    } else {
      return null;
    }
  }

}
