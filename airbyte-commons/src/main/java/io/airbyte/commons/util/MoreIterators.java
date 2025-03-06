/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.util;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * Shared code for common operations on {@link Iterator}.
 */
public class MoreIterators {

  /**
   * Create a list from an iterator.
   *
   * @param iterator iterator to convert
   * @param <T> type
   * @return list
   */
  public static <T> List<T> toList(final Iterator<T> iterator) {
    final List<T> list = new ArrayList<>();
    while (iterator.hasNext()) {
      list.add(iterator.next());
    }
    return list;
  }

  /**
   * Create a set from an iterator.
   *
   * @param iterator iterator to convert
   * @param <T> type
   * @return set
   */
  public static <T> Set<T> toSet(final Iterator<T> iterator) {
    final Set<T> set = new HashSet<>();
    while (iterator.hasNext()) {
      set.add(iterator.next());
    }
    return set;
  }

}
