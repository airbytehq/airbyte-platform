/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.util;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

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

}
