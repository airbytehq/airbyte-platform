/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.list;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/**
 * Shared code for operating on {@link List}.
 */
public class Lists {

  /**
   * Combines multiple lists into a single unmodifiable list.
   *
   * @param lists the lists to combine
   * @param <T> the {@code List}'s element type
   * @return a single unmodifiable {@code List} from combining {@code lists}
   */
  @SafeVarargs
  public static <T> List<T> concat(final List<T>... lists) {
    return Arrays.stream(lists).flatMap(Collection::stream)
        .toList();
  }

}
