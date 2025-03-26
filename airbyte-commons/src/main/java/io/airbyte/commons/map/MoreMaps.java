/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.map;

import com.google.common.base.Preconditions;
import java.util.HashMap;
import java.util.Map;

/**
 * Shared code for operating on {@link Map}.
 */
public class MoreMaps {

  /**
   * Combine the contents of multiple maps. In the event of duplicate keys, the contents of maps later
   * in the input args overwrite those earlier in the list.
   *
   * @param maps whose contents to combine
   * @param <K> type of key
   * @param <V> type of value
   * @return map with contents of input maps
   */
  @SafeVarargs
  public static <K, V> Map<K, V> merge(final Map<K, V>... maps) {
    final Map<K, V> outputMap = new HashMap<>();

    for (final Map<K, V> map : maps) {
      Preconditions.checkNotNull(map);
      outputMap.putAll(map);
    }

    return outputMap;
  }

}
