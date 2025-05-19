/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.storage.activities

class NaiveEqualityComparator<T> : Comparator<T> {
  override fun compare(
    o1: T?,
    o2: T?,
  ): Int = if (o1 == o2) 0 else 1
}
