/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.internal.book_keeping;

import java.util.Objects;

/**
 * POJO for Stats counters to avoid using tuples.
 */
public class StatsCounters {

  Long bytesCount;
  Long recordCount;

  public StatsCounters() {
    this(0L, 0L);
  }

  public StatsCounters(final long bytesCount, final long recordCount) {
    this.bytesCount = bytesCount;
    this.recordCount = recordCount;
  }

  public Long getBytesCount() {
    return bytesCount;
  }

  public Long getRecordCount() {
    return recordCount;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    StatsCounters that = (StatsCounters) o;
    return Objects.equals(bytesCount, that.bytesCount) && Objects.equals(recordCount, that.recordCount);
  }

  @Override
  public int hashCode() {
    return Objects.hash(bytesCount, recordCount);
  }

}
