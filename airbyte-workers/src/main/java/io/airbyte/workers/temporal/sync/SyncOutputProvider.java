/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.sync;

import io.airbyte.config.ActorType;
import io.airbyte.config.FailureReason.FailureType;
import io.airbyte.config.StandardSyncOutput;
import io.airbyte.config.StandardSyncSummary;
import io.airbyte.config.SyncStats;
import io.airbyte.workers.temporal.FailureConverter;
import java.util.List;

/**
 * Static helpers for handling sync output.
 */
public class SyncOutputProvider {

  public static final StandardSyncSummary EMPTY_FAILED_SYNC = new StandardSyncSummary()
      .withStatus(StandardSyncSummary.ReplicationStatus.FAILED)
      .withStartTime(System.currentTimeMillis())
      .withEndTime(System.currentTimeMillis())
      .withRecordsSynced(0L)
      .withBytesSynced(0L)
      .withTotalStats(new SyncStats()
          .withRecordsEmitted(0L)
          .withBytesEmitted(0L)
          .withSourceStateMessagesEmitted(0L)
          .withDestinationStateMessagesEmitted(0L)
          .withRecordsCommitted(0L));

  /**
   * Get refresh schema failure.
   *
   * @param e exception that caused the failure
   * @return sync output
   */
  public static StandardSyncOutput getRefreshSchemaFailure(final Exception e) {
    final var failure = new FailureConverter().getFailureReason("Refresh Schema", ActorType.SOURCE, e);
    if (failure.getFailureType() == null) {
      failure.setFailureType(FailureType.REFRESH_SCHEMA);
    }
    return new StandardSyncOutput()
        .withFailures(List.of(failure))
        .withStandardSyncSummary(EMPTY_FAILED_SYNC);
  }

}
