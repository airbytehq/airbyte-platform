/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.notification.messages;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@Builder
public class SyncSummary {

  private WorkspaceInfo workspace;

  private ConnectionInfo connection;

  private SourceInfo source;

  private DestinationInfo destination;

  private Long jobId;

  private boolean isSuccess;

  private Instant startedAt;

  private Instant finishedAt;

  private long bytesEmitted;

  private long bytesCommitted;

  private long recordsEmitted;

  private long recordsCommitted;

  private String errorMessage;

  private static String formatVolume(final long bytes) {
    long currentValue = bytes;
    for (String unit : List.of("B", "kB", "MB", "GB")) {
      if (currentValue < 1024) {
        return String.format("%d %s", currentValue, unit);
      }
      currentValue = currentValue / 1024;
    }
    return String.format("%d TB", currentValue);
  }

  private static String formatDuration(final Instant start, final Instant end) {
    Duration duration = Duration.between(start, end);
    if (duration.toMinutes() == 0) {
      return String.format("%d sec", duration.toSecondsPart());
    } else if (duration.toHours() == 0) {
      return String.format("%d min %d sec", duration.toMinutesPart(), duration.toSecondsPart());
    } else if (duration.toDays() == 0) {
      return String.format("%d hours %d min", duration.toHoursPart(), duration.toMinutesPart());
    }
    return String.format("%d days %d hours", duration.toDays(), duration.toHoursPart());
  }

  public long getDurationInSeconds() {
    return Duration.between(startedAt, finishedAt).getSeconds();
  }

  public String getDurationFormatted() {
    return formatDuration(startedAt, finishedAt);
  }

  public String getBytesEmittedFormatted() {
    return formatVolume(bytesEmitted);
  }

  public String getBytesCommittedFormatted() {
    return formatVolume(bytesCommitted);
  }

}
