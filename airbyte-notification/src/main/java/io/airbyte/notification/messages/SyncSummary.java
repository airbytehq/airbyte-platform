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

  private long recordsFilteredOut;

  private long bytesFilteredOut;

  private String errorMessage;

  private static String formatVolume(final long bytes) {
    long currentValue = bytes;
    for (String unit : List.of("B", "kB", "MB", "GB")) {
      var byteLimit = 1024;
      if (currentValue < byteLimit) {
        return String.format("%d %s", currentValue, unit);
      }
      currentValue = currentValue / byteLimit;
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

  public Long getDurationInSeconds() {
    if (startedAt != null && finishedAt != null) {
      return Duration.between(startedAt, finishedAt).getSeconds();
    }
    return null;
  }

  public String getDurationFormatted() {
    if (startedAt != null && finishedAt != null) {
      return formatDuration(startedAt, finishedAt);
    }
    return null;
  }

  public String getBytesEmittedFormatted() {
    return formatVolume(bytesEmitted);
  }

  public String getBytesCommittedFormatted() {
    return formatVolume(bytesCommitted);
  }

}
