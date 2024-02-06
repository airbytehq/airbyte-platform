/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.notification.messages;

import java.time.Instant;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@Builder
public class SyncSummary {

  private WorkspaceInfo workspace;

  private ConnectionInfo connectionInfo;

  private SourceInfo sourceInfo;

  private DestinationInfo destinationInfo;

  private Long jobId;

  private boolean isSuccess;

  private Instant startedAt;

  private Instant finishedAt;

  private long bytesEmitted;

  private long bytesCommitted;

  private long rowsEmitted;

  private long rowsCommitted;

  private String errorMessage;

}
