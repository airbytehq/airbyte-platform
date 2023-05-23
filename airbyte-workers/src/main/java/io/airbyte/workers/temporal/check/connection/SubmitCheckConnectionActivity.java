/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.check.connection;

import io.airbyte.config.ConnectorJobOutput;
import io.temporal.activity.ActivityInterface;
import java.util.UUID;

/**
 * Temporal activity to submit a check_connection request to airbyte server.
 */
@ActivityInterface
public interface SubmitCheckConnectionActivity {

  /**
   * Submits an API request to airbyte server to run check connection for source.
   */
  ConnectorJobOutput submitCheckConnectionToSource(final UUID sourceId);

  /**
   * Submits an API request to airbyte server to run check connection for destination.
   */
  ConnectorJobOutput submitCheckConnectionToDestination(final UUID destinationId);

}
