/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.general;

import io.airbyte.config.ConnectorJobOutput;
import io.airbyte.config.StandardCheckConnectionInput;
import io.airbyte.workers.Worker;

/**
 * Check Connection Worker. Worker that calls check on a connector.
 */
public interface CheckConnectionWorker extends Worker<StandardCheckConnectionInput, ConnectorJobOutput> {}
