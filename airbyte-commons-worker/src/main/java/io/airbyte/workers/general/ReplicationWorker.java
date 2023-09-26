/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.general;

import io.airbyte.config.ReplicationOutput;
import io.airbyte.persistence.job.models.ReplicationInput;
import io.airbyte.workers.Worker;

/**
 * Replication Worker. Calls the read method and write methods on a source and destination
 * respectively.
 */
public interface ReplicationWorker extends Worker<ReplicationInput, ReplicationOutput> {}
