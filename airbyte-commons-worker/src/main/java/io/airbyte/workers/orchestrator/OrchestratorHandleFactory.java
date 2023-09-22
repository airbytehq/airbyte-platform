/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.orchestrator;

import io.airbyte.commons.functional.CheckedSupplier;
import io.airbyte.config.ReplicationOutput;
import io.airbyte.persistence.job.models.IntegrationLauncherConfig;
import io.airbyte.persistence.job.models.JobRunConfig;
import io.airbyte.persistence.job.models.ReplicationInput;
import io.airbyte.workers.Worker;
import io.temporal.activity.ActivityExecutionContext;
import java.util.function.Supplier;

/**
 * Factory for building OrchestratorHandles.
 * <p>
 * An OrchestratorHandle is the piece that hosts the ReplicationWorker which is the centerpiece of
 * replication. Note, the OrchestratorHandle is currently implemented as a CheckedSupplier.
 */
public interface OrchestratorHandleFactory {

  CheckedSupplier<Worker<ReplicationInput, ReplicationOutput>, Exception> create(final IntegrationLauncherConfig sourceLauncherConfig,
                                                                                 final IntegrationLauncherConfig destinationLauncherConfig,
                                                                                 final JobRunConfig jobRunConfig,
                                                                                 final ReplicationInput replicationInput,
                                                                                 // This shouldn't be here but it is required for the Kube path.
                                                                                 final Supplier<ActivityExecutionContext> activityContext);

}
