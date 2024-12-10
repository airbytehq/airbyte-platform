/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.spec;

import static io.airbyte.featureflag.ContextKt.ANONYMOUS;
import static io.airbyte.metrics.lib.ApmTraceConstants.ACTIVITY_TRACE_OPERATION_NAME;

import datadog.trace.api.Trace;
import io.airbyte.commons.micronaut.EnvConstants;
import io.airbyte.config.ConnectorJobOutput;
import io.airbyte.featureflag.FeatureFlagClient;
import io.airbyte.featureflag.WorkloadCheckFrequencyInSeconds;
import io.airbyte.featureflag.Workspace;
import io.airbyte.metrics.lib.MetricAttribute;
import io.airbyte.metrics.lib.MetricClient;
import io.airbyte.metrics.lib.MetricTags;
import io.airbyte.metrics.lib.OssMetricsRegistry;
import io.airbyte.workers.commands.SpecCommand;
import io.airbyte.workers.exception.WorkerException;
import io.airbyte.workers.models.SpecInput;
import io.airbyte.workers.sync.WorkloadClient;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Singleton;

/**
 * SpecActivityImpl.
 */
@Singleton
@Requires(env = EnvConstants.CONTROL_PLANE)
public class SpecActivityImpl implements SpecActivity {

  private final SpecCommand specCommand;
  private final FeatureFlagClient featureFlagClient;
  private final WorkloadClient workloadClient;
  private final MetricClient metricClient;

  public SpecActivityImpl(
                          final SpecCommand specCommand,
                          final FeatureFlagClient featureFlagClient,
                          final WorkloadClient workloadClient,
                          final MetricClient metricClient) {
    this.specCommand = specCommand;
    this.featureFlagClient = featureFlagClient;
    this.workloadClient = workloadClient;
    this.metricClient = metricClient;
  }

  @Trace(operationName = ACTIVITY_TRACE_OPERATION_NAME)
  @Override
  public ConnectorJobOutput runWithWorkload(final SpecInput input) throws WorkerException {
    final String workloadId = specCommand.start(new SpecInput(input.getJobRunConfig(), input.getLauncherConfig()), null);

    final int checkFrequencyInSeconds =
        featureFlagClient.intVariation(WorkloadCheckFrequencyInSeconds.INSTANCE, new Workspace(ANONYMOUS));
    workloadClient.waitForWorkload(workloadId, checkFrequencyInSeconds);

    return specCommand.getOutput(workloadId);
  }

  @Override
  public void reportSuccess() {
    metricClient.count(OssMetricsRegistry.SPEC, 1, new MetricAttribute(MetricTags.STATUS, "success"));
  }

  @Override
  public void reportFailure() {
    metricClient.count(OssMetricsRegistry.SPEC, 1, new MetricAttribute(MetricTags.STATUS, "failed"));
  }

}
