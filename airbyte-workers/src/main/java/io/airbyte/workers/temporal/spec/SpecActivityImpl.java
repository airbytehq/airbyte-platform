/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.spec;

import static io.airbyte.featureflag.ContextKt.ANONYMOUS;
import static io.airbyte.metrics.lib.ApmTraceConstants.ACTIVITY_TRACE_OPERATION_NAME;

import datadog.trace.api.Trace;
import io.airbyte.api.client.model.generated.Geography;
import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.logging.LogClientManager;
import io.airbyte.commons.temporal.config.WorkerMode;
import io.airbyte.config.ConnectorJobOutput;
import io.airbyte.config.ConnectorJobOutput.OutputType;
import io.airbyte.featureflag.FeatureFlagClient;
import io.airbyte.featureflag.WorkloadCheckFrequencyInSeconds;
import io.airbyte.featureflag.Workspace;
import io.airbyte.metrics.lib.MetricAttribute;
import io.airbyte.metrics.lib.MetricClient;
import io.airbyte.metrics.lib.MetricTags;
import io.airbyte.metrics.lib.OssMetricsRegistry;
import io.airbyte.workers.exception.WorkerException;
import io.airbyte.workers.models.SpecInput;
import io.airbyte.workers.pod.Metadata;
import io.airbyte.workers.sync.WorkloadClient;
import io.airbyte.workers.workload.WorkloadIdGenerator;
import io.airbyte.workload.api.client.model.generated.WorkloadCreateRequest;
import io.airbyte.workload.api.client.model.generated.WorkloadLabel;
import io.airbyte.workload.api.client.model.generated.WorkloadPriority;
import io.airbyte.workload.api.client.model.generated.WorkloadType;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Singleton;
import java.nio.file.Path;
import java.util.List;
import lombok.extern.slf4j.Slf4j;

/**
 * SpecActivityImpl.
 */
@Slf4j
@Singleton
@Requires(env = WorkerMode.CONTROL_PLANE)
public class SpecActivityImpl implements SpecActivity {

  private final FeatureFlagClient featureFlagClient;
  private final WorkloadClient workloadClient;
  private final WorkloadIdGenerator workloadIdGenerator;
  private final MetricClient metricClient;
  private final LogClientManager logClientManager;

  public SpecActivityImpl(
                          final FeatureFlagClient featureFlagClient,
                          final WorkloadClient workloadClient,
                          final WorkloadIdGenerator workloadIdGenerator,
                          final MetricClient metricClient,
                          final LogClientManager logClientManager) {
    this.featureFlagClient = featureFlagClient;
    this.workloadClient = workloadClient;
    this.workloadIdGenerator = workloadIdGenerator;
    this.metricClient = metricClient;
    this.logClientManager = logClientManager;
  }

  @Trace(operationName = ACTIVITY_TRACE_OPERATION_NAME)
  @Override
  public ConnectorJobOutput runWithWorkload(final SpecInput input) throws WorkerException {
    final String jobId = input.getJobRunConfig().getJobId();
    final String workloadId =
        workloadIdGenerator.generateSpecWorkloadId(jobId);
    final String serializedInput = Jsons.serialize(input);

    final WorkloadCreateRequest workloadCreateRequest = new WorkloadCreateRequest(
        workloadId,
        List.of(new WorkloadLabel(Metadata.JOB_LABEL_KEY, jobId)),
        serializedInput,
        logClientManager.fullLogPath(Path.of(workloadId)),
        Geography.AUTO.getValue(),
        WorkloadType.SPEC,
        WorkloadPriority.HIGH,
        null,
        null,
        null);

    workloadClient.createWorkload(workloadCreateRequest);

    final int checkFrequencyInSeconds =
        featureFlagClient.intVariation(WorkloadCheckFrequencyInSeconds.INSTANCE, new Workspace(ANONYMOUS));
    workloadClient.waitForWorkload(workloadId, checkFrequencyInSeconds);

    return workloadClient.getConnectorJobOutput(
        workloadId,
        failureReason -> new ConnectorJobOutput()
            .withOutputType(OutputType.SPEC)
            .withSpec(null)
            .withFailureReason(failureReason));
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
