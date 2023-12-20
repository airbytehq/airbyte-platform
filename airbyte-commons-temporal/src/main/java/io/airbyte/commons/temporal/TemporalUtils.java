/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.temporal;

import io.airbyte.commons.lang.Exceptions;
import io.airbyte.commons.temporal.config.TemporalSdkTimeouts;
import io.airbyte.commons.temporal.factories.TemporalCloudConfig;
import io.airbyte.commons.temporal.factories.TemporalSelfHostedConfig;
import io.airbyte.commons.temporal.factories.WorkflowServiceStubsFactory;
import io.airbyte.commons.temporal.factories.WorkflowServiceStubsTimeouts;
import io.airbyte.config.helpers.LogClientSingleton;
import io.airbyte.persistence.job.models.JobRunConfig;
import io.micronaut.context.annotation.Property;
import io.micronaut.context.annotation.Value;
import io.temporal.api.namespace.v1.NamespaceConfig;
import io.temporal.api.namespace.v1.NamespaceInfo;
import io.temporal.api.workflowservice.v1.DescribeNamespaceRequest;
import io.temporal.api.workflowservice.v1.UpdateNamespaceRequest;
import io.temporal.serviceclient.WorkflowServiceStubs;
import io.temporal.serviceclient.WorkflowServiceStubsOptions;
import jakarta.inject.Singleton;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Objects;
import java.util.function.Supplier;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.time.DurationFormatUtils;

/**
 * Temporal Utility functions.
 */
// todo (cgardens) - rename? utils implies it's static utility function
@Slf4j
@Singleton
public class TemporalUtils {

  private static final Duration WAIT_INTERVAL = Duration.ofSeconds(2);
  private static final Duration MAX_TIME_TO_CONNECT = Duration.ofMinutes(2);
  private static final Duration WAIT_TIME_AFTER_CONNECT = Duration.ofSeconds(5);
  public static final String DEFAULT_NAMESPACE = "default";
  private static final double REPORT_INTERVAL_SECONDS = 120.0;

  private final TemporalCloudConfig temporalCloudConfig;
  private final WorkflowServiceStubsFactory workflowServiceStubsFactory;
  private final Boolean temporalCloudEnabled;
  private final Integer temporalRetentionInDays;

  public TemporalUtils(@Property(name = "temporal.cloud.client.cert") final String temporalCloudClientCert,
                       @Property(name = "temporal.cloud.client.key") final String temporalCloudClientKey,
                       @Property(name = "temporal.cloud.enabled",
                                 defaultValue = "false") final Boolean temporalCloudEnabled,
                       @Value("${temporal.cloud.host}") final String temporalCloudHost,
                       @Value("${temporal.cloud.namespace}") final String temporalCloudNamespace,
                       @Value("${temporal.host}") final String temporalHost,
                       @Property(name = "temporal.retention",
                                 defaultValue = "30") final Integer temporalRetentionInDays) {
    this.temporalCloudEnabled = Objects.requireNonNullElse(temporalCloudEnabled, false);
    this.temporalCloudConfig = new TemporalCloudConfig(temporalCloudClientCert, temporalCloudClientKey, temporalCloudHost, temporalCloudNamespace);
    this.workflowServiceStubsFactory = new WorkflowServiceStubsFactory(
        temporalCloudConfig,
        new TemporalSelfHostedConfig(temporalHost, this.temporalCloudEnabled ? temporalCloudNamespace : DEFAULT_NAMESPACE),
        this.temporalCloudEnabled);
    this.temporalRetentionInDays = temporalRetentionInDays;
  }

  /**
   * Create temporal service client.
   *
   * @param options client options
   * @param namespace temporal namespace
   * @return temporal service client
   */
  public WorkflowServiceStubs createTemporalService(final WorkflowServiceStubsOptions options, final String namespace) {
    return getTemporalClientWhenConnected(
        WAIT_INTERVAL,
        MAX_TIME_TO_CONNECT,
        WAIT_TIME_AFTER_CONNECT,
        () -> WorkflowServiceStubs.newInstance(options),
        namespace);
  }

  /**
   * Create temporal service client.
   *
   * @return temporal service client
   */
  public WorkflowServiceStubs createTemporalService(final TemporalSdkTimeouts temporalSdkTimeouts) {
    final var timeouts = new WorkflowServiceStubsTimeouts(temporalSdkTimeouts.getRpcTimeout(),
        temporalSdkTimeouts.getRpcLongPollTimeout(),
        temporalSdkTimeouts.getRpcQueryTimeout(),
        MAX_TIME_TO_CONNECT);
    final WorkflowServiceStubsOptions options = workflowServiceStubsFactory.createWorkflowServiceStubsOptions(timeouts);
    final String namespace = temporalCloudEnabled ? temporalCloudConfig.getNamespace() : DEFAULT_NAMESPACE;

    return createTemporalService(options, namespace);
  }

  public String getNamespace() {
    return temporalCloudEnabled ? temporalCloudConfig.getNamespace() : DEFAULT_NAMESPACE;
  }

  /**
   * Modifies the retention period for on-premise deployment of Temporal at the default namespace.
   * This should not be called when using Temporal Cloud, because Temporal Cloud does not allow
   * programmatic modification of workflow execution retention TTL.
   */
  public void configureTemporalNamespace(final WorkflowServiceStubs temporalService) {
    if (temporalCloudEnabled) {
      log.info("Skipping Temporal Namespace configuration because Temporal Cloud is in use.");
      return;
    }

    final var client = temporalService.blockingStub();
    final var describeNamespaceRequest = DescribeNamespaceRequest.newBuilder().setNamespace(DEFAULT_NAMESPACE).build();
    final var currentRetentionGrpcDuration = client.describeNamespace(describeNamespaceRequest).getConfig().getWorkflowExecutionRetentionTtl();
    final var currentRetention = Duration.ofSeconds(currentRetentionGrpcDuration.getSeconds());
    final var workflowExecutionTtl = Duration.ofDays(temporalRetentionInDays);
    final var humanReadableWorkflowExecutionTtl = DurationFormatUtils.formatDurationWords(workflowExecutionTtl.toMillis(), true, true);

    if (currentRetention.equals(workflowExecutionTtl)) {
      log.info("Workflow execution TTL already set for namespace " + DEFAULT_NAMESPACE + ". Remains unchanged as: "
          + humanReadableWorkflowExecutionTtl);
    } else {
      final var newGrpcDuration = com.google.protobuf.Duration.newBuilder().setSeconds(workflowExecutionTtl.getSeconds()).build();
      final var humanReadableCurrentRetention = DurationFormatUtils.formatDurationWords(currentRetention.toMillis(), true, true);
      final var namespaceConfig = NamespaceConfig.newBuilder().setWorkflowExecutionRetentionTtl(newGrpcDuration).build();
      final var updateNamespaceRequest = UpdateNamespaceRequest.newBuilder().setNamespace(DEFAULT_NAMESPACE).setConfig(namespaceConfig).build();
      log.info("Workflow execution TTL differs for namespace " + DEFAULT_NAMESPACE + ". Changing from (" + humanReadableCurrentRetention + ") to ("
          + humanReadableWorkflowExecutionTtl + "). ");
      client.updateNamespace(updateNamespaceRequest);
    }
  }

  /**
   * Loops and waits for the Temporal service to become available and returns a client.
   * <p>
   * This function uses a supplier as input since the creation of a WorkflowServiceStubs can result in
   * connection exceptions as well.
   */
  public WorkflowServiceStubs getTemporalClientWhenConnected(
                                                             final Duration waitInterval,
                                                             final Duration maxTimeToConnect,
                                                             final Duration waitAfterConnection,
                                                             final Supplier<WorkflowServiceStubs> temporalServiceSupplier,
                                                             final String namespace) {
    log.info("Waiting for temporal server...");

    boolean temporalNamespaceInitialized = false;
    WorkflowServiceStubs temporalService = null;
    long millisWaited = 0;

    while (!temporalNamespaceInitialized) {
      if (millisWaited >= maxTimeToConnect.toMillis()) {
        throw new RuntimeException("Could not create Temporal client within max timeout!");
      }

      log.warn("Waiting for namespace {} to be initialized in temporal...", namespace);
      Exceptions.toRuntime(() -> Thread.sleep(waitInterval.toMillis()));
      millisWaited = millisWaited + waitInterval.toMillis();

      try {
        temporalService = temporalServiceSupplier.get();
        final var namespaceInfo = getNamespaceInfo(temporalService, namespace);
        temporalNamespaceInitialized = namespaceInfo.isInitialized();
      } catch (final Exception e) {
        // Ignore the exception because this likely means that the Temporal service is still initializing.
        log.warn("Ignoring exception while trying to request Temporal namespace:", e);
      }
    }

    // sometimes it takes a few additional seconds for workflow queue listening to be available
    Exceptions.toRuntime(() -> Thread.sleep(waitAfterConnection.toMillis()));

    log.info("Temporal namespace {} initialized!", namespace);

    return temporalService;
  }

  protected NamespaceInfo getNamespaceInfo(final WorkflowServiceStubs temporalService, final String namespace) {
    return temporalService.blockingStub()
        .describeNamespace(DescribeNamespaceRequest.newBuilder().setNamespace(namespace).build())
        .getNamespaceInfo();
  }

  /**
   * Get log path (where logs should be written) for an attempt from a job root.
   *
   * @param jobRoot job root
   * @return path to logs for an attempt
   */
  public static Path getLogPath(final Path jobRoot) {
    return jobRoot.resolve(LogClientSingleton.LOG_FILENAME);
  }

  // todo (cgardens) - there are 2 sources of truth for job path. we need to reduce this down to one,
  // once we are fully on temporal.
  public static Path getJobRoot(final Path workspaceRoot, final JobRunConfig jobRunConfig) {
    return getJobRoot(workspaceRoot, jobRunConfig.getJobId(), jobRunConfig.getAttemptId());
  }

  /**
   * Get an attempt root director from workspace, job id, and attempt number.
   *
   * @param workspaceRoot workspace root
   * @param jobId job id
   * @param attemptId attempt number (as long)
   * @return working directory
   */
  public static Path getJobRoot(final Path workspaceRoot, final String jobId, final long attemptId) {
    return getJobRoot(workspaceRoot, jobId, Math.toIntExact(attemptId));
  }

  /**
   * Get an attempt root director from workspace, job id, and attempt number.
   *
   * @param workspaceRoot workspace root
   * @param jobId job id
   * @param attemptId attempt number (as int)
   * @return working directory
   */
  public static Path getJobRoot(final Path workspaceRoot, final String jobId, final int attemptId) {
    return workspaceRoot
        .resolve(String.valueOf(jobId))
        .resolve(String.valueOf(attemptId));
  }

  /**
   * Get an attempt root director from workspace and workloadId.
   *
   * @param workspaceRoot workspace root
   * @param workloadId workload id
   * @return working directory
   */
  public static Path getJobRoot(final Path workspaceRoot, final String workloadId) {
    return workspaceRoot
        .resolve(String.valueOf(workloadId));
  }

}
