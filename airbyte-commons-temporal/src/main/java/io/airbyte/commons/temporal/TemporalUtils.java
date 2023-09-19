/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.temporal;

import com.uber.m3.tally.RootScopeBuilder;
import com.uber.m3.tally.Scope;
import com.uber.m3.tally.StatsReporter;
import io.airbyte.commons.lang.Exceptions;
import io.airbyte.commons.temporal.config.TemporalSdkTimeouts;
import io.airbyte.config.helpers.LogClientSingleton;
import io.airbyte.metrics.lib.MetricClientFactory;
import io.airbyte.persistence.job.models.JobRunConfig;
import io.micrometer.core.instrument.MeterRegistry;
import io.micronaut.context.annotation.Property;
import io.micronaut.context.annotation.Value;
import io.temporal.activity.ActivityExecutionContext;
import io.temporal.api.common.v1.WorkflowExecution;
import io.temporal.api.namespace.v1.NamespaceConfig;
import io.temporal.api.namespace.v1.NamespaceInfo;
import io.temporal.api.workflowservice.v1.DescribeNamespaceRequest;
import io.temporal.api.workflowservice.v1.UpdateNamespaceRequest;
import io.temporal.client.ActivityCompletionException;
import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowStub;
import io.temporal.common.RetryOptions;
import io.temporal.common.reporter.MicrometerClientStatsReporter;
import io.temporal.serviceclient.SimpleSslContextBuilder;
import io.temporal.serviceclient.WorkflowServiceStubs;
import io.temporal.serviceclient.WorkflowServiceStubsOptions;
import io.temporal.workflow.Functions;
import jakarta.inject.Singleton;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import javax.net.ssl.SSLException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.time.DurationFormatUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;

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
  public static final Duration SEND_HEARTBEAT_INTERVAL = Duration.ofSeconds(20);
  public static final Duration HEARTBEAT_TIMEOUT = Duration.ofSeconds(30);
  public static final Duration HEARTBEAT_SHUTDOWN_GRACE_PERIOD = Duration.ofSeconds(30);
  public static final RetryOptions NO_RETRY = RetryOptions.newBuilder().setMaximumAttempts(1).build();
  private static final double REPORT_INTERVAL_SECONDS = 120.0;

  private final String temporalCloudClientCert;
  private final String temporalCloudClientKey;
  private final Boolean temporalCloudEnabled;
  private final String temporalCloudHost;
  private final String temporalCloudNamespace;
  private final String temporalHost;
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
    this.temporalCloudClientCert = temporalCloudClientCert;
    this.temporalCloudClientKey = temporalCloudClientKey;
    this.temporalCloudEnabled = temporalCloudEnabled;
    this.temporalCloudHost = temporalCloudHost;
    this.temporalCloudNamespace = temporalCloudNamespace;
    this.temporalHost = temporalHost;
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
   * @param isCloud is a cloud deployment
   * @return temporal service client
   */
  // TODO consider consolidating this method's logic into createTemporalService() after the Temporal
  // Cloud migration is complete.
  // The Temporal Migration migrator is the only reason this public method exists.
  public WorkflowServiceStubs createTemporalService(final boolean isCloud, final TemporalSdkTimeouts temporalSdkTimeouts) {
    final WorkflowServiceStubsOptions options =
        isCloud ? getCloudTemporalOptions(temporalSdkTimeouts) : TemporalWorkflowUtils.getAirbyteTemporalOptions(temporalHost, temporalSdkTimeouts);
    final String namespace = isCloud ? temporalCloudNamespace : DEFAULT_NAMESPACE;

    return createTemporalService(options, namespace);
  }

  public WorkflowServiceStubs createTemporalService(final TemporalSdkTimeouts temporalSdkTimeouts) {
    return createTemporalService(temporalCloudEnabled, temporalSdkTimeouts);
  }

  private WorkflowServiceStubsOptions getCloudTemporalOptions(final TemporalSdkTimeouts temporalSdkTimeouts) {
    final InputStream clientCert = new ByteArrayInputStream(temporalCloudClientCert.getBytes(StandardCharsets.UTF_8));
    final InputStream clientKey = new ByteArrayInputStream(temporalCloudClientKey.getBytes(StandardCharsets.UTF_8));
    final WorkflowServiceStubsOptions.Builder optionBuilder;
    try {
      optionBuilder = WorkflowServiceStubsOptions.newBuilder()
          .setRpcTimeout(temporalSdkTimeouts.getRpcTimeout())
          .setRpcLongPollTimeout(temporalSdkTimeouts.getRpcLongPollTimeout())
          .setRpcQueryTimeout(temporalSdkTimeouts.getRpcQueryTimeout())
          .setSslContext(SimpleSslContextBuilder.forPKCS8(clientCert, clientKey).build())
          .setTarget(temporalCloudHost);
    } catch (final SSLException e) {
      log.error("SSL Exception occurred attempting to establish Temporal Cloud options.");
      throw new RuntimeException(e);
    }

    configureTemporalMeterRegistry(optionBuilder);
    return optionBuilder.build();
  }

  private void configureTemporalMeterRegistry(final WorkflowServiceStubsOptions.Builder optionalBuilder) {
    final MeterRegistry registry = MetricClientFactory.getMeterRegistry();
    if (registry != null) {
      final StatsReporter reporter = new MicrometerClientStatsReporter(registry);
      final Scope scope = new RootScopeBuilder()
          .reporter(reporter)
          .reportEvery(com.uber.m3.util.Duration.ofSeconds(REPORT_INTERVAL_SECONDS));
      optionalBuilder.setMetricsScope(scope);
    }
  }

  public String getNamespace() {
    return temporalCloudEnabled ? temporalCloudNamespace : DEFAULT_NAMESPACE;
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
   * Allows running a given temporal workflow stub asynchronously. This method only works for
   * workflows that take one argument. Because of the iface that Temporal supplies, in order to handle
   * other method signatures, if we need to support them, we will need to add another helper with that
   * number of args. For a reference on how Temporal recommends to do this see their docs:
   * https://docs.temporal.io/docs/java/workflows#asynchronous-start
   *
   * @param workflowStub - workflow stub to be executed
   * @param function - function on the workflow stub to be executed
   * @param arg1 - argument to be supplied to the workflow function
   * @param outputType - class of the output type of the workflow function
   * @param <STUB> - type of the workflow stub
   * @param <A1> - type of the argument of the workflow stub
   * @param <R> - type of the return of the workflow stub
   * @return pair of the workflow execution (contains metadata on the asynchronously running job) and
   *         future that can be used to await the result of the workflow stub's function
   */
  @SuppressWarnings("MethodTypeParameterName")
  public <STUB, A1, R> ImmutablePair<WorkflowExecution, CompletableFuture<R>> asyncExecute(final STUB workflowStub,
                                                                                           final Functions.Func1<A1, R> function,
                                                                                           final A1 arg1,
                                                                                           final Class<R> outputType) {
    final WorkflowStub untyped = WorkflowStub.fromTyped(workflowStub);
    final WorkflowExecution workflowExecution = WorkflowClient.start(function, arg1);
    final CompletableFuture<R> resultAsync = untyped.getResultAsync(outputType);
    return ImmutablePair.of(workflowExecution, resultAsync);
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
   * Run a callable. If while it is running the temporal activity is cancelled, the provided callback
   * is triggered.
   *
   * It manages this by regularly calling back to temporal in order to check whether the activity has
   * been cancelled. If it is cancelled it calls the callback.
   *
   * @param afterCancellationCallbackRef callback to be triggered if the temporal activity is
   *        cancelled before the callable completes
   * @param callable callable to run with cancellation
   * @param activityContext context used to check whether the activity has been cancelled
   * @param <T> type of variable returned by the callable
   * @return if the callable succeeds without being cancelled, returns the value returned by the
   *         callable
   */
  public <T> T withBackgroundHeartbeat(final AtomicReference<Runnable> afterCancellationCallbackRef,
                                       final Callable<T> callable,
                                       final Supplier<ActivityExecutionContext> activityContext) {
    final ScheduledExecutorService scheduledExecutor = Executors.newSingleThreadScheduledExecutor();

    try {
      // Schedule the cancellation handler.
      scheduledExecutor.scheduleAtFixedRate(() -> {
        final CancellationHandler cancellationHandler = new CancellationHandler.TemporalCancellationHandler(activityContext.get());

        cancellationHandler.checkAndHandleCancellation(() -> {
          // After cancellation cleanup.
          if (afterCancellationCallbackRef != null) {
            final Runnable cancellationCallback = afterCancellationCallbackRef.get();
            if (cancellationCallback != null) {
              cancellationCallback.run();
            }
          }
        });
      }, 0, SEND_HEARTBEAT_INTERVAL.toSeconds(), TimeUnit.SECONDS);

      return callable.call();
    } catch (final ActivityCompletionException e) {
      log.warn("Job either timed out or was cancelled.");
      throw new RuntimeException(e);
    } catch (final Exception e) {
      throw new RuntimeException(e);
    } finally {
      log.info("Stopping temporal heartbeating...");
      // At this point we are done, we should try to stop all heartbeat ASAP.
      // We can safely ignore the list of tasks that didn't start since we are exiting regardless.
      scheduledExecutor.shutdownNow();

      try {
        // Making sure the heartbeat executor is terminated to avoid heartbeat attempt after we're done with
        // the activity.
        if (scheduledExecutor.awaitTermination(HEARTBEAT_SHUTDOWN_GRACE_PERIOD.toSeconds(), TimeUnit.SECONDS)) {
          log.info("Temporal heartbeating stopped.");
        } else {
          // Heartbeat thread failed to stop, we may leak a thread if this happens.
          // We should not fail the execution because of this.
          log.info("Temporal heartbeating didn't stop within {} seconds, continuing the shutdown.", HEARTBEAT_SHUTDOWN_GRACE_PERIOD.toSeconds());
        }
      } catch (InterruptedException e) {
        // We got interrupted while attempting to shutdown the executor. Not much more we can do.
        log.info("Interrupted while stopping the Temporal heartbeating, continuing the shutdown.");
        // Preserve the interrupt status
        Thread.currentThread().interrupt();
      }
    }
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

}
