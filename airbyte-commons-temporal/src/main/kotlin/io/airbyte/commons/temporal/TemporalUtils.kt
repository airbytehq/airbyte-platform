/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.temporal

import io.airbyte.commons.duration.formatMilli
import io.airbyte.commons.lang.Exceptions
import io.airbyte.commons.logging.DEFAULT_LOG_FILENAME
import io.airbyte.commons.temporal.config.TemporalSdkTimeouts
import io.airbyte.commons.temporal.factories.TemporalCloudConfig
import io.airbyte.commons.temporal.factories.TemporalSelfHostedConfig
import io.airbyte.commons.temporal.factories.WorkflowServiceStubsFactory
import io.airbyte.commons.temporal.factories.WorkflowServiceStubsTimeouts
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micrometer.core.instrument.MeterRegistry
import io.micronaut.context.annotation.Property
import io.micronaut.context.annotation.Value
import io.temporal.api.namespace.v1.NamespaceConfig
import io.temporal.api.namespace.v1.NamespaceInfo
import io.temporal.api.workflowservice.v1.DescribeNamespaceRequest
import io.temporal.api.workflowservice.v1.UpdateNamespaceRequest
import io.temporal.serviceclient.WorkflowServiceStubs
import io.temporal.serviceclient.WorkflowServiceStubsOptions
import jakarta.inject.Singleton
import java.nio.file.Path
import java.time.Duration
import java.util.Objects
import java.util.Optional
import java.util.function.Supplier

// todo (cgardens) - rename? utils implies it's static utility function

/**
 * Temporal Utility functions.
 */
@Singleton
class TemporalUtils(
  @Property(name = "temporal.cloud.client.cert") temporalCloudClientCert: String?,
  @Property(name = "temporal.cloud.client.key") temporalCloudClientKey: String?,
  @Property(name = "temporal.cloud.enabled", defaultValue = "false") temporalCloudEnabled: Boolean,
  @Value("\${temporal.cloud.host}") temporalCloudHost: String?,
  @Value("\${temporal.cloud.namespace}") temporalCloudNamespace: String?,
  @Value("\${temporal.host}") temporalHost: String?,
  @Property(name = "temporal.retention", defaultValue = "30") temporalRetentionInDays: Int,
  meterRegistry: Optional<MeterRegistry>,
) {
  private val temporalCloudConfig =
    TemporalCloudConfig(temporalCloudClientCert, temporalCloudClientKey, temporalCloudHost, temporalCloudNamespace)
  private val workflowServiceStubsFactory: WorkflowServiceStubsFactory
  private val temporalCloudEnabled: Boolean = Objects.requireNonNullElse(temporalCloudEnabled, false)
  private val temporalRetentionInDays: Int

  init {
    this.workflowServiceStubsFactory =
      WorkflowServiceStubsFactory(
        temporalCloudConfig,
        TemporalSelfHostedConfig(temporalHost, if (this.temporalCloudEnabled) temporalCloudNamespace else DEFAULT_NAMESPACE),
        this.temporalCloudEnabled,
        meterRegistry.orElse(null),
      )
    this.temporalRetentionInDays = temporalRetentionInDays
  }

  /**
   * Create temporal service client.
   *
   * Wait until the Temporal service is available and the namespace is initialized.
   *
   * @param options client options
   * @param namespace temporal namespace
   * @return temporal service client
   */
  fun createTemporalService(
    options: WorkflowServiceStubsOptions,
    namespace: String,
  ): WorkflowServiceStubs =
    getTemporalClientWhenConnected(
      WAIT_INTERVAL,
      MAX_TIME_TO_CONNECT,
      WAIT_TIME_AFTER_CONNECT,
      { WorkflowServiceStubs.newInstance(options) },
      namespace,
    )

  /**
   * Create temporal service client.
   *
   * Wait until the Temporal service is available and the namespace is initialized.
   *
   * @return temporal service client
   */
  fun createTemporalService(temporalSdkTimeouts: TemporalSdkTimeouts): WorkflowServiceStubs {
    val timeouts =
      WorkflowServiceStubsTimeouts(
        temporalSdkTimeouts.rpcTimeout,
        temporalSdkTimeouts.rpcLongPollTimeout,
        temporalSdkTimeouts.rpcQueryTimeout,
        MAX_TIME_TO_CONNECT,
      )
    val options = workflowServiceStubsFactory.createWorkflowServiceStubsOptions(timeouts)
    val namespace = if (temporalCloudEnabled) temporalCloudConfig.namespace else DEFAULT_NAMESPACE

    return createTemporalService(options, namespace!!)
  }

  fun getNamespace(): String = if (temporalCloudEnabled) temporalCloudConfig.namespace!! else DEFAULT_NAMESPACE

  /**
   * Modifies the retention period for on-premise deployment of Temporal at the default namespace.
   * This should not be called when using Temporal Cloud, because Temporal Cloud does not allow
   * programmatic modification of workflow execution retention TTL.
   */
  fun configureTemporalNamespace(temporalService: WorkflowServiceStubs) {
    if (temporalCloudEnabled) {
      log.info("Skipping Temporal Namespace configuration because Temporal Cloud is in use.")
      return
    }

    val client = temporalService.blockingStub()
    val describeNamespaceRequest = DescribeNamespaceRequest.newBuilder().setNamespace(DEFAULT_NAMESPACE).build()
    val currentRetentionGrpcDuration = client.describeNamespace(describeNamespaceRequest).config.workflowExecutionRetentionTtl
    val currentRetention = Duration.ofSeconds(currentRetentionGrpcDuration.seconds)
    val workflowExecutionTtl = Duration.ofDays(temporalRetentionInDays.toLong())
    val humanReadableWorkflowExecutionTtl = formatMilli(workflowExecutionTtl.toMillis())

    if (currentRetention == workflowExecutionTtl) {
      log.info(
        (
          "Workflow execution TTL already set for namespace " + DEFAULT_NAMESPACE + ". Remains unchanged as: " +
            humanReadableWorkflowExecutionTtl
        ),
      )
    } else {
      val newGrpcDuration =
        com.google.protobuf.Duration
          .newBuilder()
          .setSeconds(workflowExecutionTtl.seconds)
          .build()
      val humanReadableCurrentRetention = formatMilli(currentRetention.toMillis())
      val namespaceConfig = NamespaceConfig.newBuilder().setWorkflowExecutionRetentionTtl(newGrpcDuration).build()
      val updateNamespaceRequest =
        UpdateNamespaceRequest
          .newBuilder()
          .setNamespace(DEFAULT_NAMESPACE)
          .setConfig(namespaceConfig)
          .build()
      log.info(
        (
          "Workflow execution TTL differs for namespace " + DEFAULT_NAMESPACE + ". Changing from (" + humanReadableCurrentRetention + ") to (" +
            humanReadableWorkflowExecutionTtl + "). "
        ),
      )
      client.updateNamespace(updateNamespaceRequest)
    }
  }

  /**
   * Loops and waits for the Temporal service to become available and returns a client.
   *
   *
   * This function uses a supplier as input since the creation of a WorkflowServiceStubs can result in
   * connection exceptions as well.
   */
  fun getTemporalClientWhenConnected(
    waitInterval: Duration,
    maxTimeToConnect: Duration,
    waitAfterConnection: Duration,
    temporalServiceSupplier: Supplier<WorkflowServiceStubs>,
    namespace: String,
  ): WorkflowServiceStubs {
    log.info("Waiting for temporal server...")

    var temporalNamespaceInitialized = false
    var temporalService: WorkflowServiceStubs? = null
    var millisWaited: Long = 0

    while (!temporalNamespaceInitialized) {
      if (millisWaited >= maxTimeToConnect.toMillis()) {
        throw RuntimeException("Could not create Temporal client within max timeout!")
      }

      log.warn("Waiting for namespace {} to be initialized in temporal...", namespace)
      Exceptions.toRuntime { Thread.sleep(waitInterval.toMillis()) }
      millisWaited = millisWaited + waitInterval.toMillis()

      try {
        temporalService = temporalServiceSupplier.get()
        val namespaceInfo = getNamespaceInfo(temporalService, namespace)
        temporalNamespaceInitialized = namespaceInfo.isInitialized
      } catch (e: Exception) {
        // Ignore the exception because this likely means that the Temporal service is still initializing.
        log.warn("Ignoring exception while trying to request Temporal namespace:", e)
      }
    }

    // sometimes it takes a few additional seconds for workflow queue listening to be available
    Exceptions.toRuntime { Thread.sleep(waitAfterConnection.toMillis()) }

    log.info("Temporal namespace {} initialized!", namespace)

    return temporalService!!
  }

  protected fun getNamespaceInfo(
    temporalService: WorkflowServiceStubs,
    namespace: String,
  ): NamespaceInfo =
    temporalService
      .blockingStub()
      .describeNamespace(DescribeNamespaceRequest.newBuilder().setNamespace(namespace).build())
      .namespaceInfo

  companion object {
    private val log = KotlinLogging.logger {}

    private val WAIT_INTERVAL: Duration = Duration.ofSeconds(2)
    private val MAX_TIME_TO_CONNECT: Duration = Duration.ofMinutes(2)
    private val WAIT_TIME_AFTER_CONNECT: Duration = Duration.ofSeconds(5)
    const val DEFAULT_NAMESPACE: String = "default"
    private const val REPORT_INTERVAL_SECONDS = 120.0

    /**
     * Get log path (where logs should be written) for an attempt from a job root.
     *
     * @param jobRoot job root
     * @return path to logs for an attempt
     */
    fun getLogPath(jobRoot: Path): Path = jobRoot.resolve(DEFAULT_LOG_FILENAME)

    /**
     * Get an attempt root director from workspace, job id, and attempt number.
     *
     * @param workspaceRoot workspace root
     * @param jobId job id
     * @param attemptId attempt number (as long)
     * @return working directory
     */
    @JvmStatic
    fun getJobRoot(
      workspaceRoot: Path,
      jobId: String,
      attemptId: Long,
    ): Path =
      workspaceRoot
        .resolve(jobId.toString())
        .resolve(attemptId.toString())
  }
}
