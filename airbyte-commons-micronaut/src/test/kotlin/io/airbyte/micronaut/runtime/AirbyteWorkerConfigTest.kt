/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.micronaut.runtime

import io.micronaut.context.env.Environment
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import jakarta.inject.Inject
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

@MicronautTest(environments = [Environment.TEST])
internal class AirbyteWorkerConfigDefaultTest {
  @Inject
  private lateinit var airbyteWorkerConfig: AirbyteWorkerConfig

  @Test
  fun testLoadingValuesFromConfig() {
    assertEquals(true, airbyteWorkerConfig.check.enabled)
    assertEquals(DEFAULT_WORKER_CHECK_MAX_WORKERS, airbyteWorkerConfig.check.maxWorkers)
    assertEquals(true, airbyteWorkerConfig.connection.enabled)
    assertEquals(DEFAULT_WORKER_CONNECTION_NO_JITTER_CUTOFF_MINUTES, airbyteWorkerConfig.connection.scheduleJitter.noJitterCutoffMinutes)
    assertEquals(
      DEFAULT_WORKER_CONNECTION_HIGH_FREQUENCY_JITTER_AMOUNT_MINUTES,
      airbyteWorkerConfig.connection.scheduleJitter.highFrequencyBucket.jitterAmountMinutes,
    )
    assertEquals(
      DEFAULT_WORKER_CONNECTION_HIGH_FREQUENCY_JITTER_THRESHOLD_MINUTES,
      airbyteWorkerConfig.connection.scheduleJitter.highFrequencyBucket.thresholdMinutes,
    )
    assertEquals(
      DEFAULT_WORKER_CONNECTION_MEDIUM_FREQUENCY_JITTER_AMOUNT_MINUTES,
      airbyteWorkerConfig.connection.scheduleJitter.mediumFrequencyBucket.jitterAmountMinutes,
    )
    assertEquals(
      DEFAULT_WORKER_CONNECTION_MEDIUM_FREQUENCY_JITTER_THRESHOLD_MINUTES,
      airbyteWorkerConfig.connection.scheduleJitter.mediumFrequencyBucket.thresholdMinutes,
    )
    assertEquals(
      DEFAULT_WORKER_CONNECTION_LOW_FREQUENCY_JITTER_AMOUNT_MINUTES,
      airbyteWorkerConfig.connection.scheduleJitter.lowFrequencyBucket.jitterAmountMinutes,
    )
    assertEquals(
      DEFAULT_WORKER_CONNECTION_LOW_FREQUENCY_JITTER_THRESHOLD_MINUTES,
      airbyteWorkerConfig.connection.scheduleJitter.lowFrequencyBucket.thresholdMinutes,
    )
    assertEquals(
      DEFAULT_WORKER_CONNECTION_VERY_LOW_FREQUENCY_JITTER_AMOUNT_MINUTES,
      airbyteWorkerConfig.connection.scheduleJitter.veryLowFrequencyBucket.jitterAmountMinutes,
    )
    assertEquals("", airbyteWorkerConfig.connectorSidecar.resources.cpuLimit)
    assertEquals("", airbyteWorkerConfig.connectorSidecar.resources.cpuRequest)
    assertEquals("", airbyteWorkerConfig.connectorSidecar.resources.memoryLimit)
    assertEquals("", airbyteWorkerConfig.connectorSidecar.resources.memoryRequest)
    assertEquals(DEFAULT_WORKER_DISCOVER_AUTO_REFRESH_WINDOW, airbyteWorkerConfig.discover.autoRefreshWindow)
    assertEquals(true, airbyteWorkerConfig.discover.enabled)
    assertEquals(DEFAULT_WORKER_DISCOVER_MAX_WORKERS, airbyteWorkerConfig.discover.maxWorkers)
    assertEquals(DEFAULT_WORKER_FILE_TRANSFER_EPHEMERAL_STORAGE_LIMIT, airbyteWorkerConfig.fileTransfer.resources.ephemeralStorageLimit)
    assertEquals(DEFAULT_WORKER_FILE_TRANSFER_EPHEMERAL_STORAGE_REQUEST, airbyteWorkerConfig.fileTransfer.resources.ephemeralStorageRequest)
    assertEquals(false, airbyteWorkerConfig.isolated.kube.useCustomNodeSelector)
    assertEquals("", airbyteWorkerConfig.isolated.kube.nodeSelectors)
    assertEquals("", airbyteWorkerConfig.job.errorReporting.sentry.dsn)
    assertEquals(JobErrorReportingStrategy.LOGGING, airbyteWorkerConfig.job.errorReporting.strategy)
    assertEquals("", airbyteWorkerConfig.job.kubernetes.connectorImageRegistry)
    assertEquals("", airbyteWorkerConfig.job.kubernetes.init.container.image)
    assertEquals(DEFAULT_WORKER_KUBE_IMAGE_PULL_POLICY, airbyteWorkerConfig.job.kubernetes.init.container.imagePullPolicy)
    assertEquals(emptyList<String>(), airbyteWorkerConfig.job.kubernetes.init.container.imagePullSecret)
    assertEquals("", airbyteWorkerConfig.job.kubernetes.main.container.image)
    assertEquals(DEFAULT_WORKER_KUBE_IMAGE_PULL_POLICY, airbyteWorkerConfig.job.kubernetes.main.container.imagePullPolicy)
    assertEquals(emptyList<String>(), airbyteWorkerConfig.job.kubernetes.main.container.imagePullSecret)
    assertEquals("", airbyteWorkerConfig.job.kubernetes.profiler.container.image)
    assertEquals(DEFAULT_WORKER_KUBE_IMAGE_PULL_POLICY, airbyteWorkerConfig.job.kubernetes.profiler.container.imagePullPolicy)
    assertEquals(emptyList<String>(), airbyteWorkerConfig.job.kubernetes.profiler.container.imagePullSecret)
    assertEquals(DEFAULT_WORKER_KUBE_PROFILER_CPU_LIMIT, airbyteWorkerConfig.job.kubernetes.profiler.container.cpuLimit)
    assertEquals(DEFAULT_WORKER_KUBE_PROFILER_CPU_REQUEST, airbyteWorkerConfig.job.kubernetes.profiler.container.cpuRequest)
    assertEquals(DEFAULT_WORKER_KUBE_PROFILER_MEMORY_LIMIT, airbyteWorkerConfig.job.kubernetes.profiler.container.memoryLimit)
    assertEquals(DEFAULT_WORKER_KUBE_PROFILER_MEMORY_REQUEST, airbyteWorkerConfig.job.kubernetes.profiler.container.memoryRequest)
    assertEquals("", airbyteWorkerConfig.job.kubernetes.sidecar.container.image)
    assertEquals(DEFAULT_WORKER_KUBE_IMAGE_PULL_POLICY, airbyteWorkerConfig.job.kubernetes.sidecar.container.imagePullPolicy)
    assertEquals(emptyList<String>(), airbyteWorkerConfig.job.kubernetes.sidecar.container.imagePullSecret)
    assertEquals(DEFAULT_WORKER_KUBE_SERVICE_ACCOUNT, airbyteWorkerConfig.job.kubernetes.serviceAccount)
    assertEquals(DEFAULT_WORKER_JOB_NAMESPACE, airbyteWorkerConfig.job.kubernetes.namespace)
    assertEquals("", airbyteWorkerConfig.job.kubernetes.volumes.dataPlaneCreds.mountPath)
    assertEquals("", airbyteWorkerConfig.job.kubernetes.volumes.dataPlaneCreds.secretName)
    assertEquals(false, airbyteWorkerConfig.job.kubernetes.volumes.local.enabled)
    assertEquals("", airbyteWorkerConfig.job.kubernetes.volumes.gcsCreds.mountPath)
    assertEquals("", airbyteWorkerConfig.job.kubernetes.volumes.gcsCreds.secretName)
    assertEquals("", airbyteWorkerConfig.job.kubernetes.volumes.secret.mountPath)
    assertEquals("", airbyteWorkerConfig.job.kubernetes.volumes.secret.secretName)
    assertEquals(DEFAULT_WORKER_KUBE_VOLUME_STAGING_MOUNT_PATH, airbyteWorkerConfig.job.kubernetes.volumes.staging.mountPath)
    assertEquals("", airbyteWorkerConfig.job.kubernetes.tolerations)
    assertEquals(true, airbyteWorkerConfig.notify.enabled)
    assertEquals(DEFAULT_WORKER_NOTIFY_MAX_WORKERS, airbyteWorkerConfig.notify.maxWorkers)
    assertEquals(true, airbyteWorkerConfig.spec.enabled)
    assertEquals(DEFAULT_WORKER_SPEC_MAX_WORKERS, airbyteWorkerConfig.sync.maxWorkers)
    assertEquals(true, airbyteWorkerConfig.sync.enabled)
    assertEquals(DEFAULT_WORKER_SYNC_MAX_WORKERS, airbyteWorkerConfig.sync.maxWorkers)
    assertEquals(DEFAULT_WORKER_SYNC_MAX_ATTEMPTS, airbyteWorkerConfig.sync.maxAttempts)
    assertEquals(DEFAULT_WORKER_SYNC_MAX_TIMEOUT_DAYS, airbyteWorkerConfig.sync.maxTimeout)
    assertEquals(DEFAULT_WORKER_SYNC_MAX_INIT_TIMEOUT_MINUTES, airbyteWorkerConfig.sync.maxInitTimeout)
    assertEquals(emptyList<AirbyteWorkerConfig.AirbyteWorkerKubeJobConfig>(), airbyteWorkerConfig.kubeJobConfigs)
    assertEquals(DEFAULT_WORKER_REPLICATION_DISPATCHER_THREADS, airbyteWorkerConfig.replication.dispatcher.nThreads)
    assertEquals(DEFAULT_WORKER_REPLICATION_PERSISTENCE_FLUSH_PERIOD_SEC, airbyteWorkerConfig.replication.persistenceFlushPeriodSec)
  }
}

@MicronautTest(propertySources = ["classpath:application-worker.yml"])
internal class AirbyteWorkerConfigOverridesTest {
  @Inject
  private lateinit var airbyteWorkerConfig: AirbyteWorkerConfig

  @Test
  fun testLoadingValuesFromConfig() {
    assertEquals(false, airbyteWorkerConfig.check.enabled)
    assertEquals(1, airbyteWorkerConfig.check.maxWorkers)
    assertEquals(false, airbyteWorkerConfig.connection.enabled)
    assertEquals(1, airbyteWorkerConfig.connection.scheduleJitter.noJitterCutoffMinutes)
    assertEquals(10, airbyteWorkerConfig.connection.scheduleJitter.highFrequencyBucket.jitterAmountMinutes)
    assertEquals(20, airbyteWorkerConfig.connection.scheduleJitter.highFrequencyBucket.thresholdMinutes)
    assertEquals(30, airbyteWorkerConfig.connection.scheduleJitter.mediumFrequencyBucket.jitterAmountMinutes)
    assertEquals(40, airbyteWorkerConfig.connection.scheduleJitter.mediumFrequencyBucket.thresholdMinutes)
    assertEquals(50, airbyteWorkerConfig.connection.scheduleJitter.lowFrequencyBucket.jitterAmountMinutes)
    assertEquals(60, airbyteWorkerConfig.connection.scheduleJitter.lowFrequencyBucket.thresholdMinutes)
    assertEquals(70, airbyteWorkerConfig.connection.scheduleJitter.veryLowFrequencyBucket.jitterAmountMinutes)
    assertEquals("10", airbyteWorkerConfig.connectorSidecar.resources.cpuLimit)
    assertEquals("11", airbyteWorkerConfig.connectorSidecar.resources.cpuRequest)
    assertEquals("12", airbyteWorkerConfig.connectorSidecar.resources.memoryLimit)
    assertEquals("13", airbyteWorkerConfig.connectorSidecar.resources.memoryRequest)
    assertEquals(1, airbyteWorkerConfig.discover.autoRefreshWindow)
    assertEquals(false, airbyteWorkerConfig.discover.enabled)
    assertEquals(2, airbyteWorkerConfig.discover.maxWorkers)
    assertEquals("1G", airbyteWorkerConfig.fileTransfer.resources.ephemeralStorageLimit)
    assertEquals("2G", airbyteWorkerConfig.fileTransfer.resources.ephemeralStorageRequest)
    assertEquals(true, airbyteWorkerConfig.isolated.kube.useCustomNodeSelector)
    assertEquals("test-node-selectors", airbyteWorkerConfig.isolated.kube.nodeSelectors)
    assertEquals("test-sentry-dsn", airbyteWorkerConfig.job.errorReporting.sentry.dsn)
    assertEquals(JobErrorReportingStrategy.SENTRY, airbyteWorkerConfig.job.errorReporting.strategy)
    assertEquals("test-connector-image-registry", airbyteWorkerConfig.job.kubernetes.connectorImageRegistry)
    assertEquals("test-init-image", airbyteWorkerConfig.job.kubernetes.init.container.image)
    assertEquals("test-init-image-pull-policy", airbyteWorkerConfig.job.kubernetes.init.container.imagePullPolicy)
    assertEquals(
      listOf("test-init-image-pull-secret", "test-init-image-pull-secret-2"),
      airbyteWorkerConfig.job.kubernetes.init.container.imagePullSecret,
    )
    assertEquals("test-main-image", airbyteWorkerConfig.job.kubernetes.main.container.image)
    assertEquals("test-main-image-pull-policy", airbyteWorkerConfig.job.kubernetes.main.container.imagePullPolicy)
    assertEquals(
      listOf("test-main-image-pull-secret", "test-main-image-pull-secret-2"),
      airbyteWorkerConfig.job.kubernetes.main.container.imagePullSecret,
    )
    assertEquals("test-profiler-image", airbyteWorkerConfig.job.kubernetes.profiler.container.image)
    assertEquals("test-profiler-image-pull-policy", airbyteWorkerConfig.job.kubernetes.profiler.container.imagePullPolicy)
    assertEquals(
      listOf("test-profiler-image-pull-secret", "test-profiler-image-pull-secret-2"),
      airbyteWorkerConfig.job.kubernetes.profiler.container.imagePullSecret,
    )
    assertEquals("2", airbyteWorkerConfig.job.kubernetes.profiler.container.cpuLimit)
    assertEquals("3", airbyteWorkerConfig.job.kubernetes.profiler.container.cpuRequest)
    assertEquals("4", airbyteWorkerConfig.job.kubernetes.profiler.container.memoryLimit)
    assertEquals("5", airbyteWorkerConfig.job.kubernetes.profiler.container.memoryRequest)
    assertEquals("test-sidecar-image", airbyteWorkerConfig.job.kubernetes.sidecar.container.image)
    assertEquals("test-sidecar-image-pull-policy", airbyteWorkerConfig.job.kubernetes.sidecar.container.imagePullPolicy)
    assertEquals(
      listOf("test-sidecar-image-pull-secret", "test-sidecar-image-pull-secret-2"),
      airbyteWorkerConfig.job.kubernetes.sidecar.container.imagePullSecret,
    )
    assertEquals("test-namespace", airbyteWorkerConfig.job.kubernetes.namespace)
    assertEquals("test-service-account", airbyteWorkerConfig.job.kubernetes.serviceAccount)
    assertEquals("test-worker-tolerations", airbyteWorkerConfig.job.kubernetes.tolerations)
    assertEquals("test-data-plane-mount-path", airbyteWorkerConfig.job.kubernetes.volumes.dataPlaneCreds.mountPath)
    assertEquals("test-data-plane-secret-name", airbyteWorkerConfig.job.kubernetes.volumes.dataPlaneCreds.secretName)
    assertEquals(true, airbyteWorkerConfig.job.kubernetes.volumes.local.enabled)
    assertEquals("test-gcs-creds-mount-path", airbyteWorkerConfig.job.kubernetes.volumes.gcsCreds.mountPath)
    assertEquals("test-gcs-creds-secret-name", airbyteWorkerConfig.job.kubernetes.volumes.gcsCreds.secretName)
    assertEquals("test-secret-mount-path", airbyteWorkerConfig.job.kubernetes.volumes.secret.mountPath)
    assertEquals("test-secret-secret-name", airbyteWorkerConfig.job.kubernetes.volumes.secret.secretName)
    assertEquals("test-staging-mount-path", airbyteWorkerConfig.job.kubernetes.volumes.staging.mountPath)
    assertEquals("test-worker-tolerations", airbyteWorkerConfig.job.kubernetes.tolerations)
    assertEquals(false, airbyteWorkerConfig.notify.enabled)
    assertEquals(3, airbyteWorkerConfig.notify.maxWorkers)
    assertEquals(false, airbyteWorkerConfig.spec.enabled)
    assertEquals(4, airbyteWorkerConfig.spec.maxWorkers)
    assertEquals(false, airbyteWorkerConfig.sync.enabled)
    assertEquals(10, airbyteWorkerConfig.sync.maxWorkers)
    assertEquals(6, airbyteWorkerConfig.sync.maxAttempts)
    assertEquals(7, airbyteWorkerConfig.sync.maxTimeout)
    assertEquals(8, airbyteWorkerConfig.sync.maxInitTimeout)
    assertEquals("test-default-annotations", airbyteWorkerConfig.kubeJobConfigs.find { it.name == "default" }?.annotations)
    assertEquals("test-default-labels", airbyteWorkerConfig.kubeJobConfigs.find { it.name == "default" }?.labels)
    assertEquals("test-default-node-selectors", airbyteWorkerConfig.kubeJobConfigs.find { it.name == "default" }?.nodeSelectors)
    assertEquals("test-default-cpu-limit", airbyteWorkerConfig.kubeJobConfigs.find { it.name == "default" }?.cpuLimit)
    assertEquals("test-default-cpu-request", airbyteWorkerConfig.kubeJobConfigs.find { it.name == "default" }?.cpuRequest)
    assertEquals("test-default-memory-limit", airbyteWorkerConfig.kubeJobConfigs.find { it.name == "default" }?.memoryLimit)
    assertEquals("test-default-memory-request", airbyteWorkerConfig.kubeJobConfigs.find { it.name == "default" }?.memoryRequest)
    assertEquals(
      "test-default-ephemeral-storage-limit",
      airbyteWorkerConfig.kubeJobConfigs.find { it.name == "default" }?.ephemeralStorageLimit,
    )
    assertEquals(
      "test-default-ephemeral-storage-request",
      airbyteWorkerConfig.kubeJobConfigs
        .find {
          it.name == "default"
        }?.ephemeralStorageRequest,
    )
    assertEquals("test-test1-annotations", airbyteWorkerConfig.kubeJobConfigs.find { it.name == "test1" }?.annotations)
    assertEquals("test-test1-labels", airbyteWorkerConfig.kubeJobConfigs.find { it.name == "test1" }?.labels)
    assertEquals("test-test1-node-selectors", airbyteWorkerConfig.kubeJobConfigs.find { it.name == "test1" }?.nodeSelectors)
    assertEquals("test-test1-cpu-limit", airbyteWorkerConfig.kubeJobConfigs.find { it.name == "test1" }?.cpuLimit)
    assertEquals("test-test1-cpu-request", airbyteWorkerConfig.kubeJobConfigs.find { it.name == "test1" }?.cpuRequest)
    assertEquals("test-test1-memory-limit", airbyteWorkerConfig.kubeJobConfigs.find { it.name == "test1" }?.memoryLimit)
    assertEquals("test-test1-memory-request", airbyteWorkerConfig.kubeJobConfigs.find { it.name == "test1" }?.memoryRequest)
    assertEquals("test-test1-ephemeral-storage-limit", airbyteWorkerConfig.kubeJobConfigs.find { it.name == "test1" }?.ephemeralStorageLimit)
    assertEquals(
      "test-test1-ephemeral-storage-request",
      airbyteWorkerConfig.kubeJobConfigs.find { it.name == "test1" }?.ephemeralStorageRequest,
    )
    assertEquals(50L, airbyteWorkerConfig.replication.persistenceFlushPeriodSec)
    assertEquals(60, airbyteWorkerConfig.replication.dispatcher.nThreads)
  }
}

@MicronautTest(propertySources = ["classpath:application-worker-yaml-list.yml"])
internal class AirbyteWorkerConfigOverridesYamlListTest {
  @Inject
  private lateinit var airbyteWorkerConfig: AirbyteWorkerConfig

  @Test
  fun testLoadingValuesFromConfig() {
    assertEquals(false, airbyteWorkerConfig.check.enabled)
    assertEquals(1, airbyteWorkerConfig.check.maxWorkers)
    assertEquals(false, airbyteWorkerConfig.connection.enabled)
    assertEquals(1, airbyteWorkerConfig.connection.scheduleJitter.noJitterCutoffMinutes)
    assertEquals(10, airbyteWorkerConfig.connection.scheduleJitter.highFrequencyBucket.jitterAmountMinutes)
    assertEquals(20, airbyteWorkerConfig.connection.scheduleJitter.highFrequencyBucket.thresholdMinutes)
    assertEquals(30, airbyteWorkerConfig.connection.scheduleJitter.mediumFrequencyBucket.jitterAmountMinutes)
    assertEquals(40, airbyteWorkerConfig.connection.scheduleJitter.mediumFrequencyBucket.thresholdMinutes)
    assertEquals(50, airbyteWorkerConfig.connection.scheduleJitter.lowFrequencyBucket.jitterAmountMinutes)
    assertEquals(60, airbyteWorkerConfig.connection.scheduleJitter.lowFrequencyBucket.thresholdMinutes)
    assertEquals(70, airbyteWorkerConfig.connection.scheduleJitter.veryLowFrequencyBucket.jitterAmountMinutes)
    assertEquals("10", airbyteWorkerConfig.connectorSidecar.resources.cpuLimit)
    assertEquals("11", airbyteWorkerConfig.connectorSidecar.resources.cpuRequest)
    assertEquals("12", airbyteWorkerConfig.connectorSidecar.resources.memoryLimit)
    assertEquals("13", airbyteWorkerConfig.connectorSidecar.resources.memoryRequest)
    assertEquals(1, airbyteWorkerConfig.discover.autoRefreshWindow)
    assertEquals(false, airbyteWorkerConfig.discover.enabled)
    assertEquals(2, airbyteWorkerConfig.discover.maxWorkers)
    assertEquals("1G", airbyteWorkerConfig.fileTransfer.resources.ephemeralStorageLimit)
    assertEquals("2G", airbyteWorkerConfig.fileTransfer.resources.ephemeralStorageRequest)
    assertEquals(true, airbyteWorkerConfig.isolated.kube.useCustomNodeSelector)
    assertEquals("test-node-selectors", airbyteWorkerConfig.isolated.kube.nodeSelectors)
    assertEquals("test-sentry-dsn", airbyteWorkerConfig.job.errorReporting.sentry.dsn)
    assertEquals(JobErrorReportingStrategy.SENTRY, airbyteWorkerConfig.job.errorReporting.strategy)
    assertEquals("test-connector-image-registry", airbyteWorkerConfig.job.kubernetes.connectorImageRegistry)
    assertEquals("test-init-image", airbyteWorkerConfig.job.kubernetes.init.container.image)
    assertEquals("test-init-image-pull-policy", airbyteWorkerConfig.job.kubernetes.init.container.imagePullPolicy)
    assertEquals(
      listOf("test-init-image-pull-secret", "test-init-image-pull-secret-2"),
      airbyteWorkerConfig.job.kubernetes.init.container.imagePullSecret,
    )
    assertEquals("test-main-image", airbyteWorkerConfig.job.kubernetes.main.container.image)
    assertEquals("test-main-image-pull-policy", airbyteWorkerConfig.job.kubernetes.main.container.imagePullPolicy)
    assertEquals(
      listOf("test-main-image-pull-secret", "test-main-image-pull-secret-2"),
      airbyteWorkerConfig.job.kubernetes.main.container.imagePullSecret,
    )
    assertEquals("test-profiler-image", airbyteWorkerConfig.job.kubernetes.profiler.container.image)
    assertEquals("test-profiler-image-pull-policy", airbyteWorkerConfig.job.kubernetes.profiler.container.imagePullPolicy)
    assertEquals(
      listOf("test-profiler-image-pull-secret", "test-profiler-image-pull-secret-2"),
      airbyteWorkerConfig.job.kubernetes.profiler.container.imagePullSecret,
    )
    assertEquals("2", airbyteWorkerConfig.job.kubernetes.profiler.container.cpuLimit)
    assertEquals("3", airbyteWorkerConfig.job.kubernetes.profiler.container.cpuRequest)
    assertEquals("4", airbyteWorkerConfig.job.kubernetes.profiler.container.memoryLimit)
    assertEquals("5", airbyteWorkerConfig.job.kubernetes.profiler.container.memoryRequest)
    assertEquals("test-sidecar-image", airbyteWorkerConfig.job.kubernetes.sidecar.container.image)
    assertEquals("test-sidecar-image-pull-policy", airbyteWorkerConfig.job.kubernetes.sidecar.container.imagePullPolicy)
    assertEquals(
      listOf("test-sidecar-image-pull-secret", "test-sidecar-image-pull-secret-2"),
      airbyteWorkerConfig.job.kubernetes.sidecar.container.imagePullSecret,
    )
    assertEquals("test-namespace", airbyteWorkerConfig.job.kubernetes.namespace)
    assertEquals("test-service-account", airbyteWorkerConfig.job.kubernetes.serviceAccount)
    assertEquals("test-worker-tolerations", airbyteWorkerConfig.job.kubernetes.tolerations)
    assertEquals("test-data-plane-mount-path", airbyteWorkerConfig.job.kubernetes.volumes.dataPlaneCreds.mountPath)
    assertEquals("test-data-plane-secret-name", airbyteWorkerConfig.job.kubernetes.volumes.dataPlaneCreds.secretName)
    assertEquals(true, airbyteWorkerConfig.job.kubernetes.volumes.local.enabled)
    assertEquals("test-gcs-creds-mount-path", airbyteWorkerConfig.job.kubernetes.volumes.gcsCreds.mountPath)
    assertEquals("test-gcs-creds-secret-name", airbyteWorkerConfig.job.kubernetes.volumes.gcsCreds.secretName)
    assertEquals("test-secret-mount-path", airbyteWorkerConfig.job.kubernetes.volumes.secret.mountPath)
    assertEquals("test-secret-secret-name", airbyteWorkerConfig.job.kubernetes.volumes.secret.secretName)
    assertEquals("test-staging-mount-path", airbyteWorkerConfig.job.kubernetes.volumes.staging.mountPath)
    assertEquals("test-worker-tolerations", airbyteWorkerConfig.job.kubernetes.tolerations)
    assertEquals(false, airbyteWorkerConfig.notify.enabled)
    assertEquals(3, airbyteWorkerConfig.notify.maxWorkers)
    assertEquals(false, airbyteWorkerConfig.spec.enabled)
    assertEquals(4, airbyteWorkerConfig.spec.maxWorkers)
    assertEquals(false, airbyteWorkerConfig.sync.enabled)
    assertEquals(10, airbyteWorkerConfig.sync.maxWorkers)
    assertEquals(6, airbyteWorkerConfig.sync.maxAttempts)
    assertEquals(7, airbyteWorkerConfig.sync.maxTimeout)
    assertEquals(8, airbyteWorkerConfig.sync.maxInitTimeout)
    assertEquals("test-default-annotations", airbyteWorkerConfig.kubeJobConfigs.find { it.name == "default" }?.annotations)
    assertEquals("test-default-labels", airbyteWorkerConfig.kubeJobConfigs.find { it.name == "default" }?.labels)
    assertEquals("test-default-node-selectors", airbyteWorkerConfig.kubeJobConfigs.find { it.name == "default" }?.nodeSelectors)
    assertEquals("test-default-cpu-limit", airbyteWorkerConfig.kubeJobConfigs.find { it.name == "default" }?.cpuLimit)
    assertEquals("test-default-cpu-request", airbyteWorkerConfig.kubeJobConfigs.find { it.name == "default" }?.cpuRequest)
    assertEquals("test-default-memory-limit", airbyteWorkerConfig.kubeJobConfigs.find { it.name == "default" }?.memoryLimit)
    assertEquals("test-default-memory-request", airbyteWorkerConfig.kubeJobConfigs.find { it.name == "default" }?.memoryRequest)
    assertEquals(
      "test-default-ephemeral-storage-limit",
      airbyteWorkerConfig.kubeJobConfigs.find { it.name == "default" }?.ephemeralStorageLimit,
    )
    assertEquals(
      "test-default-ephemeral-storage-request",
      airbyteWorkerConfig.kubeJobConfigs
        .find {
          it.name == "default"
        }?.ephemeralStorageRequest,
    )
    assertEquals("test-test1-annotations", airbyteWorkerConfig.kubeJobConfigs.find { it.name == "test1" }?.annotations)
    assertEquals("test-test1-labels", airbyteWorkerConfig.kubeJobConfigs.find { it.name == "test1" }?.labels)
    assertEquals("test-test1-node-selectors", airbyteWorkerConfig.kubeJobConfigs.find { it.name == "test1" }?.nodeSelectors)
    assertEquals("test-test1-cpu-limit", airbyteWorkerConfig.kubeJobConfigs.find { it.name == "test1" }?.cpuLimit)
    assertEquals("test-test1-cpu-request", airbyteWorkerConfig.kubeJobConfigs.find { it.name == "test1" }?.cpuRequest)
    assertEquals("test-test1-memory-limit", airbyteWorkerConfig.kubeJobConfigs.find { it.name == "test1" }?.memoryLimit)
    assertEquals("test-test1-memory-request", airbyteWorkerConfig.kubeJobConfigs.find { it.name == "test1" }?.memoryRequest)
    assertEquals("test-test1-ephemeral-storage-limit", airbyteWorkerConfig.kubeJobConfigs.find { it.name == "test1" }?.ephemeralStorageLimit)
    assertEquals(
      "test-test1-ephemeral-storage-request",
      airbyteWorkerConfig.kubeJobConfigs.find { it.name == "test1" }?.ephemeralStorageRequest,
    )
    assertEquals(50L, airbyteWorkerConfig.replication.persistenceFlushPeriodSec)
    assertEquals(60, airbyteWorkerConfig.replication.dispatcher.nThreads)
  }
}
