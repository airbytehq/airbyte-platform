/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher.pods.factories

import io.airbyte.commons.annotation.InternalForTesting
import io.airbyte.config.Configs
import io.airbyte.config.WorkerEnvConstants
import io.airbyte.config.WorkloadType
import io.airbyte.featureflag.ANONYMOUS
import io.airbyte.featureflag.ConcurrentSourceStreamRead
import io.airbyte.featureflag.Connection
import io.airbyte.featureflag.ConnectorApmEnabled
import io.airbyte.featureflag.ContainerOrchestratorJavaOpts
import io.airbyte.featureflag.Context
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.featureflag.InjectAwsSecretsToConnectorPods
import io.airbyte.featureflag.Organization
import io.airbyte.featureflag.UseAllowCustomCode
import io.airbyte.featureflag.UseRuntimeSecretPersistence
import io.airbyte.featureflag.Workspace
import io.airbyte.persistence.job.models.IntegrationLauncherConfig
import io.airbyte.persistence.job.models.ReplicationInput
import io.airbyte.workers.input.getAttemptId
import io.airbyte.workers.input.getJobId
import io.airbyte.workers.pod.Metadata.AWS_ASSUME_ROLE_EXTERNAL_ID
import io.airbyte.workload.launcher.constants.EnvVarConstants
import io.airbyte.workload.launcher.helper.ConnectorApmSupportHelper
import io.airbyte.workload.launcher.model.toEnvVarList
import io.airbyte.workload.launcher.pods.ResourceConversionUtils
import io.fabric8.kubernetes.api.model.EnvVar
import io.micronaut.context.annotation.Value
import jakarta.inject.Named
import jakarta.inject.Singleton
import java.util.UUID
import kotlin.collections.plus
import io.airbyte.commons.envvar.EnvVar as AirbyteEnvVar
import io.airbyte.config.ResourceRequirements as AirbyteResourceRequirements

/**
 * Legacy deployment mode environment variable that is still used by some connectors.
 */
internal const val CLOUD_DEPLOYMENT_MODE = "CLOUD"
internal const val DEPLOYMENT_MODE = "DEPLOYMENT_MODE"
internal const val OSS_DEPLOYMENT_MODE = "OSS"

/**
 * Performs dynamic mapping of config to env vars based on runtime inputs.
 *
 * For static stat time configuration see EnvVarConfigBeanFactory.
 */
@Singleton
class RuntimeEnvVarFactory(
  @Named("connectorAwsAssumedRoleSecretEnv") private val connectorAwsAssumedRoleSecretEnvList: List<EnvVar>,
  @Value("\${airbyte.worker.job.kube.volumes.staging.mount-path}") private val stagingMountPath: String,
  @Value("\${airbyte.container.orchestrator.java-opts}") private val containerOrchestratorJavaOpts: String,
  @Value("\${airbyte.container.orchestrator.enable-unsafe-code}") private val enableUnsafeCodeGlobalOverride: Boolean,
  private val connectorApmSupportHelper: ConnectorApmSupportHelper,
  private val featureFlagClient: FeatureFlagClient,
  private val airbyteEdition: Configs.AirbyteEdition,
) {
  internal fun orchestratorEnvVars(
    replicationInput: ReplicationInput,
    workloadId: String,
  ): List<EnvVar> {
    val optionsOverride: String = featureFlagClient.stringVariation(ContainerOrchestratorJavaOpts, Connection(replicationInput.connectionId))
    val javaOpts = optionsOverride.trim().ifEmpty { containerOrchestratorJavaOpts }
    val secretPersistenceEnvVars = getSecretPersistenceEnvVars(replicationInput.connectionContext.organizationId)
    val useFileTransferEnvVar =
      replicationInput.useFileTransfer == true &&
        (replicationInput.omitFileTransferEnvVar == null || replicationInput.omitFileTransferEnvVar == false)

    return listOf(
      EnvVar(AirbyteEnvVar.OPERATION_TYPE.toString(), WorkloadType.SYNC.toString(), null),
      EnvVar(AirbyteEnvVar.WORKLOAD_ID.toString(), workloadId, null),
      EnvVar(AirbyteEnvVar.JOB_ID.toString(), replicationInput.getJobId(), null),
      EnvVar(AirbyteEnvVar.ATTEMPT_ID.toString(), replicationInput.getAttemptId().toString(), null),
      EnvVar(AirbyteEnvVar.CONNECTION_ID.toString(), replicationInput.connectionId.toString(), null),
      EnvVar(EnvVarConstants.USE_FILE_TRANSFER, useFileTransferEnvVar.toString(), null),
      EnvVar(EnvVarConstants.JAVA_OPTS_ENV_VAR, javaOpts, null),
      EnvVar(EnvVarConstants.AIRBYTE_STAGING_DIRECTORY, stagingMountPath, null),
    ) + secretPersistenceEnvVars
  }

  fun replicationConnectorEnvVars(
    launcherConfig: IntegrationLauncherConfig,
    resourceReqs: AirbyteResourceRequirements?,
    useFileTransfers: Boolean,
  ): List<EnvVar> {
    val awsEnvVars = resolveAwsAssumedRoleEnvVars(launcherConfig)
    val apmEnvVars = getConnectorApmEnvVars(launcherConfig.dockerImage, Workspace(launcherConfig.workspaceId))
    val configurationEnvVars = getConfigurationEnvVars(launcherConfig.dockerImage, launcherConfig.connectionId ?: ANONYMOUS, useFileTransfers)
    val metadataEnvVars = getMetadataEnvVars(launcherConfig)
    val resourceEnvVars = getResourceEnvVars(resourceReqs)
    val customCodeEnvVars = getDeclarativeCustomCodeSupportEnvVars(Workspace(launcherConfig.workspaceId))
    val configPassThroughEnv = launcherConfig.additionalEnvironmentVariables?.toEnvVarList().orEmpty()

    return awsEnvVars + apmEnvVars + configurationEnvVars + metadataEnvVars + resourceEnvVars + configPassThroughEnv + customCodeEnvVars
  }

  // TODO: Separate env factory methods per container (init, sidecar, main, etc.)
  fun checkConnectorEnvVars(
    launcherConfig: IntegrationLauncherConfig,
    organizationId: UUID,
    workloadId: String,
  ): List<EnvVar> =
    resolveAwsAssumedRoleEnvVars(launcherConfig) +
      getSecretPersistenceEnvVars(organizationId) +
      getDeclarativeCustomCodeSupportEnvVars(Workspace(launcherConfig.workspaceId)) +
      EnvVar(AirbyteEnvVar.OPERATION_TYPE.toString(), WorkloadType.CHECK.toString(), null) +
      EnvVar(AirbyteEnvVar.WORKLOAD_ID.toString(), workloadId, null)

  // TODO: Separate env factory methods per container (init, sidecar, main, etc.)
  fun discoverConnectorEnvVars(
    launcherConfig: IntegrationLauncherConfig,
    organizationId: UUID,
    workloadId: String,
  ): List<EnvVar> =
    resolveAwsAssumedRoleEnvVars(launcherConfig) +
      getSecretPersistenceEnvVars(organizationId) +
      getDeclarativeCustomCodeSupportEnvVars(Workspace(launcherConfig.workspaceId)) +
      EnvVar(AirbyteEnvVar.OPERATION_TYPE.toString(), WorkloadType.DISCOVER.toString(), null) +
      EnvVar(AirbyteEnvVar.WORKLOAD_ID.toString(), workloadId, null)

  // TODO: Separate env factory methods per container (init, sidecar, main, etc.)
  fun specConnectorEnvVars(
    launcherConfig: IntegrationLauncherConfig,
    workloadId: String,
  ): List<EnvVar> =
    getDeclarativeCustomCodeSupportEnvVars(Workspace(launcherConfig.workspaceId)) +
      EnvVar(AirbyteEnvVar.OPERATION_TYPE.toString(), WorkloadType.SPEC.toString(), null) +
      EnvVar(AirbyteEnvVar.WORKLOAD_ID.toString(), workloadId, null)

  /**
   * Env vars to enable APM metrics for the connector if enabled.
   */
  @InternalForTesting
  internal fun getConnectorApmEnvVars(
    image: String,
    context: Context,
  ): List<EnvVar> {
    val connectorApmEnvVars = mutableListOf<EnvVar>()
    if (featureFlagClient.boolVariation(ConnectorApmEnabled, context)) {
      connectorApmSupportHelper.addApmEnvVars(connectorApmEnvVars)
      connectorApmSupportHelper.addServerNameAndVersionToEnvVars(image, connectorApmEnvVars)
    }
    return connectorApmEnvVars.toList()
  }

  /**
   * Metadata env vars. Unsure of purpose. Copied from AirbyteIntegrationLauncher.
   */
  @InternalForTesting
  internal fun getMetadataEnvVars(launcherConfig: IntegrationLauncherConfig): List<EnvVar> =
    listOf(
      // Connectors still rely on the DEPLOYMENT_MODE env var, so map the Airbyte edition to it.
      EnvVar(DEPLOYMENT_MODE, if (airbyteEdition == Configs.AirbyteEdition.CLOUD) CLOUD_DEPLOYMENT_MODE else OSS_DEPLOYMENT_MODE, null),
      EnvVar(WorkerEnvConstants.WORKER_CONNECTOR_IMAGE, launcherConfig.dockerImage, null),
      EnvVar(WorkerEnvConstants.WORKER_JOB_ID, launcherConfig.jobId, null),
      EnvVar(WorkerEnvConstants.WORKER_JOB_ATTEMPT, launcherConfig.attemptId.toString(), null),
    )

  /**
   * Env vars that specify the resource limits of the connectors. For use by the connectors.
   */
  @InternalForTesting
  internal fun getResourceEnvVars(resourceReqs: AirbyteResourceRequirements?): List<EnvVar> {
    val envList = mutableListOf<EnvVar>()
    if (resourceReqs?.ephemeralStorageLimit != null) {
      val bytes = ResourceConversionUtils.kubeQuantityStringToBytes(resourceReqs.ephemeralStorageLimit)
      bytes.let {
        envList.add(EnvVar(EnvVarConstants.CONNECTOR_STORAGE_LIMIT_BYTES, it.toString(), null))
      }
    }

    return envList
  }

  /**
   * These theoretically configure runtime connector behavior. Copied from AirbyteIntegrationLauncher.
   * Unsure if still necessary.
   */
  @InternalForTesting
  internal fun getConfigurationEnvVars(
    dockerImage: String,
    connectionId: UUID,
    useFileTransfers: Boolean,
  ): List<EnvVar> {
    val envVars = mutableListOf<EnvVar>()
    envVars.add(EnvVar(EnvVarConstants.USE_STREAM_CAPABLE_STATE_ENV_VAR, true.toString(), null))
    envVars.add(EnvVar(EnvVarConstants.USE_FILE_TRANSFER, useFileTransfers.toString(), null))
    if (useFileTransfers) {
      envVars.add(EnvVar(EnvVarConstants.AIRBYTE_STAGING_DIRECTORY, stagingMountPath, null))
    }
    val concurrentSourceStreamReadEnabled =
      dockerImage.startsWith(MYSQL_SOURCE_NAME) &&
        featureFlagClient.boolVariation(ConcurrentSourceStreamRead, Connection(connectionId))

    envVars.add(EnvVar(EnvVarConstants.CONCURRENT_SOURCE_STREAM_READ_ENV_VAR, concurrentSourceStreamReadEnabled.toString(), null))

    return envVars
  }

  /**
   * Env vars for controlling runtime secrets hydration behavior.
   */
  @InternalForTesting
  internal fun getSecretPersistenceEnvVars(organizationId: UUID): List<EnvVar> {
    val useRuntimeSecretPersistence = featureFlagClient.boolVariation(UseRuntimeSecretPersistence, Organization(organizationId))

    return listOf(
      EnvVar(EnvVarConstants.USE_RUNTIME_SECRET_PERSISTENCE, useRuntimeSecretPersistence.toString(), null),
    )
  }

  /**
   * Env vars for controlling runtime custom code execution behaviour.
   */
  @InternalForTesting
  internal fun getDeclarativeCustomCodeSupportEnvVars(context: Context): List<EnvVar> {
    val useAllowCustomCode = featureFlagClient.boolVariation(UseAllowCustomCode, context)

    // Turn on unsafe code execution if the feature flag is enabled or the global override is set.
    // PROBLEM TO WORKAROUND: We do not haven't the ability to set feature flags for OSS/Enterprise customers.
    // HACK: We in stead use a global override in the form of an environment variable AIRBYTE_ENABLE_UNSAFE_CODE.
    //       Any customer can set this environment variable to enable unsafe code execution.
    // WHEN TO REMOVE: When we have the ability to set feature flags for OSS/Enterprise customers.
    val useUnsafeCode = useAllowCustomCode || enableUnsafeCodeGlobalOverride

    return listOf(
      EnvVar(EnvVarConstants.AIRBYTE_ENABLE_UNSAFE_CODE_ENV_VAR, useUnsafeCode.toString(), null),
    )
  }

  /**
   * Conditionally adds AWS assumed role env vars for use by connector pods.
   */
  @InternalForTesting
  internal fun resolveAwsAssumedRoleEnvVars(launcherConfig: IntegrationLauncherConfig): List<EnvVar> {
    // Only inject into connectors we own.
    if (launcherConfig.isCustomConnector) {
      return listOf()
    }
    // Only inject into enabled workspaces.
    val workspaceEnabled =
      launcherConfig.workspaceId != null &&
        this.featureFlagClient.boolVariation(InjectAwsSecretsToConnectorPods, Workspace(launcherConfig.workspaceId))
    if (!workspaceEnabled) {
      return listOf()
    }

    val externalIdVar = EnvVar(AWS_ASSUME_ROLE_EXTERNAL_ID, launcherConfig.workspaceId.toString(), null)

    return connectorAwsAssumedRoleSecretEnvList + externalIdVar
  }

  companion object {
    internal const val MYSQL_SOURCE_NAME = "airbyte/source-mysql"
  }
}
