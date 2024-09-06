package io.airbyte.workload.launcher.pods.factories

import com.google.common.annotations.VisibleForTesting
import io.airbyte.config.WorkerEnvConstants
import io.airbyte.config.WorkloadType
import io.airbyte.featureflag.ANONYMOUS
import io.airbyte.featureflag.ConcurrentSourceStreamRead
import io.airbyte.featureflag.Connection
import io.airbyte.featureflag.ConnectorApmEnabled
import io.airbyte.featureflag.Context
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.featureflag.InjectAwsSecretsToConnectorPods
import io.airbyte.featureflag.Workspace
import io.airbyte.persistence.job.models.IntegrationLauncherConfig
import io.airbyte.workers.helper.ConnectorApmSupportHelper
import io.airbyte.workers.process.Metadata.AWS_ASSUME_ROLE_EXTERNAL_ID
import io.airbyte.workload.launcher.constants.EnvVarConstants
import io.airbyte.workload.launcher.model.toEnvVarList
import io.fabric8.kubernetes.api.model.EnvVar
import jakarta.inject.Named
import jakarta.inject.Singleton
import java.util.UUID
import io.airbyte.commons.envvar.EnvVar as AirbyteEnvVar

/**
 * Performs dynamic mapping of config to env vars based on runtime inputs.
 * For static stat time configuration see EnvVarConfigBeanFactory.
 */
@Singleton
class RuntimeEnvVarFactory(
  @Named("connectorAwsAssumedRoleSecretEnv") private val connectorAwsAssumedRoleSecretEnvList: List<EnvVar>,
  private val connectorApmSupportHelper: ConnectorApmSupportHelper,
  private val featureFlagClient: FeatureFlagClient,
) {
  fun replicationConnectorEnvVars(launcherConfig: IntegrationLauncherConfig): List<EnvVar> {
    val awsEnvVars = resolveAwsAssumedRoleEnvVars(launcherConfig)
    val apmEnvVars = getConnectorApmEnvVars(launcherConfig.dockerImage, Workspace(launcherConfig.workspaceId))
    val configurationEnvVars = getConfigurationEnvVars(launcherConfig.dockerImage, launcherConfig.connectionId ?: ANONYMOUS)
    val metadataEnvVars = getMetadataEnvVars(launcherConfig)
    val configPassThroughEnv = launcherConfig.additionalEnvironmentVariables?.toEnvVarList().orEmpty()

    return awsEnvVars + apmEnvVars + configurationEnvVars + metadataEnvVars + configPassThroughEnv
  }

  fun checkConnectorEnvVars(
    launcherConfig: IntegrationLauncherConfig,
    workloadId: String,
  ): List<EnvVar> {
    return resolveAwsAssumedRoleEnvVars(launcherConfig) +
      EnvVar(AirbyteEnvVar.OPERATION_TYPE.toString(), WorkloadType.CHECK.toString(), null) +
      EnvVar(AirbyteEnvVar.WORKLOAD_ID.toString(), workloadId, null)
  }

  fun discoverConnectorEnvVars(
    launcherConfig: IntegrationLauncherConfig,
    workloadId: String,
  ): List<EnvVar> {
    return resolveAwsAssumedRoleEnvVars(launcherConfig) +
      EnvVar(AirbyteEnvVar.OPERATION_TYPE.toString(), WorkloadType.DISCOVER.toString(), null) +
      EnvVar(AirbyteEnvVar.WORKLOAD_ID.toString(), workloadId, null)
  }

  fun specConnectorEnvVars(workloadId: String): List<EnvVar> {
    return listOf(
      EnvVar(AirbyteEnvVar.OPERATION_TYPE.toString(), WorkloadType.SPEC.toString(), null),
      EnvVar(AirbyteEnvVar.WORKLOAD_ID.toString(), workloadId, null),
    )
  }

  /**
   * Env vars to enable APM metrics for the connector if enabled.
   */
  @VisibleForTesting
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
  @VisibleForTesting
  internal fun getMetadataEnvVars(launcherConfig: IntegrationLauncherConfig): List<EnvVar> {
    return listOf(
      EnvVar(WorkerEnvConstants.WORKER_CONNECTOR_IMAGE, launcherConfig.dockerImage, null),
      EnvVar(WorkerEnvConstants.WORKER_JOB_ID, launcherConfig.jobId, null),
      EnvVar(WorkerEnvConstants.WORKER_JOB_ATTEMPT, launcherConfig.attemptId.toString(), null),
    )
  }

  /**
   * These theoretically configure runtime connector behavior. Copied from AirbyteIntegrationLauncher.
   * Unsure if still necessary.
   */
  @VisibleForTesting
  internal fun getConfigurationEnvVars(
    dockerImage: String,
    connectionId: UUID,
  ): List<EnvVar> {
    val envVars = mutableListOf<EnvVar>()
    envVars.add(EnvVar(EnvVarConstants.USE_STREAM_CAPABLE_STATE_ENV_VAR, true.toString(), null))

    val concurrentSourceStreamReadEnabled =
      dockerImage.startsWith(MYSQL_SOURCE_NAME) &&
        featureFlagClient.boolVariation(ConcurrentSourceStreamRead, Connection(connectionId))

    envVars.add(EnvVar(EnvVarConstants.CONCURRENT_SOURCE_STREAM_READ_ENV_VAR, concurrentSourceStreamReadEnabled.toString(), null))

    return envVars
  }

  /**
   * Conditionally adds AWS assumed role env vars for use by connector pods.
   */
  @VisibleForTesting
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
    const val MYSQL_SOURCE_NAME = "airbyte/source-mysql"
  }
}
