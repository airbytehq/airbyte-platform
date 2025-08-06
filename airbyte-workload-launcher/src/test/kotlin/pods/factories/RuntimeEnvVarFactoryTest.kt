/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher.pods.factories

import io.airbyte.config.Configs.AirbyteEdition
import io.airbyte.config.ConnectionContext
import io.airbyte.config.WorkerEnvConstants
import io.airbyte.config.WorkloadType
import io.airbyte.featureflag.ConcurrentSourceStreamRead
import io.airbyte.featureflag.Connection
import io.airbyte.featureflag.ConnectorApmEnabled
import io.airbyte.featureflag.ContainerOrchestratorJavaOpts
import io.airbyte.featureflag.InjectAwsSecretsToConnectorPods
import io.airbyte.featureflag.ReplicationDebugLogLevelEnabled
import io.airbyte.featureflag.TestClient
import io.airbyte.featureflag.UseAllowCustomCode
import io.airbyte.featureflag.UseRuntimeSecretPersistence
import io.airbyte.featureflag.Workspace
import io.airbyte.persistence.job.models.IntegrationLauncherConfig
import io.airbyte.persistence.job.models.JobRunConfig
import io.airbyte.persistence.job.models.ReplicationInput
import io.airbyte.workers.pod.Metadata.AWS_ASSUME_ROLE_EXTERNAL_ID
import io.airbyte.workload.launcher.constants.EnvVarConstants
import io.airbyte.workload.launcher.helper.ConnectorApmSupportHelper
import io.airbyte.workload.launcher.model.toEnvVarList
import io.airbyte.workload.launcher.pods.ResourceConversionUtils
import io.airbyte.workload.launcher.pods.factories.RuntimeEnvVarFactory.Companion.MYSQL_SOURCE_NAME
import io.airbyte.workload.launcher.pods.factories.RuntimeEnvVarFactoryTest.Fixtures.CONTAINER_ORCH_JAVA_OPTS
import io.airbyte.workload.launcher.pods.factories.RuntimeEnvVarFactoryTest.Fixtures.WORKLOAD_ID
import io.airbyte.workload.launcher.pods.factories.RuntimeEnvVarFactoryTest.Fixtures.connectorAwsAssumedRoleSecretEnvList
import io.airbyte.workload.launcher.pods.factories.RuntimeEnvVarFactoryTest.Fixtures.organizationId
import io.airbyte.workload.launcher.pods.factories.RuntimeEnvVarFactoryTest.Fixtures.workspaceId
import io.fabric8.kubernetes.api.model.EnvVar
import io.mockk.every
import io.mockk.mockk
import io.mockk.spyk
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.CsvSource
import org.junit.jupiter.params.provider.MethodSource
import org.junit.jupiter.params.provider.ValueSource
import org.mockito.ArgumentMatchers.anyList
import java.util.UUID
import java.util.stream.Stream
import io.airbyte.commons.envvar.EnvVar as AirbyteEnvVar
import io.airbyte.config.ResourceRequirements as AirbyteResourceRequirements

class RuntimeEnvVarFactoryTest {
  private lateinit var connectorApmSupportHelper: ConnectorApmSupportHelper

  private lateinit var ffClient: TestClient

  private lateinit var factory: RuntimeEnvVarFactory

  private val stagingMountPath = "/staging-dir"

  private lateinit var airbyteEdition: AirbyteEdition

  @BeforeEach
  fun setup() {
    connectorApmSupportHelper = mockk()
    ffClient = mockk()
    every { ffClient.boolVariation(InjectAwsSecretsToConnectorPods, any()) } returns false
    every { ffClient.boolVariation(UseAllowCustomCode, any()) } returns false
    every { ffClient.boolVariation(ReplicationDebugLogLevelEnabled, any()) } returns false
    airbyteEdition = AirbyteEdition.COMMUNITY

    factory =
      spyk(
        RuntimeEnvVarFactory(
          connectorAwsAssumedRoleSecretEnvList,
          stagingMountPath,
          CONTAINER_ORCH_JAVA_OPTS,
          false,
          "info",
          connectorApmSupportHelper,
          ffClient,
          airbyteEdition,
        ),
      )
  }

  @Test
  fun `does not build aws env vars if custom connector`() {
    val config =
      IntegrationLauncherConfig()
        .withIsCustomConnector(true)
        .withWorkspaceId(workspaceId)

    val result = factory.resolveAwsAssumedRoleEnvVars(config)

    assertTrue(result.isEmpty())
  }

  @Test
  fun `does not build aws env vars if ff disabled`() {
    every { ffClient.boolVariation(InjectAwsSecretsToConnectorPods, any()) } returns false
    val config =
      IntegrationLauncherConfig()
        .withIsCustomConnector(false)
        .withWorkspaceId(workspaceId)

    val result = factory.resolveAwsAssumedRoleEnvVars(config)

    assertTrue(result.isEmpty())
  }

  @Test
  fun `builds aws env vars if ff enabled and airbyte connector`() {
    every { ffClient.boolVariation(InjectAwsSecretsToConnectorPods, any()) } returns true
    val config =
      IntegrationLauncherConfig()
        .withIsCustomConnector(false)
        .withWorkspaceId(workspaceId)

    val result = factory.resolveAwsAssumedRoleEnvVars(config)

    val assumedRoleExternalIdEnvVar = EnvVar(AWS_ASSUME_ROLE_EXTERNAL_ID, workspaceId.toString(), null)
    assertEquals(
      connectorAwsAssumedRoleSecretEnvList + assumedRoleExternalIdEnvVar,
      result,
    )
  }

  @Test
  fun `adds apm env vars if enabled (mutative API)`() {
    val image = "image-name"
    val context = Connection(UUID.randomUUID())
    every { ffClient.boolVariation(ConnectorApmEnabled, context) } returns true

    every { connectorApmSupportHelper.addApmEnvVars(anyList()) } returns Unit
    every { connectorApmSupportHelper.addServerNameAndVersionToEnvVars(image, anyList()) } returns Unit

    factory.getConnectorApmEnvVars(image, context)

    // the API is mutative for some reason which means this is the best we can do
    verify { connectorApmSupportHelper.addApmEnvVars(anyList()) }
    verify { connectorApmSupportHelper.addServerNameAndVersionToEnvVars(image, anyList()) }
  }

  @Test
  fun `does not build apm env vars if disabled (mutative API)`() {
    val image = "image-name"
    val context = Connection(UUID.randomUUID())
    every { ffClient.boolVariation(ConnectorApmEnabled, context) } returns false

    every { connectorApmSupportHelper.addApmEnvVars(anyList()) } returns Unit
    every { connectorApmSupportHelper.addServerNameAndVersionToEnvVars(image, anyList()) } returns Unit

    factory.getConnectorApmEnvVars(image, context)

    // the API is mutative for some reason which means this is the best we can do
    verify(exactly = 0) { connectorApmSupportHelper.addApmEnvVars(anyList()) }
    verify(exactly = 0) { connectorApmSupportHelper.addServerNameAndVersionToEnvVars(image, anyList()) }
  }

  @Test
  fun `builds metadata env vars`() {
    val image = "docker-image-name"
    val jobId = "3145"
    val attemptId = 7L
    val config =
      IntegrationLauncherConfig()
        .withDockerImage(image)
        .withJobId(jobId)
        .withAttemptId(attemptId)

    val result = factory.getMetadataEnvVars(config)

    val expected =
      listOf(
        EnvVar(DEPLOYMENT_MODE, OSS_DEPLOYMENT_MODE, null),
        EnvVar(WorkerEnvConstants.WORKER_CONNECTOR_IMAGE, image, null),
        EnvVar(WorkerEnvConstants.WORKER_JOB_ID, jobId, null),
        EnvVar(WorkerEnvConstants.WORKER_JOB_ATTEMPT, attemptId.toString(), null),
      )

    assertEquals(expected, result)
  }

  @ParameterizedTest
  @CsvSource(
    value = [
      "CLOUD,CLOUD",
      "COMMUNITY,OSS",
      "ENTERPRISE,OSS",
    ],
  )
  fun `builds metadata env vars for each edition`(
    airbyteEdition: AirbyteEdition,
    expectedDeploymentMode: String,
  ) {
    val image = "docker-image-name"
    val jobId = "3145"
    val attemptId = 7L
    val config =
      IntegrationLauncherConfig()
        .withDockerImage(image)
        .withJobId(jobId)
        .withAttemptId(attemptId)

    factory =
      spyk(
        RuntimeEnvVarFactory(
          connectorAwsAssumedRoleSecretEnvList,
          stagingMountPath,
          CONTAINER_ORCH_JAVA_OPTS,
          false,
          "info",
          connectorApmSupportHelper,
          ffClient,
          airbyteEdition,
        ),
      )
    val result = factory.getMetadataEnvVars(config)

    val expected =
      listOf(
        EnvVar(DEPLOYMENT_MODE, expectedDeploymentMode, null),
        EnvVar(WorkerEnvConstants.WORKER_CONNECTOR_IMAGE, image, null),
        EnvVar(WorkerEnvConstants.WORKER_JOB_ID, jobId, null),
        EnvVar(WorkerEnvConstants.WORKER_JOB_ATTEMPT, attemptId.toString(), null),
      )

    assertEquals(expected, result)
  }

  @Test
  fun `builds connector configuration env vars with file transfer (concurrentStreamReadEnabled)`() {
    every { ffClient.boolVariation(ConcurrentSourceStreamRead, any()) } returns false

    val result = factory.getConfigurationEnvVars("img", UUID.randomUUID(), true)

    val expected =
      listOf(
        EnvVar(EnvVarConstants.USE_STREAM_CAPABLE_STATE_ENV_VAR, "true", null),
        EnvVar(EnvVarConstants.USE_FILE_TRANSFER, "true", null),
        EnvVar(EnvVarConstants.AIRBYTE_STAGING_DIRECTORY, stagingMountPath, null),
        EnvVar(EnvVarConstants.CONCURRENT_SOURCE_STREAM_READ_ENV_VAR, "false", null),
      )

    assertEquals(expected, result)
  }

  @ParameterizedTest
  @MethodSource("concurrentStreamReadEnabledMatrix")
  fun `builds connector configuration env vars (concurrentStreamReadEnabled)`(
    ffEnabled: Boolean,
    sourceImageName: String,
  ) {
    every { ffClient.boolVariation(ConcurrentSourceStreamRead, any()) } returns ffEnabled

    val result = factory.getConfigurationEnvVars(sourceImageName, UUID.randomUUID(), false)

    val expected =
      listOf(
        EnvVar(EnvVarConstants.USE_STREAM_CAPABLE_STATE_ENV_VAR, "true", null),
        EnvVar(EnvVarConstants.USE_FILE_TRANSFER, "false", null),
        EnvVar(EnvVarConstants.CONCURRENT_SOURCE_STREAM_READ_ENV_VAR, "true", null),
      )

    assertEquals(expected, result)
  }

  @ParameterizedTest
  @MethodSource("concurrentStreamReadDisabledMatrix")
  fun `builds connector configuration env vars (concurrentStreamReadDisabled)`(
    ffEnabled: Boolean,
    sourceImageName: String,
  ) {
    every { ffClient.boolVariation(ConcurrentSourceStreamRead, any()) } returns ffEnabled

    val result = factory.getConfigurationEnvVars(sourceImageName, UUID.randomUUID(), false)

    val expected =
      listOf(
        EnvVar(EnvVarConstants.USE_STREAM_CAPABLE_STATE_ENV_VAR, "true", null),
        EnvVar(EnvVarConstants.USE_FILE_TRANSFER, "false", null),
        EnvVar(EnvVarConstants.CONCURRENT_SOURCE_STREAM_READ_ENV_VAR, "false", null),
      )

    assertEquals(expected, result)
  }

  @ParameterizedTest
  @ValueSource(strings = ["1G", "2Mi", "5G", "10G", "87Ki"])
  fun `builds connector resource requirement env vars `(kubeStorageLimit: String) {
    val reqs =
      AirbyteResourceRequirements()
        .withEphemeralStorageLimit(kubeStorageLimit)

    val result = factory.getResourceEnvVars(reqs)

    val expectedStorageBytes = ResourceConversionUtils.kubeQuantityStringToBytes(kubeStorageLimit)

    val expected =
      listOf(
        EnvVar(EnvVarConstants.CONNECTOR_STORAGE_LIMIT_BYTES, expectedStorageBytes.toString(), null),
      )

    assertEquals(expected, result)
  }

  @ParameterizedTest
  @MethodSource("additionalEnvironmentVariablesMatrix")
  fun `builds expected env vars for replication connector container`(passThroughEnvMap: Map<String, String>?) {
    val awsEnvVars = listOf(EnvVar("aws-var", "1", null))
    val apmEnvVars = listOf(EnvVar("apm-var", "2", null))
    val configurationEnvVars = listOf(EnvVar("config-var", "3", null))
    val metadataEnvVars = listOf(EnvVar("metadata-var", "4", null))
    val resourceEnvVars = listOf(EnvVar("resource-var", "5", null))
    val customCodeEnvVars = listOf(EnvVar("custom-code-var", "6", null))
    val debugLogLevelEnvVars = listOf(EnvVar("debug-log-var", "7", null))
    val passThroughVars = passThroughEnvMap?.toEnvVarList().orEmpty()
    every { factory.resolveAwsAssumedRoleEnvVars(any()) } returns awsEnvVars
    every { factory.getConnectorApmEnvVars(any(), any()) } returns apmEnvVars
    every { factory.getConfigurationEnvVars(any(), any(), any()) } returns configurationEnvVars
    every { factory.getMetadataEnvVars(any()) } returns metadataEnvVars
    every { factory.getResourceEnvVars(any()) } returns resourceEnvVars
    every { factory.getDeclarativeCustomCodeSupportEnvVars(any()) } returns customCodeEnvVars
    every { factory.getLogLevelEnvVars(any()) } returns debugLogLevelEnvVars

    val config =
      IntegrationLauncherConfig()
        .withAdditionalEnvironmentVariables(passThroughEnvMap)
        .withDockerImage("image-name")
        .withWorkspaceId(workspaceId)
        .withConnectionId(UUID.randomUUID())

    val resourceReqs = AirbyteResourceRequirements()

    val result = factory.replicationConnectorEnvVars(config, resourceReqs, false)

    val expected =
      awsEnvVars + apmEnvVars + configurationEnvVars + metadataEnvVars + resourceEnvVars + passThroughVars + customCodeEnvVars + debugLogLevelEnvVars

    assertEquals(expected, result)
  }

  @ParameterizedTest
  @ValueSource(booleans = [true, false])
  fun `builds expected env vars for check connector container`(flagValue: Boolean) {
    every { factory.resolveAwsAssumedRoleEnvVars(any()) } returns connectorAwsAssumedRoleSecretEnvList
    every { ffClient.boolVariation(UseRuntimeSecretPersistence, any()) } returns flagValue
    val config =
      IntegrationLauncherConfig()
        .withWorkspaceId(workspaceId)
    val result = factory.checkConnectorEnvVars(config, organizationId, WORKLOAD_ID)

    assertEquals(
      connectorAwsAssumedRoleSecretEnvList +
        EnvVar(EnvVarConstants.USE_RUNTIME_SECRET_PERSISTENCE, flagValue.toString(), null) +
        EnvVar(AirbyteEnvVar.AIRBYTE_ENABLE_UNSAFE_CODE.toString(), false.toString(), null) +
        EnvVar(AirbyteEnvVar.OPERATION_TYPE.toString(), WorkloadType.CHECK.toString(), null) +
        EnvVar(AirbyteEnvVar.WORKLOAD_ID.toString(), WORKLOAD_ID, null),
      result,
    )
  }

  @ParameterizedTest
  @ValueSource(booleans = [true, false])
  fun `builds expected env vars for discover connector container`(flagValue: Boolean) {
    every { factory.resolveAwsAssumedRoleEnvVars(any()) } returns connectorAwsAssumedRoleSecretEnvList
    every { ffClient.boolVariation(UseRuntimeSecretPersistence, any()) } returns flagValue
    val config =
      IntegrationLauncherConfig()
        .withWorkspaceId(workspaceId)
    val result = factory.discoverConnectorEnvVars(config, organizationId, WORKLOAD_ID)

    assertEquals(
      connectorAwsAssumedRoleSecretEnvList +
        EnvVar(EnvVarConstants.USE_RUNTIME_SECRET_PERSISTENCE, flagValue.toString(), null) +
        EnvVar(AirbyteEnvVar.AIRBYTE_ENABLE_UNSAFE_CODE.toString(), false.toString(), null) +
        EnvVar(AirbyteEnvVar.OPERATION_TYPE.toString(), WorkloadType.DISCOVER.toString(), null) +
        EnvVar(AirbyteEnvVar.WORKLOAD_ID.toString(), WORKLOAD_ID, null),
      result,
    )
  }

  @Test
  fun `builds expected env vars for spec connector container`() {
    val config =
      IntegrationLauncherConfig()
        .withWorkspaceId(workspaceId)

    val result = factory.specConnectorEnvVars(config, WORKLOAD_ID)

    assertEquals(
      listOf(
        EnvVar(AirbyteEnvVar.AIRBYTE_ENABLE_UNSAFE_CODE.toString(), false.toString(), null),
        EnvVar(AirbyteEnvVar.OPERATION_TYPE.toString(), WorkloadType.SPEC.toString(), null),
        EnvVar(AirbyteEnvVar.WORKLOAD_ID.toString(), WORKLOAD_ID, null),
      ),
      result,
    )
  }

  @ParameterizedTest
  @CsvSource(
    "true, true, true",
    "true, false, true",
    "false, true, true",
    "false, false, false",
  )
  fun `builds expected env vars for getDeclarativeCustomCodeSupportEnvVars`(
    useAllowCustomCode: Boolean,
    globalOverride: Boolean,
    expectedEnvValue: Boolean,
  ) {
    every { ffClient.boolVariation(UseAllowCustomCode, any()) } returns useAllowCustomCode

    val envFactory =
      spyk(
        RuntimeEnvVarFactory(
          connectorAwsAssumedRoleSecretEnvList,
          stagingMountPath,
          CONTAINER_ORCH_JAVA_OPTS,
          globalOverride,
          "info",
          connectorApmSupportHelper,
          ffClient,
          airbyteEdition,
        ),
      )

    val result = envFactory.getDeclarativeCustomCodeSupportEnvVars(Workspace(workspaceId))

    assertEquals(
      listOf(EnvVar(AirbyteEnvVar.AIRBYTE_ENABLE_UNSAFE_CODE.toString(), expectedEnvValue.toString(), null)),
      result,
    )
  }

  @ParameterizedTest
  @MethodSource("orchestratorEnvVarMatrix")
  fun `builds expected env vars for orchestrator container`(
    optsOverride: String,
    expectedOpts: String,
    useFileTransfer: Boolean,
    useRuntimeSecretPersistence: Boolean,
  ) {
    every { ffClient.stringVariation(ContainerOrchestratorJavaOpts, any()) } returns optsOverride
    every { ffClient.boolVariation(UseRuntimeSecretPersistence, any()) } returns useRuntimeSecretPersistence
    val connectionId = UUID.randomUUID()
    val jobRunConfig =
      JobRunConfig()
        .withJobId("2324")
        .withAttemptId(1)
    val input =
      ReplicationInput()
        .withJobRunConfig(jobRunConfig)
        .withConnectionId(connectionId)
        .withUseFileTransfer(useFileTransfer)
        .withConnectionContext(ConnectionContext().withOrganizationId(UUID.randomUUID()).withWorkspaceId(workspaceId).withConnectionId(connectionId))
        .withSourceLauncherConfig(IntegrationLauncherConfig().withWorkspaceId(workspaceId))
    val result = factory.orchestratorEnvVars(input, WORKLOAD_ID)

    assertEquals(
      listOf(
        EnvVar(AirbyteEnvVar.OPERATION_TYPE.toString(), WorkloadType.SYNC.toString(), null),
        EnvVar(AirbyteEnvVar.WORKLOAD_ID.toString(), WORKLOAD_ID, null),
        EnvVar(AirbyteEnvVar.JOB_ID.toString(), jobRunConfig.jobId, null),
        EnvVar(AirbyteEnvVar.ATTEMPT_ID.toString(), jobRunConfig.attemptId.toString(), null),
        EnvVar(AirbyteEnvVar.CONNECTION_ID.toString(), input.connectionId.toString(), null),
        EnvVar(EnvVarConstants.USE_FILE_TRANSFER, useFileTransfer.toString(), null),
        EnvVar(EnvVarConstants.JAVA_OPTS_ENV_VAR, expectedOpts, null),
        EnvVar(EnvVarConstants.AIRBYTE_STAGING_DIRECTORY, stagingMountPath, null),
        EnvVar(EnvVarConstants.USE_RUNTIME_SECRET_PERSISTENCE, useRuntimeSecretPersistence.toString(), null),
        EnvVar(EnvVarConstants.LOG_LEVEL, "info", null),
      ),
      result,
    )
  }

  @Test
  fun `builds debug log level env vars when feature flag is enabled`() {
    val context = Workspace(workspaceId)
    every { ffClient.boolVariation(ReplicationDebugLogLevelEnabled, context) } returns true

    val result = factory.getLogLevelEnvVars(context)

    assertEquals(
      listOf(EnvVar(EnvVarConstants.LOG_LEVEL, "debug", null)),
      result,
    )
  }

  @Test
  fun `returns list with default log level when debug log level feature flag is disabled`() {
    val context = Workspace(workspaceId)
    every { ffClient.boolVariation(ReplicationDebugLogLevelEnabled, context) } returns false

    val result = factory.getLogLevelEnvVars(context)

    assertEquals(
      listOf(EnvVar(EnvVarConstants.LOG_LEVEL, "info", null)),
      result,
    )
  }

  object Fixtures {
    const val WORKLOAD_ID = "test-workload-id"
    const val CONTAINER_ORCH_JAVA_OPTS = "OPTS"
    val organizationId = UUID.randomUUID()!!
    val workspaceId = UUID.randomUUID()!!
    val connectorAwsAssumedRoleSecretEnvList = listOf(EnvVar("test", "creds", null))
  }

  companion object {
    @JvmStatic
    private fun concurrentStreamReadEnabledMatrix(): Stream<Arguments> =
      Stream.of(
        Arguments.of(true, MYSQL_SOURCE_NAME),
        Arguments.of(true, MYSQL_SOURCE_NAME + "asdf"),
      )

    @JvmStatic
    private fun concurrentStreamReadDisabledMatrix(): Stream<Arguments> =
      Stream.of(
        Arguments.of(true, "anything else"),
        Arguments.of(true, "asdf$MYSQL_SOURCE_NAME"),
        Arguments.of(false, MYSQL_SOURCE_NAME),
        Arguments.of(false, "${MYSQL_SOURCE_NAME}asdf"),
        Arguments.of(false, "anything else"),
      )

    @JvmStatic
    private fun additionalEnvironmentVariablesMatrix(): Stream<Arguments> =
      Stream.of(
        Arguments.of(null),
        Arguments.of(mapOf("key-1" to "value-1")),
        Arguments.of(mapOf("key-1" to "value-1", "key-2" to "value-2")),
      )

    @JvmStatic
    private fun orchestratorEnvVarMatrix(): Stream<Arguments> =
      Stream.of(
        Arguments.of(" ", CONTAINER_ORCH_JAVA_OPTS, true, false),
        Arguments.of("", CONTAINER_ORCH_JAVA_OPTS, false, true),
        Arguments.of("opts 1", "opts 1", true, true),
        Arguments.of("opts 2", "opts 2", false, false),
        Arguments.of("opts 3 ", "opts 3", true, false),
        Arguments.of("  opts 4  ", "opts 4", true, false),
      )
  }
}
