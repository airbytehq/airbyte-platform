package io.airbyte.workload.launcher.pods.factories

import io.airbyte.config.WorkerEnvConstants
import io.airbyte.config.WorkloadType
import io.airbyte.featureflag.ConcurrentSourceStreamRead
import io.airbyte.featureflag.Connection
import io.airbyte.featureflag.ConnectorApmEnabled
import io.airbyte.featureflag.InjectAwsSecretsToConnectorPods
import io.airbyte.featureflag.TestClient
import io.airbyte.persistence.job.models.IntegrationLauncherConfig
import io.airbyte.workers.helper.ConnectorApmSupportHelper
import io.airbyte.workers.pod.Metadata.AWS_ASSUME_ROLE_EXTERNAL_ID
import io.airbyte.workload.launcher.constants.EnvVarConstants
import io.airbyte.workload.launcher.model.toEnvVarList
import io.airbyte.workload.launcher.pods.factories.RuntimeEnvVarFactory.Companion.MYSQL_SOURCE_NAME
import io.airbyte.workload.launcher.pods.factories.RuntimeEnvVarFactoryTest.Fixtures.WORKLOAD_ID
import io.airbyte.workload.launcher.pods.factories.RuntimeEnvVarFactoryTest.Fixtures.connectorAwsAssumedRoleSecretEnvList
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
import org.junit.jupiter.params.provider.MethodSource
import org.mockito.ArgumentMatchers.anyList
import java.util.UUID
import java.util.stream.Stream
import io.airbyte.commons.envvar.EnvVar as AirbyteEnvVar

class RuntimeEnvVarFactoryTest {
  private lateinit var connectorApmSupportHelper: ConnectorApmSupportHelper

  private lateinit var ffClient: TestClient

  private lateinit var factory: RuntimeEnvVarFactory

  @BeforeEach
  fun setup() {
    connectorApmSupportHelper = mockk()
    ffClient = mockk()
    every { ffClient.boolVariation(InjectAwsSecretsToConnectorPods, any()) } returns false

    factory = spyk(RuntimeEnvVarFactory(connectorAwsAssumedRoleSecretEnvList, connectorApmSupportHelper, ffClient))
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
        EnvVar(WorkerEnvConstants.WORKER_CONNECTOR_IMAGE, image, null),
        EnvVar(WorkerEnvConstants.WORKER_JOB_ID, jobId, null),
        EnvVar(WorkerEnvConstants.WORKER_JOB_ATTEMPT, attemptId.toString(), null),
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

    val result = factory.getConfigurationEnvVars(sourceImageName, UUID.randomUUID())

    val expected =
      listOf(
        EnvVar(EnvVarConstants.USE_STREAM_CAPABLE_STATE_ENV_VAR, "true", null),
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

    val result = factory.getConfigurationEnvVars(sourceImageName, UUID.randomUUID())

    val expected =
      listOf(
        EnvVar(EnvVarConstants.USE_STREAM_CAPABLE_STATE_ENV_VAR, "true", null),
        EnvVar(EnvVarConstants.CONCURRENT_SOURCE_STREAM_READ_ENV_VAR, "false", null),
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
    val passThroughVars = passThroughEnvMap?.toEnvVarList().orEmpty()
    every { factory.resolveAwsAssumedRoleEnvVars(any()) } returns awsEnvVars
    every { factory.getConnectorApmEnvVars(any(), any()) } returns apmEnvVars
    every { factory.getConfigurationEnvVars(any(), any()) } returns configurationEnvVars
    every { factory.getMetadataEnvVars(any()) } returns metadataEnvVars

    val config =
      IntegrationLauncherConfig()
        .withAdditionalEnvironmentVariables(passThroughEnvMap)
        .withDockerImage("image-name")
        .withWorkspaceId(workspaceId)

    val result = factory.replicationConnectorEnvVars(config)

    val expected = awsEnvVars + apmEnvVars + configurationEnvVars + metadataEnvVars + passThroughVars

    assertEquals(expected, result)
  }

  @Test
  fun `builds expected env vars for check connector container`() {
    every { factory.resolveAwsAssumedRoleEnvVars(any()) } returns connectorAwsAssumedRoleSecretEnvList
    val config =
      IntegrationLauncherConfig()
        .withWorkspaceId(workspaceId)
    val result = factory.checkConnectorEnvVars(config, WORKLOAD_ID)

    assertEquals(
      connectorAwsAssumedRoleSecretEnvList +
        EnvVar(AirbyteEnvVar.OPERATION_TYPE.toString(), WorkloadType.CHECK.toString(), null) +
        EnvVar(AirbyteEnvVar.WORKLOAD_ID.toString(), WORKLOAD_ID, null),
      result,
    )
  }

  @Test
  fun `builds expected env vars for discover connector container`() {
    every { factory.resolveAwsAssumedRoleEnvVars(any()) } returns connectorAwsAssumedRoleSecretEnvList
    val config =
      IntegrationLauncherConfig()
        .withWorkspaceId(workspaceId)
    val result = factory.discoverConnectorEnvVars(config, WORKLOAD_ID)

    assertEquals(
      connectorAwsAssumedRoleSecretEnvList +
        EnvVar(AirbyteEnvVar.OPERATION_TYPE.toString(), WorkloadType.DISCOVER.toString(), null) +
        EnvVar(AirbyteEnvVar.WORKLOAD_ID.toString(), WORKLOAD_ID, null),
      result,
    )
  }

  @Test
  fun `builds expected env vars for spec connector container`() {
    val result = factory.specConnectorEnvVars(WORKLOAD_ID)

    assertEquals(
      listOf(
        EnvVar(AirbyteEnvVar.OPERATION_TYPE.toString(), WorkloadType.SPEC.toString(), null),
        EnvVar(AirbyteEnvVar.WORKLOAD_ID.toString(), WORKLOAD_ID, null),
      ),
      result,
    )
  }

  object Fixtures {
    const val WORKLOAD_ID = "test-workload-id"
    val workspaceId = UUID.randomUUID()!!
    val connectorAwsAssumedRoleSecretEnvList = listOf(EnvVar("test", "creds", null))
  }

  companion object {
    @JvmStatic
    private fun concurrentStreamReadEnabledMatrix(): Stream<Arguments> {
      return Stream.of(
        Arguments.of(true, MYSQL_SOURCE_NAME),
        Arguments.of(true, MYSQL_SOURCE_NAME + "asdf"),
      )
    }

    @JvmStatic
    private fun concurrentStreamReadDisabledMatrix(): Stream<Arguments> {
      return Stream.of(
        Arguments.of(true, "anything else"),
        Arguments.of(true, "asdf" + MYSQL_SOURCE_NAME),
        Arguments.of(false, MYSQL_SOURCE_NAME),
        Arguments.of(false, MYSQL_SOURCE_NAME + "asdf"),
        Arguments.of(false, "anything else"),
      )
    }

    @JvmStatic
    private fun additionalEnvironmentVariablesMatrix(): Stream<Arguments> {
      return Stream.of(
        Arguments.of(null),
        Arguments.of(mapOf("key-1" to "value-1")),
        Arguments.of(mapOf("key-1" to "value-1", "key-2" to "value-2")),
      )
    }
  }
}
