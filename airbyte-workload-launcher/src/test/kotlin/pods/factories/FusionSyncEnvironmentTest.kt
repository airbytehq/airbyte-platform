/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package pods.factories

import io.airbyte.commons.json.Jsons
import io.airbyte.commons.workers.config.WorkerConfigs
import io.airbyte.config.Configs.AirbyteEdition
import io.airbyte.config.ConnectionContext
import io.airbyte.config.ResourceRequirements
import io.airbyte.featureflag.InjectAwsSecretsToConnectorPods
import io.airbyte.featureflag.TestClient
import io.airbyte.micronaut.runtime.AirbyteConnectorConfig
import io.airbyte.micronaut.runtime.AirbyteContainerOrchestratorConfig
import io.airbyte.micronaut.runtime.AirbyteLoggingConfig
import io.airbyte.micronaut.runtime.AirbyteWorkerConfig
import io.airbyte.persistence.job.models.IntegrationLauncherConfig
import io.airbyte.persistence.job.models.JobRunConfig
import io.airbyte.persistence.job.models.ReplicationInput
import io.airbyte.workload.launcher.config.EnvVarConfigBeanFactory
import io.airbyte.workload.launcher.constants.EnvVarConstants
import io.airbyte.workload.launcher.pipeline.stages.model.ArchitectureEnvironmentVariables
import io.airbyte.workload.launcher.pipeline.stages.model.SyncPayload
import io.airbyte.workload.launcher.pods.KubeContainerInfo
import io.airbyte.workload.launcher.pods.KubeNodeSelector
import io.airbyte.workload.launcher.pods.PayloadKubeInputMapper
import io.airbyte.workload.launcher.pods.PodLabeler
import io.airbyte.workload.launcher.pods.PodNameGenerator
import io.airbyte.workload.launcher.pods.factories.ReplicationContainerFactory
import io.airbyte.workload.launcher.pods.factories.RuntimeEnvVarFactory
import io.fabric8.kubernetes.api.model.EnvVar
import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.Assertions.assertDoesNotThrow
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.util.UUID

class FusionSyncEnvironmentTest {
  @Test
  fun `final pods contain Fusion destination identity and copy across architectures`() {
    for (scenario in 0 until 128) {
      val endpoint = scenario and 128 != 0
      val edition = if (scenario and 64 == 0) AirbyteEdition.COMMUNITY else AirbyteEdition.CLOUD
      val copy = scenario and 32 != 0
      val legacyAws = scenario and 16 != 0
      val socket = scenario and 4 != 0
      val reset = scenario and 2 != 0
      val customSource = scenario and 1 != 0
      val flags = TestClient(mapOf(InjectAwsSecretsToConnectorPods.key to legacyAws))
      val context =
        ConnectionContext()
          .withOrganizationId(UUID.randomUUID())
          .withWorkspaceId(UUID.randomUUID())
          .withSourceId(UUID.randomUUID())
          .withDestinationId(UUID.randomUUID())
          .withConnectionId(UUID.randomUUID())
      val ids =
        mapOf(
          "AIRBYTE_ORGANIZATION_ID" to context.organizationId,
          "AIRBYTE_WORKSPACE_ID" to context.workspaceId,
          "AIRBYTE_SOURCE_ID" to context.sourceId,
          "AIRBYTE_DESTINATION_ID" to context.destinationId,
          "AIRBYTE_CONNECTION_ID" to context.connectionId,
        )
      val collisions =
        ids.keys.associateWith { "stale" } +
          mapOf(
            "AIRBYTE_FUSION_ENABLED" to "true",
            "AIRBYTE_FUSION_S3_ORGANIZATION_ID" to "stale",
            "AIRBYTE_S3_COPY_ENDPOINT" to "http://stale:4566",
            "AWS_ENDPOINT_URL" to "http://stale:4566",
          )
      val config = AirbyteConnectorConfig()
      val aws =
        config.source.credentials.aws.assumedRole
          .copy(secretName = "bootstrap")
      val refs =
        EnvVarConfigBeanFactory().connectorAwsAssumedRoleSecretEnv(
          config.copy(
            source =
              config.source.copy(
                credentials =
                  config.source.credentials.copy(
                    aws =
                      config.source.credentials.aws
                        .copy(assumedRole = aws),
                  ),
              ),
          ),
        )
      val runtime =
        RuntimeEnvVarFactory(refs, AirbyteContainerOrchestratorConfig(), AirbyteWorkerConfig(), AirbyteLoggingConfig(), mockk(), flags, edition)
      val launcherConfig =
        IntegrationLauncherConfig()
          .withDockerImage("airbyte/test:1")
          .withWorkspaceId(context.workspaceId)
          .withConnectionId(
            context.connectionId,
          ).withJobId("42")
          .withAttemptId(0)
          .withIsCustomConnector(false)
          .withAdditionalEnvironmentVariables(collisions)
      val input =
        ReplicationInput()
          .withConnectionContext(context)
          .withWorkspaceId(context.workspaceId)
          .withConnectionId(context.connectionId)
          .withSourceId(context.sourceId)
          .withDestinationId(context.destinationId)
          .withJobRunConfig(JobRunConfig().withJobId("42").withAttemptId(0))
          .withSourceLauncherConfig(
            Jsons.clone(launcherConfig).withIsCustomConnector(customSource),
          ).withDestinationLauncherConfig(launcherConfig)
          .withUseFileTransfer(!socket)
          .withIsReset(reset)
      val fusionEnabled = copy && edition == AirbyteEdition.CLOUD
      val copyEnv =
        if (fusionEnabled) {
          mapOf(
            "AIRBYTE_FUSION_ENABLED" to "true",
            "AIRBYTE_FUSION_S3_BUCKET" to "bucket",
            "AIRBYTE_FUSION_S3_REGION" to "us-west-2",
            "AIRBYTE_FUSION_S3_PREFIX" to "fusion",
            "AIRBYTE_FUSION_S3_ROLE_ARN" to "arn:aws:iam::123456789012:role/airbyte-fusion-writer-${context.organizationId}",
          ) + if (endpoint) mapOf("AIRBYTE_S3_COPY_ENDPOINT" to "http://localstack:4566") else emptyMap()
        } else {
          emptyMap()
        }
      val req = ResourceRequirements()
      val workers = WorkerConfigs(req, emptyList(), emptyMap(), null, emptyMap(), emptyMap(), emptyList(), "Always")
      val mapper =
        PayloadKubeInputMapper(
          PodLabeler(mockk()),
          PodNameGenerator(),
          AirbyteWorkerConfig(),
          KubeContainerInfo("orch:1", "Always"),
          workers,
          workers,
          workers,
          workers,
          mockk {
            every { orchestrator(any()) } returns req
            every { replSource(any()) } returns req
            every { replDestination(any()) } returns req
            every { replInit(any()) } returns req
          },
          runtime,
          flags,
          KubeNodeSelector(flags),
        )
      val mapped = mapper.toKubeInput("workload", SyncPayload(input, fusionDestinationEnvironment = copyEnv), emptyMap())
      assertEquals("replication-job-42-attempt-0", mapped.podName)
      val collisionEnv = collisions.map { EnvVar(it.key, it.value, null) }
      val architecture =
        ArchitectureEnvironmentVariables(
          collisionEnv + EnvVar("DATA_CHANNEL_MEDIUM", if (socket) "SOCKET" else "STDIO", null),
          listOf(EnvVar("PLATFORM_MODE", if (socket) "BOOKKEEPER" else "ORCHESTRATOR", null)),
          collisionEnv,
        )
      val fixtures = ReplicationPodFactoryTest.Fixtures
      val podFactory =
        fixtures.defaultReplicationPodFactory.copy(
          replContainerFactory =
            ReplicationContainerFactory(
              fixtures.workloadSecurityContextProvider,
              emptyList(),
              collisionEnv,
              collisionEnv,
              fixtures.airbyteWorkerConfig,
            ),
        )
      val pod =
        if (reset) {
          podFactory.createReset(
            mapped.podName,
            emptyMap(),
            emptyMap(),
            emptyMap(),
            "orch:1",
            "dest:1",
            null,
            null,
            emptyList(),
            mapped.destinationRuntimeEnvVars,
            !socket,
            context.workspaceId,
            architecture,
          )
        } else {
          podFactory.create(
            mapped.podName,
            emptyMap(),
            emptyMap(),
            emptyMap(),
            "orch:1",
            "source:1",
            "dest:1",
            null,
            null,
            null,
            emptyList(),
            mapped.sourceRuntimeEnvVars,
            mapped.destinationRuntimeEnvVars,
            !socket,
            context.workspaceId,
            architectureEnvironmentVariables = architecture,
          )
        }
      for (container in pod.spec.containers.filter { it.name in listOf("source", "destination") }) {
        for ((name, value) in ids) {
          val receivesIdentity = fusionEnabled && container.name == "destination"
          assertEquals(if (receivesIdentity) listOf(value.toString()) else emptyList(), container.env.filter { it.name == name }.map { it.value })
        }
        val conditional = container.env.filter { (it.name.startsWith("AIRBYTE_FUSION_") || it.name == "AIRBYTE_S3_COPY_ENDPOINT") }
        assertEquals(if (fusionEnabled && container.name == "destination") copyEnv else emptyMap(), conditional.associate { it.name to it.value })
        val endpointUrl = container.env.filter { it.name == EnvVarConstants.AWS_ENDPOINT_URL }.map { it.value }
        // Fusion replaces the connector's AWS environment on the destination; otherwise passthrough is untouched.
        val expectedEndpointUrl =
          when {
            fusionEnabled && container.name == "destination" -> if (endpoint) listOf("http://localstack:4566") else emptyList()
            else -> listOf("http://stale:4566")
          }
        assertEquals(expectedEndpointUrl, endpointUrl)
        val credentials = container.env.filter { it.name in setOf("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY") }
        val expectCredentials =
          (legacyAws && (!customSource || container.name == "destination")) || (fusionEnabled && container.name == "destination")
        assertEquals(
          if (expectCredentials) 2 else 0,
          credentials.size,
        )
        credentials.forEach {
          assertNull(it.value)
          assertEquals("bootstrap", it.valueFrom.secretKeyRef.name)
        }
      }
      assertEquals(if (reset) 1 else 2, pod.spec.containers.count { it.name in listOf("source", "destination") })
      for (container in pod.spec.containers.filter { it.name !in listOf("source", "destination") }) {
        assertTrue(
          container.env.none {
            (it.name.startsWith("AIRBYTE_FUSION_") || it.name == "AIRBYTE_S3_COPY_ENDPOINT")
          },
          "${container.name} must not receive copy settings",
        )
      }
      if (scenario == 0) {
        val enabledCopyEnvironment =
          mapOf(
            "AIRBYTE_FUSION_ENABLED" to "true",
            "AIRBYTE_FUSION_S3_BUCKET" to "bucket",
            "AIRBYTE_FUSION_S3_REGION" to "us-west-2",
            "AIRBYTE_FUSION_S3_PREFIX" to "fusion",
            "AIRBYTE_FUSION_S3_ROLE_ARN" to "arn:aws:iam::123456789012:role/airbyte-fusion-writer-${context.organizationId}",
          )
        input.sourceId = null
        assertDoesNotThrow { mapper.toKubeInput("workload", SyncPayload(input), emptyMap()) }
        assertThrows<IllegalArgumentException> {
          mapper.toKubeInput("workload", SyncPayload(input, fusionDestinationEnvironment = enabledCopyEnvironment), emptyMap())
        }
        input.connectionContext = null
        assertThrows<IllegalArgumentException> {
          mapper.toKubeInput("workload", SyncPayload(input, fusionDestinationEnvironment = enabledCopyEnvironment), emptyMap())
        }
        input.connectionContext = context
        input.sourceId = context.sourceId
        context.destinationId = UUID.randomUUID()
        assertDoesNotThrow { mapper.toKubeInput("workload", SyncPayload(input), emptyMap()) }
        assertThrows<IllegalArgumentException> {
          mapper.toKubeInput("workload", SyncPayload(input, fusionDestinationEnvironment = enabledCopyEnvironment), emptyMap())
        }
        context.destinationId = input.destinationId
        context.organizationId = null
        assertThrows<IllegalArgumentException> {
          mapper.toKubeInput("workload", SyncPayload(input, fusionDestinationEnvironment = enabledCopyEnvironment), emptyMap())
        }
      }
    }
  }

  @Test
  fun `enabled copy rejects absent or incomplete bootstrap references`() {
    val runtime =
      RuntimeEnvVarFactory(
        emptyList(),
        AirbyteContainerOrchestratorConfig(),
        AirbyteWorkerConfig(),
        AirbyteLoggingConfig(),
        mockk(),
        TestClient(),
        AirbyteEdition.CLOUD,
      )
    assertTrue(runtime.fusionDestinationEnvVars(emptyMap()).isEmpty())
    val launcherConfig = IntegrationLauncherConfig().withWorkspaceId(UUID.randomUUID()).withIsCustomConnector(false)
    assertThrows<IllegalArgumentException> { runtime.resolveAwsAssumedRoleEnvVars(launcherConfig, fusionDestination = true) }
    val invalid =
      RuntimeEnvVarFactory(
        listOf(EnvVar("AWS_ACCESS_KEY_ID", "literal", null), EnvVar("AWS_SECRET_ACCESS_KEY", "literal", null)),
        AirbyteContainerOrchestratorConfig(),
        AirbyteWorkerConfig(),
        AirbyteLoggingConfig(),
        mockk(),
        TestClient(),
        AirbyteEdition.CLOUD,
      )
    assertThrows<IllegalArgumentException> { invalid.resolveAwsAssumedRoleEnvVars(launcherConfig, fusionDestination = true) }
  }
}
