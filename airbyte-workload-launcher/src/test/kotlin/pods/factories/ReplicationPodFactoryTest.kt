package pods.factories

import io.airbyte.commons.storage.STORAGE_CLAIM_NAME
import io.airbyte.commons.storage.STORAGE_MOUNT
import io.airbyte.commons.storage.STORAGE_VOLUME_NAME
import io.airbyte.featureflag.PlaneName
import io.airbyte.featureflag.TestClient
import io.airbyte.workers.context.WorkloadSecurityContextProvider
import io.airbyte.workers.pod.KubeContainerInfo
import io.airbyte.workers.pod.ResourceConversionUtils
import io.airbyte.workload.launcher.pods.factories.InitContainerFactory
import io.airbyte.workload.launcher.pods.factories.NodeSelectionFactory
import io.airbyte.workload.launcher.pods.factories.ReplicationContainerFactory
import io.airbyte.workload.launcher.pods.factories.ReplicationPodFactory
import io.airbyte.workload.launcher.pods.factories.ResourceRequirementsFactory
import io.airbyte.workload.launcher.pods.factories.VolumeFactory
import io.airbyte.workload.launcher.pods.model.NodeSelection
import io.fabric8.kubernetes.api.model.Affinity
import io.fabric8.kubernetes.api.model.Container
import io.fabric8.kubernetes.api.model.EnvVar
import io.fabric8.kubernetes.api.model.ResourceRequirements
import io.fabric8.kubernetes.api.model.Toleration
import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotEquals
import org.junit.jupiter.api.Test
import pods.factories.ReplicationPodFactoryTest.Fixtures.resourceRequirements
import java.util.UUID

class ReplicationPodFactoryTest {
  @Test
  fun basics() {
    val pod = Fixtures.createPodWithDefaults(Fixtures.defaultReplicationPodFactory)
    assertEquals(3, pod.spec.containers.size)
    assertEquals(1, pod.spec.initContainers.size)

    val initSpec = pod.spec.initContainers[0]
    val orchSpec = pod.spec.containers[0]
    val sourceSpec = pod.spec.containers[1]
    val destSpec = pod.spec.containers[2]
    assertEquals("test-init-image", initSpec.image)
    assertEquals("test-orch-image", orchSpec.image)
    assertEquals("test-source-image", sourceSpec.image)
    assertEquals("test-dest-image", destSpec.image)
  }

  @Test
  fun `create pod with local storage and connector file access enabled`() {
    val fac =
      Fixtures.defaultReplicationPodFactory.copy(
        volumeFactory =
          Fixtures.defaultVolumeFactory.copy(
            cloudStorageType = "LOCAL",
            localVolumeEnabled = true,
          ),
      )

    val pod = Fixtures.createPodWithDefaults(fac)
    // the pod gets two new volumes: storage and local
    assertEquals(STORAGE_VOLUME_NAME, pod.spec.volumes[4].name)
    assertEquals(STORAGE_CLAIM_NAME, pod.spec.volumes[4].persistentVolumeClaim.claimName)
    assertEquals(VolumeFactory.LOCAL_VOLUME_NAME, pod.spec.volumes[5].name)
    assertEquals(VolumeFactory.LOCAL_CLAIM_NAME, pod.spec.volumes[5].persistentVolumeClaim.claimName)

    val initSpec = pod.spec.initContainers[0]
    val orchSpec = pod.spec.containers[0]
    val sourceSpec = pod.spec.containers[1]
    val destSpec = pod.spec.containers[2]

    fun Container.mountIdx(name: String) = this.volumeMounts.indexOfFirst { it.name == name }

    val initStorageIdx = initSpec.mountIdx(STORAGE_VOLUME_NAME)
    val initLocalIdx = initSpec.mountIdx(VolumeFactory.LOCAL_VOLUME_NAME)
    val orchStorageIdx = orchSpec.mountIdx(STORAGE_VOLUME_NAME)
    val orchLocalIdx = orchSpec.mountIdx(VolumeFactory.LOCAL_VOLUME_NAME)
    val sourceStorageIdx = sourceSpec.mountIdx(STORAGE_VOLUME_NAME)
    val sourceLocalIdx = sourceSpec.mountIdx(VolumeFactory.LOCAL_VOLUME_NAME)
    val destStorageIdx = destSpec.mountIdx(STORAGE_VOLUME_NAME)
    val destLocalIdx = destSpec.mountIdx(VolumeFactory.LOCAL_VOLUME_NAME)

    // the init container gets the storage mount, but not the local files.
    assertEquals(-1, initLocalIdx)
    assertNotEquals(-1, initStorageIdx)

    // the orchestrator container gets the storage mount, but not the local files mount.
    assertEquals(-1, orchLocalIdx)
    assertNotEquals(-1, orchStorageIdx)

    // the source and destination containers get the local files mount, but not the storage.
    assertEquals(-1, sourceStorageIdx)
    assertEquals(-1, destStorageIdx)
    assertNotEquals(-1, sourceLocalIdx)
    assertNotEquals(-1, destLocalIdx)

    // check the mount paths
    assertEquals(STORAGE_MOUNT, initSpec.volumeMounts[initStorageIdx].mountPath)
    assertEquals(STORAGE_MOUNT, orchSpec.volumeMounts[initStorageIdx].mountPath)
    assertEquals(VolumeFactory.LOCAL_VOLUME_MOUNT, sourceSpec.volumeMounts[sourceLocalIdx].mountPath)
    assertEquals(VolumeFactory.LOCAL_VOLUME_MOUNT, destSpec.volumeMounts[destLocalIdx].mountPath)
  }

  @Test
  fun `create replication pod with node selection`() {
    val expectedNodeSelection =
      NodeSelection(
        nodeSelectors = mapOf("node" to "replication"),
        tolerations = listOf(Toleration().apply { key = "repl" }),
        podAffinity = Affinity().apply { additionalProperties["sanity check"] = "replicate" },
      )
    val nodeSelectionFactory: NodeSelectionFactory =
      mockk {
        every { createReplicationNodeSelection(any(), any()) } returns expectedNodeSelection
      }
    val fac =
      Fixtures.defaultReplicationPodFactory.copy(
        nodeSelectionFactory = nodeSelectionFactory,
      )

    val pod = Fixtures.createPodWithDefaults(fac)

    assertEquals(expectedNodeSelection.nodeSelectors, pod.spec.nodeSelector)
    assertEquals(expectedNodeSelection.tolerations, pod.spec.tolerations)
    assertEquals(expectedNodeSelection.podAffinity, pod.spec.affinity)
  }

  @Test
  fun `create reset pod with node selection`() {
    val expectedNodeSelection =
      NodeSelection(
        nodeSelectors = mapOf("node" to "reset"),
        tolerations = listOf(Toleration().apply { key = "reset" }),
        podAffinity = Affinity().apply { additionalProperties["sanity check"] = "reset" },
      )
    val nodeSelectionFactory: NodeSelectionFactory =
      mockk {
        every { createResetNodeSelection(any(), any()) } returns expectedNodeSelection
      }
    val fac =
      Fixtures.defaultReplicationPodFactory.copy(
        nodeSelectionFactory = nodeSelectionFactory,
      )

    val pod = Fixtures.createResetWithDefaults(fac)

    assertEquals(expectedNodeSelection.nodeSelectors, pod.spec.nodeSelector)
    assertEquals(expectedNodeSelection.tolerations, pod.spec.tolerations)
    assertEquals(expectedNodeSelection.podAffinity, pod.spec.affinity)
  }

  object Fixtures {
    val workloadSecurityContextProvider = WorkloadSecurityContextProvider(rootlessWorkload = true)
    val featureFlagClient = TestClient()
    val resourceRequirements =
      io.airbyte.config.ResourceRequirements()
        .withCpuLimit("2")
        .withCpuRequest("1")
        .withMemoryLimit("200")
        .withMemoryRequest("100")

    val resourceRequirementsFactory =
      ResourceRequirementsFactory(
        checkConnectorReqs = resourceRequirements,
        discoverConnectorReqs = resourceRequirements,
        specConnectorReqs = resourceRequirements,
        sidecarReqs = resourceRequirements,
        fileTransferReqs = resourceRequirements,
      )

    val defaultVolumeFactory =
      VolumeFactory(
        googleApplicationCredentials = null,
        secretName = "test-vol-secret-name",
        secretMountPath = "/secret-mount-path",
        dataPlaneCredsSecretName = "test-dp-secret-name",
        dataPlaneCredsMountPath = "/dp-secret-mount-path",
        stagingMountPath = "/staging-mount-path",
        cloudStorageType = "gcs",
        localVolumeEnabled = false,
      )

    val defaultTolerations = listOf(Toleration().apply { key = "configuredByUser" })
    val spotToleration = Toleration().apply { key = "spotToleration" }

    val defaultNodeSelectionFactory =
      NodeSelectionFactory(
        featureFlagClient = featureFlagClient,
        tolerations = defaultTolerations,
        infraFlagContexts = listOf(PlaneName("test")),
        spotToleration = spotToleration,
      )

    val defaultReplicationPodFactory =
      ReplicationPodFactory(
        featureFlagClient = featureFlagClient,
        initContainerFactory =
          InitContainerFactory(
            workloadSecurityContextProvider = workloadSecurityContextProvider,
            envVars = listOf(EnvVar("INIT_ENV_1", "INIT_ENV_VAL_1", null)),
            initContainerInfo = KubeContainerInfo("test-init-image", "Always"),
            featureFlagClient = featureFlagClient,
          ),
        replContainerFactory =
          ReplicationContainerFactory(
            workloadSecurityContextProvider = workloadSecurityContextProvider,
            orchestratorEnvVars = emptyList(),
            sourceEnvVars = emptyList(),
            destinationEnvVars = emptyList(),
            imagePullPolicy = "Always",
          ),
        volumeFactory = defaultVolumeFactory,
        workloadSecurityContextProvider = workloadSecurityContextProvider,
        serviceAccount = "test-sa",
        nodeSelectionFactory = defaultNodeSelectionFactory,
        imagePullSecrets = emptyList(),
      )

    fun createPodWithDefaults(
      factory: ReplicationPodFactory,
      podName: String = "test-pod-name",
      allLabels: Map<String, String> = emptyMap(),
      annotations: Map<String, String> = emptyMap(),
      nodeSelectors: Map<String, String> = emptyMap(),
      orchImage: String = "test-orch-image",
      sourceImage: String = "test-source-image",
      destImage: String = "test-dest-image",
      orchResourceReqs: ResourceRequirements? = ResourceConversionUtils.domainToApi(resourceRequirements),
      sourceResourceReqs: ResourceRequirements? = ResourceConversionUtils.domainToApi(resourceRequirements),
      destResourceReqs: ResourceRequirements? = ResourceConversionUtils.domainToApi(resourceRequirements),
      orchRuntimeEnvVars: List<EnvVar> = emptyList(),
      sourceRuntimeEnvVars: List<EnvVar> = emptyList(),
      destRuntimeEnvVars: List<EnvVar> = emptyList(),
      isFileTransfer: Boolean = false,
      workspaceId: UUID = UUID.randomUUID(),
    ) = factory.create(
      podName, allLabels, annotations, nodeSelectors, orchImage, sourceImage, destImage, orchResourceReqs,
      sourceResourceReqs, destResourceReqs, orchRuntimeEnvVars, sourceRuntimeEnvVars, destRuntimeEnvVars,
      isFileTransfer, workspaceId,
    )

    fun createResetWithDefaults(
      factory: ReplicationPodFactory,
      podName: String = "test-pod-name",
      allLabels: Map<String, String> = emptyMap(),
      annotations: Map<String, String> = emptyMap(),
      nodeSelectors: Map<String, String> = emptyMap(),
      orchImage: String = "test-orch-image",
      destImage: String = "test-dest-image",
      orchResourceReqs: ResourceRequirements? = ResourceConversionUtils.domainToApi(resourceRequirements),
      destResourceReqs: ResourceRequirements? = ResourceConversionUtils.domainToApi(resourceRequirements),
      orchRuntimeEnvVars: List<EnvVar> = emptyList(),
      destRuntimeEnvVars: List<EnvVar> = emptyList(),
      isFileTransfer: Boolean = false,
      workspaceId: UUID = UUID.randomUUID(),
    ) = factory.createReset(
      podName, allLabels, annotations, nodeSelectors, orchImage, destImage, orchResourceReqs,
      destResourceReqs, orchRuntimeEnvVars, destRuntimeEnvVars, isFileTransfer, workspaceId,
    )
  }
}
