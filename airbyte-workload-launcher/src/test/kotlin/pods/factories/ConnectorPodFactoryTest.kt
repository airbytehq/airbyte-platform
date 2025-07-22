/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package pods.factories

import io.airbyte.commons.storage.STORAGE_CLAIM_NAME
import io.airbyte.commons.storage.STORAGE_MOUNT
import io.airbyte.commons.storage.STORAGE_VOLUME_NAME
import io.airbyte.config.ResourceRequirements
import io.airbyte.featureflag.TestClient
import io.airbyte.workload.launcher.context.WorkloadSecurityContextProvider
import io.airbyte.workload.launcher.pods.KubeContainerInfo
import io.airbyte.workload.launcher.pods.KubePodInfo
import io.airbyte.workload.launcher.pods.ResourceConversionUtils
import io.airbyte.workload.launcher.pods.factories.ConnectorPodFactory
import io.airbyte.workload.launcher.pods.factories.InitContainerFactory
import io.airbyte.workload.launcher.pods.factories.NodeSelectionFactory
import io.airbyte.workload.launcher.pods.factories.ResourceRequirementsFactory
import io.airbyte.workload.launcher.pods.factories.VolumeFactory
import io.airbyte.workload.launcher.pods.model.NodeSelection
import io.fabric8.kubernetes.api.model.Affinity
import io.fabric8.kubernetes.api.model.Container
import io.fabric8.kubernetes.api.model.EnvVar
import io.fabric8.kubernetes.api.model.Toleration
import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotEquals
import org.junit.jupiter.api.Test
import java.util.UUID

class ConnectorPodFactoryTest {
  @Test
  fun basics() {
    val pod =
      Fixtures.createPodWithDefaults(
        Fixtures.defaultConnectorPodFactory,
        allLabels = mapOf("test-label-1" to "test-label-val-1"),
        nodeSelectors = mapOf("node-sel-1" to "node-sel-val-1"),
        annotations = mapOf("test-anno-1" to "test-anno-val-1"),
      )

    assertEquals(2, pod.spec.containers.size)
    assertEquals(1, pod.spec.initContainers.size)

    val sidecarSpec = pod.spec.containers[0]
    val mainSpec = pod.spec.containers[1]
    val initSpec = pod.spec.initContainers[0]

    assertEquals("sidecar-image", sidecarSpec.image)
    assertEquals("main-image", mainSpec.image)
    assertEquals("init-image", initSpec.image)

    assertEquals(2, pod.spec.volumes.size)
    assertEquals(VolumeFactory.CONFIG_VOLUME_NAME, pod.spec.volumes[0].name)
    assertEquals(VolumeFactory.DATA_PLANE_CREDS_VOLUME_NAME, pod.spec.volumes[1].name)
    assertEquals("test-label-val-1", pod.metadata.labels["test-label-1"])
    assertEquals("test-anno-val-1", pod.metadata.annotations["test-anno-1"])
    assertEquals("node-sel-val-1", pod.spec.nodeSelector["node-sel-1"])
  }

  @Test
  fun `create pod with local storage and connector host file access`() {
    val pod =
      Fixtures.createPodWithDefaults(
        Fixtures.defaultConnectorPodFactory.copy(
          volumeFactory =
            Fixtures.defaultVolumeFactory.copy(
              cloudStorageType = "LOCAL",
              localVolumeEnabled = true,
            ),
        ),
      )

    // the pod gets two new volumes: storage and local
    assertEquals(STORAGE_VOLUME_NAME, pod.spec.volumes[2].name)
    assertEquals(
      STORAGE_CLAIM_NAME,
      pod.spec.volumes[2]
        .persistentVolumeClaim.claimName,
    )
    assertEquals(VolumeFactory.LOCAL_VOLUME_NAME, pod.spec.volumes[3].name)
    assertEquals(
      VolumeFactory.LOCAL_CLAIM_NAME,
      pod.spec.volumes[3]
        .persistentVolumeClaim.claimName,
    )

    val sidecarSpec = pod.spec.containers[0]
    val mainSpec = pod.spec.containers[1]
    val initSpec = pod.spec.initContainers[0]

    fun Container.mountIdx(name: String) = this.volumeMounts.indexOfFirst { it.name == name }

    val sidecarStorageIdx = sidecarSpec.mountIdx(STORAGE_VOLUME_NAME)
    val sidecarLocalIdx = sidecarSpec.mountIdx(VolumeFactory.LOCAL_VOLUME_NAME)
    val mainStorageIdx = mainSpec.mountIdx(STORAGE_VOLUME_NAME)
    val mainLocalIdx = mainSpec.mountIdx(VolumeFactory.LOCAL_VOLUME_NAME)
    val initStorageIdx = initSpec.mountIdx(STORAGE_VOLUME_NAME)
    val initLocalIdx = initSpec.mountIdx(VolumeFactory.LOCAL_VOLUME_NAME)

    // The init container mounts the storage but not the local files.
    assertNotEquals(-1, initStorageIdx)
    assertEquals(-1, initLocalIdx)

    // The sidecar container mounts the storage but not the local files.
    assertNotEquals(-1, sidecarStorageIdx)
    assertEquals(-1, sidecarLocalIdx)

    // The main (connector) container mounts the local files but not the storage.
    assertNotEquals(-1, mainLocalIdx)
    assertEquals(-1, mainStorageIdx)

    // check the mount path
    assertEquals(STORAGE_MOUNT, initSpec.volumeMounts[initStorageIdx].mountPath)
    assertEquals(STORAGE_MOUNT, sidecarSpec.volumeMounts[sidecarStorageIdx].mountPath)
    assertEquals(VolumeFactory.LOCAL_VOLUME_MOUNT, mainSpec.volumeMounts[mainLocalIdx].mountPath)
  }

  @Test
  fun `create a pod with spot toleration`() {
    val defaultToleration = listOf(Toleration().apply { key = "default" })
    val expectedNodeSelection =
      NodeSelection(
        nodeSelectors = mapOf("node" to "check"),
        tolerations = listOf(Toleration().apply { key = "custom" }),
        podAffinity = Affinity().apply { additionalProperties["sanity check"] = "check" },
      )
    val nodeSelectionFactory: NodeSelectionFactory =
      mockk {
        every { createNodeSelection(any(), any()) } returns expectedNodeSelection
      }
    val pod =
      Fixtures.createPodWithDefaults(
        Fixtures.defaultConnectorPodFactory.copy(
          tolerations = defaultToleration,
          volumeFactory =
            Fixtures.defaultVolumeFactory.copy(
              cloudStorageType = "LOCAL",
              localVolumeEnabled = true,
            ),
          nodeSelectionFactory = nodeSelectionFactory,
        ),
      )

    assertEquals(expectedNodeSelection.nodeSelectors, pod.spec.nodeSelector)
    assertEquals(defaultToleration + expectedNodeSelection.tolerations, pod.spec.tolerations)
    assertEquals(expectedNodeSelection.podAffinity, pod.spec.affinity)
  }

  object Fixtures {
    val workloadSecurityContextProvider = WorkloadSecurityContextProvider(rootlessWorkload = true)
    val featureFlagClient = TestClient()

    val resourceRequirements =
      ResourceRequirements()
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

    val podInfo =
      KubePodInfo(
        "test-ns",
        "test-pod-name",
        KubeContainerInfo("main-image", "Always"),
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

    val spotToleration = Toleration().apply { key = "spotToleration" }

    val nodeSelectionFactory =
      NodeSelectionFactory(
        featureFlagClient = featureFlagClient,
        tolerations = listOf(Toleration().apply { key = "for replication, not used" }),
        spotToleration = spotToleration,
      )

    val defaultConnectorPodFactory =
      ConnectorPodFactory(
        operationCommand = "test-op",
        featureFlagClient = featureFlagClient,
        tolerations = emptyList(),
        imagePullSecrets = emptyList(),
        connectorEnvVars = emptyList(),
        sideCarEnvVars = emptyList(),
        sidecarContainerInfo = KubeContainerInfo("sidecar-image", "Always"),
        serviceAccount = "test-sa",
        volumeFactory = defaultVolumeFactory,
        initContainerFactory =
          InitContainerFactory(
            workloadSecurityContextProvider = workloadSecurityContextProvider,
            envVars = listOf(EnvVar("INIT_ENV_1", "INIT_ENV_VAL_1", null)),
            initContainerInfo = KubeContainerInfo("init-image", "Always"),
            featureFlagClient = featureFlagClient,
          ),
        connectorArgs = emptyMap(),
        workloadSecurityContextProvider = workloadSecurityContextProvider,
        resourceRequirementsFactory = resourceRequirementsFactory,
        nodeSelectionFactory = nodeSelectionFactory,
      )

    fun createPodWithDefaults(
      factory: ConnectorPodFactory,
      allLabels: Map<String, String> = emptyMap(),
      nodeSelectors: Map<String, String> = emptyMap(),
      kubePodInfo: KubePodInfo = podInfo,
      annotations: Map<String, String> = emptyMap(),
      connectorContainerReqs: io.fabric8.kubernetes.api.model.ResourceRequirements = ResourceConversionUtils.domainToApi(resourceRequirements),
      initContainerReqs: io.fabric8.kubernetes.api.model.ResourceRequirements = ResourceConversionUtils.domainToApi(resourceRequirements),
      runtimeEnvVars: List<EnvVar> = emptyList(),
      workspaceId: UUID = UUID.randomUUID(),
    ) = factory.create(allLabels, nodeSelectors, kubePodInfo, annotations, connectorContainerReqs, initContainerReqs, runtimeEnvVars, workspaceId)
  }
}
