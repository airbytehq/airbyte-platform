/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.process;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import io.airbyte.commons.features.EnvVariableFeatureFlags;
import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.workers.config.WorkerConfigs;
import io.airbyte.config.EnvConfigs;
import io.airbyte.config.storage.CloudStorageConfigs;
import io.airbyte.config.storage.MinioS3ClientFactory;
import io.airbyte.metrics.lib.MetricClient;
import io.airbyte.workers.storage.DocumentStoreClient;
import io.airbyte.workers.storage.S3DocumentStoreClient;
import io.airbyte.workers.sync.OrchestratorConstants;
import io.airbyte.workers.workload.JobOutputDocStore;
import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.ContainerPort;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;

@Disabled
class AsyncOrchestratorPodProcessIntegrationTest {

  private static KubernetesClient kubernetesClient;
  private static DocumentStoreClient documentStoreClient;
  private static Process portForwardProcess;

  @BeforeAll
  static void init() throws Exception {
    kubernetesClient = new DefaultKubernetesClient();

    final var podName = "test-minio-" + RandomStringUtils.randomAlphabetic(10).toLowerCase();

    final var minioContainer = new ContainerBuilder()
        .withName("minio")
        .withImage("minio/minio:RELEASE.2023-11-15T20-43-25Z")
        .withArgs("server", "/home/shared")
        .withEnv(
            new EnvVar("MINIO_ACCESS_KEY", "minio", null),
            new EnvVar("MINIO_SECRET_KEY", "minio123", null))
        .withPorts(new ContainerPort(9000, null, null, null, null))
        .build();

    final Pod minioPod = new PodBuilder()
        .withApiVersion("v1")
        .withNewMetadata()
        .withName(podName)
        .withNamespace("default")
        .endMetadata()
        .withNewSpec()
        .withRestartPolicy("Never")
        .withContainers(minioContainer)
        .endSpec()
        .build();

    kubernetesClient.pods().inNamespace("default").create(minioPod);
    kubernetesClient.resource(minioPod).waitUntilReady(1, TimeUnit.MINUTES);

    portForwardProcess = new ProcessBuilder("kubectl", "port-forward", "pod/" + podName, "9432:9000").start();

    final var localMinioEndpoint = "http://localhost:9432";

    final var minioConfig = new CloudStorageConfigs.MinioConfig(
        "anything",
        "minio",
        "minio123",
        localMinioEndpoint);

    final var s3Client = new MinioS3ClientFactory(minioConfig).get();

    final var createBucketRequest = CreateBucketRequest.builder()
        .bucket("anything")
        .build();

    s3Client.createBucket(createBucketRequest);

    documentStoreClient = S3DocumentStoreClient.minio(
        minioConfig,
        Path.of("/"));
  }

  @ValueSource(strings = {"IfNotPresent", " Always"})
  @ParameterizedTest
  void testAsyncOrchestratorPodProcess(final String pullPolicy) throws InterruptedException {
    final var serverPort = 8080;
    final var podName = "test-async-" + RandomStringUtils.randomAlphabetic(10).toLowerCase();
    final var mainContainerInfo = new KubeContainerInfo("airbyte/container-orchestrator:dev", pullPolicy);
    // make kubepodinfo
    final var kubePodInfo = new KubePodInfo("default", podName, mainContainerInfo);

    // another activity issues the request to create the pod process -> here we'll just create it
    final var asyncProcess = new AsyncOrchestratorPodProcess(
        kubePodInfo,
        documentStoreClient,
        kubernetesClient,
        null,
        null,
        null,
        null,
        null,
        Map.of(EnvVariableFeatureFlags.AUTO_DETECT_SCHEMA, "true"),
        Map.of("k8s.io/example", "true"),
        serverPort,
        "airbyte-admin",
        null,
        mock(MetricClient.class),
        "test",
        new JobOutputDocStore(documentStoreClient),
        "workload_id");

    final Map<Integer, Integer> portMap = Map.of(
        serverPort, serverPort,
        OrchestratorConstants.PORT1, OrchestratorConstants.PORT1,
        OrchestratorConstants.PORT2, OrchestratorConstants.PORT2,
        OrchestratorConstants.PORT3, OrchestratorConstants.PORT3,
        OrchestratorConstants.PORT4, OrchestratorConstants.PORT4);

    final Map<String, String> envMap = System.getenv().entrySet().stream()
        .filter(entry -> OrchestratorConstants.ENV_VARS_TO_TRANSFER.contains(entry.getKey()))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

    final WorkerConfigs workerConfigs = new WorkerConfigs(new EnvConfigs());

    asyncProcess.create(Map.of(), new WorkerConfigs(new EnvConfigs()).getResourceRequirements(), Map.of(
        OrchestratorConstants.INIT_FILE_APPLICATION, AsyncOrchestratorPodProcess.NO_OP,
        OrchestratorConstants.INIT_FILE_ENV_MAP, Jsons.serialize(envMap)), portMap, workerConfigs.getworkerKubeNodeSelectors(),
        workerConfigs.getWorkerKubeTolerations());

    // a final activity waits until there is output from the kube pod process
    asyncProcess.waitFor(10, TimeUnit.SECONDS);

    final var exitValue = asyncProcess.exitValue();
    final var output = asyncProcess.getOutput();

    assertEquals(0, exitValue);
    assertTrue(output.isPresent());
    assertEquals("expected output", output.get());

  }

  @AfterAll
  public static void teardown() throws KubernetesClientException {
    portForwardProcess.destroyForcibly();
    kubernetesClient.pods().delete();
  }

}
