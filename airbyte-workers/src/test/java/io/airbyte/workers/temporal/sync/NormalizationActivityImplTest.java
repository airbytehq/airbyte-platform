/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.sync;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.airbyte.api.client.AirbyteApiClient;
import io.airbyte.api.client.generated.ConnectionApi;
import io.airbyte.api.client.model.generated.AirbyteCatalog;
import io.airbyte.api.client.model.generated.ConnectionIdRequestBody;
import io.airbyte.api.client.model.generated.ConnectionRead;
import io.airbyte.commons.temporal.TemporalUtils;
import io.airbyte.commons.workers.config.WorkerConfigs;
import io.airbyte.commons.workers.config.WorkerConfigsProvider;
import io.airbyte.config.AirbyteConfigValidator;
import io.airbyte.config.Configs;
import io.airbyte.config.NormalizationInput;
import io.airbyte.config.NormalizationSummary;
import io.airbyte.config.helpers.LogConfigs;
import io.airbyte.config.persistence.split_secrets.SecretsHydrator;
import io.airbyte.config.storage.CloudStorageConfigs;
import io.airbyte.featureflag.FeatureFlagClient;
import io.airbyte.featureflag.RemoveLargeSyncInputs;
import io.airbyte.featureflag.TestClient;
import io.airbyte.featureflag.UseCustomK8sScheduler;
import io.airbyte.metrics.lib.MetricClient;
import io.airbyte.persistence.job.models.IntegrationLauncherConfig;
import io.airbyte.persistence.job.models.JobRunConfig;
import io.airbyte.workers.ContainerOrchestratorConfig;
import io.airbyte.workers.process.AsyncKubePodStatus;
import io.airbyte.workers.process.ProcessFactory;
import io.airbyte.workers.storage.DocumentStoreClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.temporal.testing.TestActivityEnvironment;
import java.nio.file.Path;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Callable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

@SuppressWarnings("PMD.AvoidDuplicateLiterals")
class NormalizationActivityImplTest {

  private static TestActivityEnvironment testEnv;

  private static final UUID CONNECTION_ID = UUID.randomUUID();
  private static ContainerOrchestratorConfig mContainerOrchestratorConfig;
  private static WorkerConfigsProvider mWorkerConfigsProvider;
  private static ProcessFactory mProcessFactory;
  private static SecretsHydrator mSecretsHydrator;
  private static Path WORKSPACE_ROOT = Path.of("/unused/path");
  private static Configs.WorkerEnvironment WORKER_ENVIRONMENT = Configs.WorkerEnvironment.KUBERNETES;
  private static CloudStorageConfigs mCloudStorageConfigs;
  private static LogConfigs LOG_CONFIGS;
  private static String AIRBYTE_VERSION = "1.0";
  private static Integer SERVER_PORT = 8888;
  private static AirbyteConfigValidator mAirbyteConfigValidator;
  private static TemporalUtils mTemporalUtils;
  private static AirbyteApiClient mAirbyteApiClient;
  private static ConnectionApi mConnectionApi;
  private static FeatureFlagClient mFeatureFlagClient;
  private static NormalizationActivityImpl normalizationActivityImpl;
  private static NormalizationActivity normalizationActivity;
  private static ArgumentCaptor<Callable> callableCaptor;
  private static JobRunConfig JOB_RUN_CONFIG = new JobRunConfig().withJobId("unused").withAttemptId(0L);
  private static IntegrationLauncherConfig DESTINATION_CONFIG = new IntegrationLauncherConfig()
      .withDockerImage("unused")
      .withNormalizationDockerImage("unused:unused");
  private static WorkerConfigs mWorkerConfigs;
  private DocumentStoreClient mDocumentStoreClient;
  private KubernetesClient mKubernetesClient;

  @BeforeEach
  void beforeEach() throws Exception {
    testEnv = TestActivityEnvironment.newInstance();
    mContainerOrchestratorConfig = mock(ContainerOrchestratorConfig.class);
    mDocumentStoreClient = mock(DocumentStoreClient.class);
    mKubernetesClient = mock(KubernetesClient.class);
    when(mContainerOrchestratorConfig.workerEnvironment()).thenReturn(Configs.WorkerEnvironment.KUBERNETES);
    when(mContainerOrchestratorConfig.containerOrchestratorImage()).thenReturn("gcr.io/my-project/image-name:v2");
    when(mContainerOrchestratorConfig.containerOrchestratorImagePullPolicy()).thenReturn("Always");
    when(mContainerOrchestratorConfig.documentStoreClient()).thenReturn(mDocumentStoreClient);
    when(mDocumentStoreClient.read(argThat(key -> key.contains(AsyncKubePodStatus.SUCCEEDED.name())))).thenReturn(Optional.of(""));
    when(mContainerOrchestratorConfig.kubernetesClient()).thenReturn(mKubernetesClient);
    mWorkerConfigsProvider = mock(WorkerConfigsProvider.class);
    mWorkerConfigs = mock(WorkerConfigs.class);
    mCloudStorageConfigs = mock(CloudStorageConfigs.class);
    when(mCloudStorageConfigs.getType()).thenReturn(CloudStorageConfigs.WorkerStorageType.GCS);
    when(mCloudStorageConfigs.getGcsConfig()).thenReturn(new CloudStorageConfigs.GcsConfig("unused", "unused"));
    LOG_CONFIGS = new LogConfigs(Optional.of(mCloudStorageConfigs));
    when(mWorkerConfigsProvider.getConfig(any())).thenReturn(mWorkerConfigs);
    mProcessFactory = mock(ProcessFactory.class);
    mSecretsHydrator = mock(SecretsHydrator.class);
    mTemporalUtils = mock(TemporalUtils.class);
    mAirbyteConfigValidator = mock(AirbyteConfigValidator.class);
    mAirbyteApiClient = mock(AirbyteApiClient.class);
    mConnectionApi = mock(ConnectionApi.class);
    when(mAirbyteApiClient.getConnectionApi()).thenReturn(mConnectionApi);
    mFeatureFlagClient = mock(TestClient.class);
    when(mFeatureFlagClient.stringVariation(eq(UseCustomK8sScheduler.INSTANCE), any())).thenReturn("");
    callableCaptor = ArgumentCaptor.forClass(Callable.class);
    when(mTemporalUtils.withBackgroundHeartbeat(any(), callableCaptor.capture(), any())).thenReturn(new NormalizationSummary());
    normalizationActivityImpl = new NormalizationActivityImpl(
        Optional.of(mContainerOrchestratorConfig),
        mWorkerConfigsProvider,
        mProcessFactory,
        mSecretsHydrator,
        WORKSPACE_ROOT,
        WORKER_ENVIRONMENT,
        LOG_CONFIGS,
        AIRBYTE_VERSION,
        SERVER_PORT,
        mAirbyteConfigValidator,
        mTemporalUtils,
        mAirbyteApiClient,
        mFeatureFlagClient,
        mock(MetricClient.class));
    testEnv.registerActivitiesImplementations(normalizationActivityImpl);
    normalizationActivity = testEnv.newActivityStub(NormalizationActivity.class);
  }

  @AfterEach
  void afterEach() {
    testEnv.close();
  }

  @Test
  void checkNormalizationDataTypesSupportFromVersionString() {
    Assertions.assertFalse(NormalizationActivityImpl.normalizationSupportsV1DataTypes(withNormalizationVersion("0.2.5")));
    Assertions.assertFalse(NormalizationActivityImpl.normalizationSupportsV1DataTypes(withNormalizationVersion("0.1.1")));
    Assertions.assertTrue(NormalizationActivityImpl.normalizationSupportsV1DataTypes(withNormalizationVersion("0.3.0")));
    Assertions.assertFalse(NormalizationActivityImpl.normalizationSupportsV1DataTypes(withNormalizationVersion("0.4.1")));
    Assertions.assertFalse(NormalizationActivityImpl.normalizationSupportsV1DataTypes(withNormalizationVersion("dev")));
    Assertions.assertFalse(NormalizationActivityImpl.normalizationSupportsV1DataTypes(withNormalizationVersion("protocolv1")));
  }

  private IntegrationLauncherConfig withNormalizationVersion(final String version) {
    return new IntegrationLauncherConfig()
        .withNormalizationDockerImage("normalization:" + version);
  }

  @Test
  void retrievesCatalog() throws Exception {
    // Test that it retrieves the catalog via the API. This is enabled by feature flag currently.
    when(mFeatureFlagClient.boolVariation(eq(RemoveLargeSyncInputs.INSTANCE), any())).thenReturn(true);
    when(mConnectionApi.getConnection(new ConnectionIdRequestBody().connectionId(CONNECTION_ID))).thenReturn(
        new ConnectionRead().syncCatalog(new AirbyteCatalog()));
    normalizationActivity.normalize(JOB_RUN_CONFIG, DESTINATION_CONFIG, new NormalizationInput()
        .withConnectionId(CONNECTION_ID)
        .withWorkspaceId(UUID.randomUUID()));
    callableCaptor.getValue().call();
    verify(mConnectionApi).getConnection(new ConnectionIdRequestBody().connectionId(CONNECTION_ID));
  }

}
