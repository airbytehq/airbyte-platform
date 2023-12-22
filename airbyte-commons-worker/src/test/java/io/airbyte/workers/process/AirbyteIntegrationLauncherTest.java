/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.process;

import static io.airbyte.workers.process.Metadata.CHECK_JOB;
import static io.airbyte.workers.process.Metadata.CHECK_STEP_KEY;
import static io.airbyte.workers.process.Metadata.CONNECTOR_STEP;
import static io.airbyte.workers.process.Metadata.DISCOVER_JOB;
import static io.airbyte.workers.process.Metadata.JOB_TYPE_KEY;
import static io.airbyte.workers.process.Metadata.READ_STEP;
import static io.airbyte.workers.process.Metadata.SPEC_JOB;
import static io.airbyte.workers.process.Metadata.SYNC_JOB;
import static io.airbyte.workers.process.Metadata.SYNC_STEP_KEY;
import static io.airbyte.workers.process.Metadata.WRITE_STEP;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.airbyte.commons.features.EnvVariableFeatureFlags;
import io.airbyte.commons.features.FeatureFlags;
import io.airbyte.commons.workers.config.WorkerConfigs;
import io.airbyte.commons.workers.config.WorkerConfigsProvider.ResourceType;
import io.airbyte.config.Configs;
import io.airbyte.config.EnvConfigs;
import io.airbyte.config.ResourceRequirements;
import io.airbyte.config.SyncResourceRequirements;
import io.airbyte.config.WorkerEnvConstants;
import io.airbyte.workers.exception.WorkerException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.testcontainers.shaded.com.fasterxml.jackson.core.JsonProcessingException;

@ExtendWith(MockitoExtension.class)
class AirbyteIntegrationLauncherTest {

  private static final String CONFIG = "config";
  private static final String CATALOG = "catalog";
  private static final String CATALOG_ARG = "--catalog";
  private static final String CONFIG_ARG = "--config";
  private static final String JOB_ID = "0";
  private static final String STATE = "state";
  private static final String WRITE = "write";
  private static final int JOB_ATTEMPT = 0;
  private static final UUID CONNECTION_ID = null;
  private static final UUID WORKSPACE_ID = null;
  private static final Path JOB_ROOT = Path.of("abc");
  public static final String FAKE_IMAGE = "fake_image";
  private static final Map<String, String> CONFIG_FILES = ImmutableMap.of(
      CONFIG, "{}");
  private static final Map<String, String> CONFIG_CATALOG_FILES = ImmutableMap.of(
      CONFIG, "{}",
      CATALOG, "{}");
  private static final Map<String, String> CONFIG_CATALOG_STATE_FILES = ImmutableMap.of(
      CONFIG, "{}",
      CATALOG, "{}",
      "state", "{}");

  private static final FeatureFlags FEATURE_FLAGS = new EnvVariableFeatureFlags();
  private static final Configs CONFIGS = new EnvConfigs();

  private static final Map<String, String> JOB_METADATA =
      Maps.newHashMap(
          ImmutableMap.<String, String>builder()
              .put(WorkerEnvConstants.WORKER_CONNECTOR_IMAGE, FAKE_IMAGE)
              .put(WorkerEnvConstants.WORKER_JOB_ID, JOB_ID)
              .put(WorkerEnvConstants.WORKER_JOB_ATTEMPT, String.valueOf(JOB_ATTEMPT))
              .put(EnvVariableFeatureFlags.AUTO_DETECT_SCHEMA, String.valueOf(FEATURE_FLAGS.autoDetectSchema()))
              .put(EnvVariableFeatureFlags.APPLY_FIELD_SELECTION, String.valueOf(FEATURE_FLAGS.applyFieldSelection()))
              .put(EnvVariableFeatureFlags.FIELD_SELECTION_WORKSPACES, FEATURE_FLAGS.fieldSelectionWorkspaces())
              .put("USE_STREAM_CAPABLE_STATE", "true")
              .put(EnvConfigs.SOCAT_KUBE_CPU_LIMIT, CONFIGS.getSocatSidecarKubeCpuLimit())
              .put(EnvConfigs.SOCAT_KUBE_CPU_REQUEST, CONFIGS.getSocatSidecarKubeCpuRequest())
              .put(EnvConfigs.LAUNCHDARKLY_KEY, CONFIGS.getLaunchDarklyKey())
              .put(EnvConfigs.FEATURE_FLAG_CLIENT, CONFIGS.getFeatureFlagClient())
              .put(EnvConfigs.OTEL_COLLECTOR_ENDPOINT, CONFIGS.getOtelCollectorEndpoint())
              .build());

  private WorkerConfigs workerConfigs;
  private SyncResourceRequirements syncResourceRequirements;
  private ResourceRequirements rssReqDestination;
  private ResourceRequirements rssReqDestinationStdErr;
  private ResourceRequirements rssReqDestinationStdIn;
  private ResourceRequirements rssReqDestinationStdOut;
  private ResourceRequirements rssReqHeartbeat;
  private ResourceRequirements rssReqOrchestrator;
  private ResourceRequirements rssReqSource;
  private ResourceRequirements rssReqSourceStdErr;
  private ResourceRequirements rssReqSourceStdOut;
  @Mock
  private ProcessFactory processFactory;
  private AirbyteIntegrationLauncher launcher;

  @BeforeEach
  void setUp() {
    workerConfigs = new WorkerConfigs(new EnvConfigs());
    rssReqDestination = new ResourceRequirements().withMemoryRequest("100");
    rssReqDestinationStdErr = new ResourceRequirements().withMemoryRequest("101");
    rssReqDestinationStdIn = new ResourceRequirements().withMemoryRequest("102");
    rssReqDestinationStdOut = new ResourceRequirements().withMemoryRequest("103");
    rssReqHeartbeat = new ResourceRequirements().withMemoryRequest("200");
    rssReqOrchestrator = new ResourceRequirements().withMemoryRequest("300");
    rssReqSource = new ResourceRequirements().withMemoryRequest("400");
    rssReqSourceStdErr = new ResourceRequirements().withMemoryRequest("401");
    rssReqSourceStdOut = new ResourceRequirements().withMemoryRequest("402");
    syncResourceRequirements = new SyncResourceRequirements()
        .withDestination(rssReqDestination)
        .withDestinationStdErr(rssReqDestinationStdErr)
        .withDestinationStdIn(rssReqDestinationStdIn)
        .withDestinationStdOut(rssReqDestinationStdOut)
        .withHeartbeat(rssReqHeartbeat)
        .withOrchestrator(rssReqOrchestrator)
        .withSource(rssReqSource)
        .withSourceStdErr(rssReqSourceStdErr)
        .withSourceStdOut(rssReqSourceStdOut);
    launcher = new AirbyteIntegrationLauncher(JOB_ID, JOB_ATTEMPT, CONNECTION_ID, WORKSPACE_ID, FAKE_IMAGE, processFactory,
        workerConfigs.getResourceRequirements(), syncResourceRequirements, null, false,
        FEATURE_FLAGS, Collections.emptyMap(), Collections.emptyMap());
  }

  @Test
  void spec() throws WorkerException {
    launcher.spec(JOB_ROOT);

    final ConnectorResourceRequirements expectedResourceRequirements =
        AirbyteIntegrationLauncher.buildGenericConnectorResourceRequirements(workerConfigs.getResourceRequirements());
    Mockito.verify(processFactory).create(ResourceType.SPEC, SPEC_JOB, JOB_ID, JOB_ATTEMPT, CONNECTION_ID, WORKSPACE_ID, JOB_ROOT, FAKE_IMAGE, false,
        false,
        Collections.emptyMap(),
        null,
        expectedResourceRequirements, null, Map.of(JOB_TYPE_KEY, SPEC_JOB), JOB_METADATA,
        Map.of(),
        Collections.emptyMap(), "spec");
  }

  @Test
  void check() throws WorkerException {
    launcher.check(JOB_ROOT, CONFIG, "{}");

    final ConnectorResourceRequirements expectedResourceRequirements =
        AirbyteIntegrationLauncher.buildGenericConnectorResourceRequirements(workerConfigs.getResourceRequirements());
    Mockito.verify(processFactory).create(ResourceType.CHECK, CHECK_JOB, JOB_ID, JOB_ATTEMPT, CONNECTION_ID, WORKSPACE_ID, JOB_ROOT, FAKE_IMAGE,
        false, false, CONFIG_FILES, null,
        expectedResourceRequirements,
        null,
        Map.of(JOB_TYPE_KEY, CHECK_JOB, CHECK_STEP_KEY, CONNECTOR_STEP),
        JOB_METADATA,
        Map.of(),
        Collections.emptyMap(), "check",
        CONFIG_ARG, CONFIG);
  }

  @Test
  void discover() throws WorkerException {
    launcher.discover(JOB_ROOT, CONFIG, "{}");

    final ConnectorResourceRequirements expectedResourceRequirements =
        AirbyteIntegrationLauncher.buildGenericConnectorResourceRequirements(workerConfigs.getResourceRequirements());
    Mockito.verify(processFactory).create(ResourceType.DISCOVER, DISCOVER_JOB, JOB_ID, JOB_ATTEMPT, CONNECTION_ID, WORKSPACE_ID, JOB_ROOT, FAKE_IMAGE,
        false, false, CONFIG_FILES,
        null,
        expectedResourceRequirements,
        null,
        Map.of(JOB_TYPE_KEY, DISCOVER_JOB),
        JOB_METADATA,
        Map.of(),
        Collections.emptyMap(), "discover",
        CONFIG_ARG, CONFIG);
  }

  @Test
  void read() throws WorkerException {
    final var additionalLabels = Map.of("other1", "label1");

    launcher = new AirbyteIntegrationLauncher(JOB_ID, JOB_ATTEMPT, CONNECTION_ID, WORKSPACE_ID, FAKE_IMAGE, processFactory,
        workerConfigs.getResourceRequirements(), syncResourceRequirements, null, false,
        FEATURE_FLAGS, Collections.emptyMap(), additionalLabels);

    launcher.read(JOB_ROOT, CONFIG, "{}", CATALOG, "{}", STATE, "{}");

    final ConnectorResourceRequirements expectedResourceRequirements = new ConnectorResourceRequirements(
        rssReqSource,
        rssReqHeartbeat,
        rssReqSourceStdErr,
        null,
        rssReqSourceStdOut);
    Mockito.verify(processFactory).create(ResourceType.REPLICATION, READ_STEP, JOB_ID, JOB_ATTEMPT, CONNECTION_ID, WORKSPACE_ID, JOB_ROOT, FAKE_IMAGE,
        false, false,
        CONFIG_CATALOG_STATE_FILES,
        null,
        expectedResourceRequirements,
        null,
        Map.of(JOB_TYPE_KEY, SYNC_JOB, SYNC_STEP_KEY, READ_STEP, "other1", "label1"),
        JOB_METADATA,
        Map.of(),
        Collections.emptyMap(), Lists.newArrayList(
            "read",
            CONFIG_ARG, CONFIG,
            CATALOG_ARG, CATALOG,
            "--state", STATE).toArray(new String[0]));
  }

  @Test
  void readWithoutSyncResources() throws WorkerException {
    launcher = new AirbyteIntegrationLauncher(JOB_ID, JOB_ATTEMPT, CONNECTION_ID, WORKSPACE_ID, FAKE_IMAGE, processFactory,
        workerConfigs.getResourceRequirements(), null, null, false,
        FEATURE_FLAGS, Collections.emptyMap(), Collections.emptyMap());
    launcher.read(JOB_ROOT, CONFIG, "{}", CATALOG, "{}", STATE, "{}");

    final ConnectorResourceRequirements expectedResourceRequirements =
        AirbyteIntegrationLauncher.buildGenericConnectorResourceRequirements(workerConfigs.getResourceRequirements());
    Mockito.verify(processFactory).create(ResourceType.REPLICATION, READ_STEP, JOB_ID, JOB_ATTEMPT, CONNECTION_ID, WORKSPACE_ID, JOB_ROOT, FAKE_IMAGE,
        false, false,
        CONFIG_CATALOG_STATE_FILES,
        null,
        expectedResourceRequirements,
        null,
        Map.of(JOB_TYPE_KEY, SYNC_JOB, SYNC_STEP_KEY, READ_STEP),
        JOB_METADATA,
        Map.of(),
        Collections.emptyMap(), Lists.newArrayList(
            "read",
            CONFIG_ARG, CONFIG,
            CATALOG_ARG, CATALOG,
            "--state", STATE).toArray(new String[0]));
  }

  @Test
  void write() throws WorkerException, JsonProcessingException {
    launcher.write(JOB_ROOT, CONFIG, "{}", CATALOG, "{}");

    final ConnectorResourceRequirements expectedResourceRequirements = new ConnectorResourceRequirements(
        rssReqDestination,
        rssReqHeartbeat,
        rssReqDestinationStdErr,
        rssReqDestinationStdIn,
        rssReqDestinationStdOut);
    Mockito.verify(processFactory).create(ResourceType.REPLICATION, WRITE_STEP, JOB_ID, JOB_ATTEMPT, CONNECTION_ID, WORKSPACE_ID, JOB_ROOT,
        FAKE_IMAGE, false, true,
        CONFIG_CATALOG_FILES, null,
        expectedResourceRequirements,
        null,
        Map.of(JOB_TYPE_KEY, SYNC_JOB, SYNC_STEP_KEY, WRITE_STEP),
        JOB_METADATA,
        Map.of(),
        Collections.emptyMap(), WRITE,
        CONFIG_ARG, CONFIG,
        CATALOG_ARG, CATALOG);

    final var additionalEnvVars = Map.of("HELLO", "WORLD");
    final var additionalLabels = Map.of("other2", "label2");
    final var envVarLauncher =
        new AirbyteIntegrationLauncher(JOB_ID, JOB_ATTEMPT, CONNECTION_ID, WORKSPACE_ID, FAKE_IMAGE, processFactory,
            workerConfigs.getResourceRequirements(), syncResourceRequirements, null, false,
            FEATURE_FLAGS, additionalEnvVars, additionalLabels);
    envVarLauncher.write(JOB_ROOT, CONFIG, "{}", CATALOG, "{}");
    Mockito.verify(processFactory).create(ResourceType.REPLICATION, WRITE_STEP, JOB_ID, JOB_ATTEMPT, CONNECTION_ID, WORKSPACE_ID, JOB_ROOT,
        FAKE_IMAGE, false, true,
        CONFIG_CATALOG_FILES, null,
        expectedResourceRequirements,
        null,
        Map.of(JOB_TYPE_KEY, SYNC_JOB, SYNC_STEP_KEY, WRITE_STEP, "other2", "label2"),
        JOB_METADATA,
        Map.of(),
        additionalEnvVars, WRITE,
        CONFIG_ARG, CONFIG,
        CATALOG_ARG, CATALOG);

  }

  @Test
  void writeWithoutSyncResources() throws WorkerException, JsonProcessingException {
    launcher = new AirbyteIntegrationLauncher(JOB_ID, JOB_ATTEMPT, CONNECTION_ID, WORKSPACE_ID, FAKE_IMAGE, processFactory,
        workerConfigs.getResourceRequirements(), null, null, false,
        FEATURE_FLAGS, Collections.emptyMap(), Collections.emptyMap());
    launcher.write(JOB_ROOT, CONFIG, "{}", CATALOG, "{}");

    final ConnectorResourceRequirements expectedResourceRequirements =
        AirbyteIntegrationLauncher.buildGenericConnectorResourceRequirements(workerConfigs.getResourceRequirements());
    Mockito.verify(processFactory).create(ResourceType.REPLICATION, WRITE_STEP, JOB_ID, JOB_ATTEMPT, CONNECTION_ID, WORKSPACE_ID, JOB_ROOT,
        FAKE_IMAGE, false, true,
        CONFIG_CATALOG_FILES, null,
        expectedResourceRequirements,
        null,
        Map.of(JOB_TYPE_KEY, SYNC_JOB, SYNC_STEP_KEY, WRITE_STEP),
        JOB_METADATA,
        Map.of(),
        Collections.emptyMap(), WRITE,
        CONFIG_ARG, CONFIG,
        CATALOG_ARG, CATALOG);

    final var additionalEnvVars = Map.of("HELLO", "WORLD");
    final var envVarLauncher =
        new AirbyteIntegrationLauncher(JOB_ID, JOB_ATTEMPT, CONNECTION_ID, WORKSPACE_ID, FAKE_IMAGE, processFactory,
            workerConfigs.getResourceRequirements(), null, null, false,
            FEATURE_FLAGS, additionalEnvVars, Collections.emptyMap());
    envVarLauncher.write(JOB_ROOT, CONFIG, "{}", CATALOG, "{}");
    Mockito.verify(processFactory).create(ResourceType.REPLICATION, WRITE_STEP, JOB_ID, JOB_ATTEMPT, CONNECTION_ID, WORKSPACE_ID, JOB_ROOT,
        FAKE_IMAGE, false, true,
        CONFIG_CATALOG_FILES, null,
        expectedResourceRequirements,
        null,
        Map.of(JOB_TYPE_KEY, SYNC_JOB, SYNC_STEP_KEY, WRITE_STEP),
        JOB_METADATA,
        Map.of(),
        additionalEnvVars, WRITE,
        CONFIG_ARG, CONFIG,
        CATALOG_ARG, CATALOG);
  }

}
