/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.persistence.job;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableMap;
import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.version.Version;
import io.airbyte.config.ActorDefinitionResourceRequirements;
import io.airbyte.config.ActorDefinitionVersion;
import io.airbyte.config.DestinationConnection;
import io.airbyte.config.JobConfig;
import io.airbyte.config.JobConfig.ConfigType;
import io.airbyte.config.JobResetConnectionConfig;
import io.airbyte.config.JobSyncConfig;
import io.airbyte.config.JobSyncConfig.NamespaceDefinitionType;
import io.airbyte.config.JobTypeResourceLimit;
import io.airbyte.config.JobTypeResourceLimit.JobType;
import io.airbyte.config.OperatorNormalization;
import io.airbyte.config.OperatorNormalization.Option;
import io.airbyte.config.ResetSourceConfiguration;
import io.airbyte.config.ResourceRequirements;
import io.airbyte.config.ResourceRequirementsType;
import io.airbyte.config.SourceConnection;
import io.airbyte.config.StandardDestinationDefinition;
import io.airbyte.config.StandardSourceDefinition;
import io.airbyte.config.StandardSourceDefinition.SourceType;
import io.airbyte.config.StandardSync;
import io.airbyte.config.StandardSyncOperation;
import io.airbyte.config.StandardSyncOperation.OperatorType;
import io.airbyte.config.SyncResourceRequirements;
import io.airbyte.config.SyncResourceRequirementsKey;
import io.airbyte.config.provider.ResourceRequirementsProvider;
import io.airbyte.featureflag.DestResourceOverrides;
import io.airbyte.featureflag.OrchestratorResourceOverrides;
import io.airbyte.featureflag.SourceResourceOverrides;
import io.airbyte.featureflag.TestClient;
import io.airbyte.protocol.models.CatalogHelpers;
import io.airbyte.protocol.models.ConfiguredAirbyteCatalog;
import io.airbyte.protocol.models.ConfiguredAirbyteStream;
import io.airbyte.protocol.models.DestinationSyncMode;
import io.airbyte.protocol.models.Field;
import io.airbyte.protocol.models.JsonSchemaType;
import io.airbyte.protocol.models.StreamDescriptor;
import io.airbyte.protocol.models.SyncMode;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.platform.commons.util.StringUtils;
import org.mockito.ArgumentCaptor;

@SuppressWarnings("PMD.AvoidDuplicateLiterals")
class DefaultJobCreatorTest {

  private static final String DEFAULT_VARIANT = "default";
  private static final String STREAM1_NAME = "stream1";
  private static final String STREAM2_NAME = "stream2";
  private static final String STREAM3_NAME = "stream3";
  private static final String NAMESPACE = "namespace";
  private static final String FIELD_NAME = "id";
  private static final StreamDescriptor STREAM1_DESCRIPTOR = new StreamDescriptor().withName(STREAM1_NAME);
  private static final StreamDescriptor STREAM2_DESCRIPTOR = new StreamDescriptor().withName(STREAM2_NAME).withNamespace(NAMESPACE);

  private static final String SOURCE_IMAGE_NAME = "daxtarity/sourceimagename";
  private static final Version SOURCE_PROTOCOL_VERSION = new Version("0.2.2");
  private static final String DESTINATION_IMAGE_NAME = "daxtarity/destinationimagename";
  private static final Version DESTINATION_PROTOCOL_VERSION = new Version("0.2.3");
  private static final SourceConnection SOURCE_CONNECTION;
  private static final DestinationConnection DESTINATION_CONNECTION;
  private static final StandardSync STANDARD_SYNC;
  private static final StandardSyncOperation STANDARD_SYNC_OPERATION;

  private static final StandardSourceDefinition STANDARD_SOURCE_DEFINITION;
  private static final StandardSourceDefinition STANDARD_SOURCE_DEFINITION_WITH_SOURCE_TYPE;
  private static final StandardDestinationDefinition STANDARD_DESTINATION_DEFINITION;
  private static final ActorDefinitionVersion SOURCE_DEFINITION_VERSION;
  private static final ActorDefinitionVersion DESTINATION_DEFINITION_VERSION;
  private static final ConfiguredAirbyteCatalog CONFIGURED_AIRBYTE_CATALOG;
  private static final long JOB_ID = 12L;
  private static final UUID WORKSPACE_ID = UUID.randomUUID();

  private JobPersistence jobPersistence;
  private JobCreator jobCreator;
  private ResourceRequirementsProvider resourceRequirementsProvider;
  private ResourceRequirements workerResourceRequirements;
  private ResourceRequirements sourceResourceRequirements;
  private ResourceRequirements destResourceRequirements;

  private static final JsonNode PERSISTED_WEBHOOK_CONFIGS;

  private static final UUID WEBHOOK_CONFIG_ID;
  private static final String WEBHOOK_NAME;

  static {
    final UUID workspaceId = UUID.randomUUID();
    final UUID sourceId = UUID.randomUUID();
    final UUID sourceDefinitionId = UUID.randomUUID();
    WEBHOOK_CONFIG_ID = UUID.randomUUID();
    WEBHOOK_NAME = "test-name";

    final JsonNode implementationJson = Jsons.jsonNode(ImmutableMap.builder()
        .put("apiKey", "123-abc")
        .put("hostname", "airbyte.io")
        .build());

    SOURCE_CONNECTION = new SourceConnection()
        .withWorkspaceId(workspaceId)
        .withSourceDefinitionId(sourceDefinitionId)
        .withSourceId(sourceId)
        .withConfiguration(implementationJson)
        .withTombstone(false);

    final UUID destinationId = UUID.randomUUID();
    final UUID destinationDefinitionId = UUID.randomUUID();

    DESTINATION_CONNECTION = new DestinationConnection()
        .withWorkspaceId(workspaceId)
        .withDestinationDefinitionId(destinationDefinitionId)
        .withDestinationId(destinationId)
        .withConfiguration(implementationJson)
        .withTombstone(false);

    final UUID connectionId = UUID.randomUUID();
    final UUID operationId = UUID.randomUUID();

    final ConfiguredAirbyteStream stream1 = new ConfiguredAirbyteStream()
        .withStream(CatalogHelpers.createAirbyteStream(STREAM1_NAME, Field.of(FIELD_NAME, JsonSchemaType.STRING)))
        .withSyncMode(SyncMode.FULL_REFRESH)
        .withDestinationSyncMode(DestinationSyncMode.APPEND);
    final ConfiguredAirbyteStream stream2 = new ConfiguredAirbyteStream()
        .withStream(CatalogHelpers.createAirbyteStream(STREAM2_NAME, NAMESPACE, Field.of(FIELD_NAME, JsonSchemaType.STRING)))
        .withSyncMode(SyncMode.INCREMENTAL)
        .withDestinationSyncMode(DestinationSyncMode.APPEND);
    final ConfiguredAirbyteStream stream3 = new ConfiguredAirbyteStream()
        .withStream(CatalogHelpers.createAirbyteStream(STREAM3_NAME, NAMESPACE, Field.of(FIELD_NAME, JsonSchemaType.STRING)))
        .withSyncMode(SyncMode.FULL_REFRESH)
        .withDestinationSyncMode(DestinationSyncMode.OVERWRITE);
    CONFIGURED_AIRBYTE_CATALOG = new ConfiguredAirbyteCatalog().withStreams(List.of(stream1, stream2, stream3));

    STANDARD_SYNC = new StandardSync()
        .withConnectionId(connectionId)
        .withName("presto to hudi")
        .withNamespaceDefinition(NamespaceDefinitionType.SOURCE)
        .withNamespaceFormat(null)
        .withPrefix("presto_to_hudi")
        .withStatus(StandardSync.Status.ACTIVE)
        .withCatalog(CONFIGURED_AIRBYTE_CATALOG)
        .withSourceId(sourceId)
        .withDestinationId(destinationId)
        .withOperationIds(List.of(operationId));

    STANDARD_SYNC_OPERATION = new StandardSyncOperation()
        .withOperationId(operationId)
        .withName("normalize")
        .withTombstone(false)
        .withOperatorType(OperatorType.NORMALIZATION)
        .withOperatorNormalization(new OperatorNormalization().withOption(Option.BASIC));

    PERSISTED_WEBHOOK_CONFIGS = Jsons.deserialize(
        String.format("{\"webhookConfigs\": [{\"id\": \"%s\", \"name\": \"%s\", \"authToken\": {\"_secret\": \"a-secret_v1\"}}]}",
            WEBHOOK_CONFIG_ID, WEBHOOK_NAME));

    STANDARD_SOURCE_DEFINITION = new StandardSourceDefinition().withCustom(false);
    STANDARD_SOURCE_DEFINITION_WITH_SOURCE_TYPE = new StandardSourceDefinition().withSourceType(SourceType.DATABASE).withCustom(false);
    STANDARD_DESTINATION_DEFINITION = new StandardDestinationDefinition().withCustom(false);

    SOURCE_DEFINITION_VERSION = new ActorDefinitionVersion().withVersionId(UUID.randomUUID());
    DESTINATION_DEFINITION_VERSION = new ActorDefinitionVersion().withVersionId(UUID.randomUUID());
  }

  @BeforeEach
  void setup() {
    jobPersistence = mock(JobPersistence.class);
    workerResourceRequirements = new ResourceRequirements()
        .withCpuLimit("0.2")
        .withCpuRequest("0.2")
        .withMemoryLimit("200Mi")
        .withMemoryRequest("200Mi");
    sourceResourceRequirements = new ResourceRequirements()
        .withCpuLimit("0.1")
        .withCpuRequest("0.1")
        .withMemoryLimit("400Mi")
        .withMemoryRequest("300Mi");
    destResourceRequirements = new ResourceRequirements()
        .withCpuLimit("0.3")
        .withCpuRequest("0.3")
        .withMemoryLimit("1200Mi")
        .withMemoryRequest("1000Mi");
    resourceRequirementsProvider = mock(ResourceRequirementsProvider.class);
    when(resourceRequirementsProvider.getResourceRequirements(any(), any(), any()))
        .thenReturn(workerResourceRequirements);
    jobCreator = new DefaultJobCreator(jobPersistence, resourceRequirementsProvider, new TestClient());
  }

  @Test
  void testCreateSyncJob() throws IOException {
    final Optional<String> expectedSourceType = Optional.of("database");
    when(resourceRequirementsProvider.getResourceRequirements(ResourceRequirementsType.ORCHESTRATOR, expectedSourceType, DEFAULT_VARIANT))
        .thenReturn(workerResourceRequirements);
    when(resourceRequirementsProvider.getResourceRequirements(ResourceRequirementsType.SOURCE, expectedSourceType, DEFAULT_VARIANT))
        .thenReturn(sourceResourceRequirements);
    when(resourceRequirementsProvider.getResourceRequirements(ResourceRequirementsType.DESTINATION, expectedSourceType, DEFAULT_VARIANT))
        .thenReturn(destResourceRequirements);
    // More explicit resource requirements to verify data mapping
    final ResourceRequirements destStderrResourceRequirements = new ResourceRequirements().withCpuLimit("10");
    when(resourceRequirementsProvider.getResourceRequirements(ResourceRequirementsType.DESTINATION_STDERR, expectedSourceType, DEFAULT_VARIANT))
        .thenReturn(destStderrResourceRequirements);
    final ResourceRequirements destStdinResourceRequirements = new ResourceRequirements().withCpuLimit("11");
    when(resourceRequirementsProvider.getResourceRequirements(ResourceRequirementsType.DESTINATION_STDIN, expectedSourceType, DEFAULT_VARIANT))
        .thenReturn(destStdinResourceRequirements);
    final ResourceRequirements destStdoutResourceRequirements = new ResourceRequirements().withCpuLimit("12");
    when(resourceRequirementsProvider.getResourceRequirements(ResourceRequirementsType.DESTINATION_STDOUT, expectedSourceType, DEFAULT_VARIANT))
        .thenReturn(destStdoutResourceRequirements);
    final ResourceRequirements heartbeatResourceRequirements = new ResourceRequirements().withCpuLimit("13");
    when(resourceRequirementsProvider.getResourceRequirements(ResourceRequirementsType.HEARTBEAT, expectedSourceType, DEFAULT_VARIANT))
        .thenReturn(heartbeatResourceRequirements);
    final ResourceRequirements srcStderrResourceRequirements = new ResourceRequirements().withCpuLimit("14");
    when(resourceRequirementsProvider.getResourceRequirements(ResourceRequirementsType.SOURCE_STDERR, expectedSourceType, DEFAULT_VARIANT))
        .thenReturn(srcStderrResourceRequirements);
    final ResourceRequirements srcStdoutResourceRequirements = new ResourceRequirements().withCpuLimit("14");
    when(resourceRequirementsProvider.getResourceRequirements(ResourceRequirementsType.SOURCE_STDOUT, expectedSourceType, DEFAULT_VARIANT))
        .thenReturn(srcStdoutResourceRequirements);

    final SyncResourceRequirements expectedSyncResourceRequirements = new SyncResourceRequirements()
        .withConfigKey(new SyncResourceRequirementsKey().withVariant(DEFAULT_VARIANT).withSubType("database"))
        .withDestination(destResourceRequirements)
        .withDestinationStdErr(destStderrResourceRequirements)
        .withDestinationStdIn(destStdinResourceRequirements)
        .withDestinationStdOut(destStdoutResourceRequirements)
        .withOrchestrator(workerResourceRequirements)
        .withHeartbeat(heartbeatResourceRequirements)
        .withSource(sourceResourceRequirements)
        .withSourceStdErr(srcStderrResourceRequirements)
        .withSourceStdOut(srcStdoutResourceRequirements);

    final JobSyncConfig jobSyncConfig = new JobSyncConfig()
        .withNamespaceDefinition(STANDARD_SYNC.getNamespaceDefinition())
        .withNamespaceFormat(STANDARD_SYNC.getNamespaceFormat())
        .withPrefix(STANDARD_SYNC.getPrefix())
        .withSourceDockerImage(SOURCE_IMAGE_NAME)
        .withSourceProtocolVersion(SOURCE_PROTOCOL_VERSION)
        .withDestinationDockerImage(DESTINATION_IMAGE_NAME)
        .withDestinationProtocolVersion(DESTINATION_PROTOCOL_VERSION)
        .withConfiguredAirbyteCatalog(STANDARD_SYNC.getCatalog())
        .withOperationSequence(List.of(STANDARD_SYNC_OPERATION))
        .withSyncResourceRequirements(expectedSyncResourceRequirements)
        .withWebhookOperationConfigs(PERSISTED_WEBHOOK_CONFIGS)
        .withIsSourceCustomConnector(false)
        .withIsDestinationCustomConnector(false)
        .withWorkspaceId(WORKSPACE_ID)
        .withSourceDefinitionVersionId(SOURCE_DEFINITION_VERSION.getVersionId())
        .withDestinationDefinitionVersionId(DESTINATION_DEFINITION_VERSION.getVersionId());

    final JobConfig jobConfig = new JobConfig()
        .withConfigType(JobConfig.ConfigType.SYNC)
        .withSync(jobSyncConfig);

    jobCreator.createSyncJob(
        SOURCE_CONNECTION,
        DESTINATION_CONNECTION,
        STANDARD_SYNC,
        SOURCE_IMAGE_NAME,
        SOURCE_PROTOCOL_VERSION,
        DESTINATION_IMAGE_NAME,
        DESTINATION_PROTOCOL_VERSION,
        List.of(STANDARD_SYNC_OPERATION),
        PERSISTED_WEBHOOK_CONFIGS,
        STANDARD_SOURCE_DEFINITION_WITH_SOURCE_TYPE,
        STANDARD_DESTINATION_DEFINITION,
        SOURCE_DEFINITION_VERSION,
        DESTINATION_DEFINITION_VERSION,
        WORKSPACE_ID);

    final String expectedScope = STANDARD_SYNC.getConnectionId().toString();
    verify(jobPersistence).enqueueJob(expectedScope, jobConfig);
  }

  @Test
  void testCreateSyncJobEnsureNoQueuing() throws IOException {
    final JobSyncConfig jobSyncConfig = new JobSyncConfig()
        .withNamespaceDefinition(STANDARD_SYNC.getNamespaceDefinition())
        .withNamespaceFormat(STANDARD_SYNC.getNamespaceFormat())
        .withPrefix(STANDARD_SYNC.getPrefix())
        .withSourceDockerImage(SOURCE_IMAGE_NAME)
        .withDestinationProtocolVersion(SOURCE_PROTOCOL_VERSION)
        .withDestinationDockerImage(DESTINATION_IMAGE_NAME)
        .withDestinationProtocolVersion(DESTINATION_PROTOCOL_VERSION)
        .withConfiguredAirbyteCatalog(STANDARD_SYNC.getCatalog())
        .withOperationSequence(List.of(STANDARD_SYNC_OPERATION))
        .withSourceDefinitionVersionId(SOURCE_DEFINITION_VERSION.getVersionId())
        .withDestinationDefinitionVersionId(DESTINATION_DEFINITION_VERSION.getVersionId());

    final JobConfig jobConfig = new JobConfig()
        .withConfigType(JobConfig.ConfigType.SYNC)
        .withSync(jobSyncConfig);

    final String expectedScope = STANDARD_SYNC.getConnectionId().toString();
    when(jobPersistence.enqueueJob(expectedScope, jobConfig)).thenReturn(Optional.empty());

    assertTrue(jobCreator.createSyncJob(
        SOURCE_CONNECTION,
        DESTINATION_CONNECTION,
        STANDARD_SYNC,
        SOURCE_IMAGE_NAME,
        SOURCE_PROTOCOL_VERSION,
        DESTINATION_IMAGE_NAME,
        DESTINATION_PROTOCOL_VERSION,
        List.of(STANDARD_SYNC_OPERATION),
        null,
        STANDARD_SOURCE_DEFINITION,
        STANDARD_DESTINATION_DEFINITION,
        SOURCE_DEFINITION_VERSION,
        DESTINATION_DEFINITION_VERSION,
        UUID.randomUUID()).isEmpty());
  }

  @Test
  void testCreateSyncJobDefaultWorkerResourceReqs() throws IOException {
    jobCreator.createSyncJob(
        SOURCE_CONNECTION,
        DESTINATION_CONNECTION,
        STANDARD_SYNC,
        SOURCE_IMAGE_NAME,
        SOURCE_PROTOCOL_VERSION,
        DESTINATION_IMAGE_NAME,
        DESTINATION_PROTOCOL_VERSION,
        List.of(STANDARD_SYNC_OPERATION),
        null,
        STANDARD_SOURCE_DEFINITION,
        STANDARD_DESTINATION_DEFINITION,
        SOURCE_DEFINITION_VERSION,
        DESTINATION_DEFINITION_VERSION,
        WORKSPACE_ID);

    final SyncResourceRequirements expectedSyncResourceRequirements = new SyncResourceRequirements()
        .withConfigKey(new SyncResourceRequirementsKey().withVariant(DEFAULT_VARIANT))
        .withDestination(workerResourceRequirements)
        .withDestinationStdErr(workerResourceRequirements)
        .withDestinationStdIn(workerResourceRequirements)
        .withDestinationStdOut(workerResourceRequirements)
        .withOrchestrator(workerResourceRequirements)
        .withHeartbeat(workerResourceRequirements)
        .withSource(workerResourceRequirements)
        .withSourceStdErr(workerResourceRequirements)
        .withSourceStdOut(workerResourceRequirements);

    final JobSyncConfig expectedJobSyncConfig = new JobSyncConfig()
        .withNamespaceDefinition(STANDARD_SYNC.getNamespaceDefinition())
        .withNamespaceFormat(STANDARD_SYNC.getNamespaceFormat())
        .withPrefix(STANDARD_SYNC.getPrefix())
        .withSourceDockerImage(SOURCE_IMAGE_NAME)
        .withSourceProtocolVersion(SOURCE_PROTOCOL_VERSION)
        .withDestinationDockerImage(DESTINATION_IMAGE_NAME)
        .withDestinationProtocolVersion(DESTINATION_PROTOCOL_VERSION)
        .withConfiguredAirbyteCatalog(STANDARD_SYNC.getCatalog())
        .withOperationSequence(List.of(STANDARD_SYNC_OPERATION))
        .withSyncResourceRequirements(expectedSyncResourceRequirements)
        .withIsSourceCustomConnector(false)
        .withIsDestinationCustomConnector(false)
        .withWorkspaceId(WORKSPACE_ID)
        .withSourceDefinitionVersionId(SOURCE_DEFINITION_VERSION.getVersionId())
        .withDestinationDefinitionVersionId(DESTINATION_DEFINITION_VERSION.getVersionId());

    final JobConfig expectedJobConfig = new JobConfig()
        .withConfigType(JobConfig.ConfigType.SYNC)
        .withSync(expectedJobSyncConfig);

    final String expectedScope = STANDARD_SYNC.getConnectionId().toString();

    verify(jobPersistence, times(1)).enqueueJob(expectedScope, expectedJobConfig);
  }

  @Test
  void testCreateSyncJobConnectionResourceReqs() throws IOException {
    final ResourceRequirements standardSyncResourceRequirements = new ResourceRequirements()
        .withCpuLimit("0.5")
        .withCpuRequest("0.5")
        .withMemoryLimit("500Mi")
        .withMemoryRequest("500Mi");
    final StandardSync standardSync = Jsons.clone(STANDARD_SYNC).withResourceRequirements(standardSyncResourceRequirements);

    jobCreator.createSyncJob(
        SOURCE_CONNECTION,
        DESTINATION_CONNECTION,
        standardSync,
        SOURCE_IMAGE_NAME,
        SOURCE_PROTOCOL_VERSION,
        DESTINATION_IMAGE_NAME,
        DESTINATION_PROTOCOL_VERSION,
        List.of(STANDARD_SYNC_OPERATION),
        null,
        STANDARD_SOURCE_DEFINITION,
        STANDARD_DESTINATION_DEFINITION,
        SOURCE_DEFINITION_VERSION,
        DESTINATION_DEFINITION_VERSION,
        WORKSPACE_ID);

    final SyncResourceRequirements expectedSyncResourceRequirements = new SyncResourceRequirements()
        .withConfigKey(new SyncResourceRequirementsKey().withVariant(DEFAULT_VARIANT))
        .withDestination(standardSyncResourceRequirements)
        .withDestinationStdErr(workerResourceRequirements)
        .withDestinationStdIn(workerResourceRequirements)
        .withDestinationStdOut(workerResourceRequirements)
        .withOrchestrator(standardSyncResourceRequirements)
        .withHeartbeat(workerResourceRequirements)
        .withSource(standardSyncResourceRequirements)
        .withSourceStdErr(workerResourceRequirements)
        .withSourceStdOut(workerResourceRequirements);

    final JobSyncConfig expectedJobSyncConfig = new JobSyncConfig()
        .withNamespaceDefinition(STANDARD_SYNC.getNamespaceDefinition())
        .withNamespaceFormat(STANDARD_SYNC.getNamespaceFormat())
        .withPrefix(STANDARD_SYNC.getPrefix())
        .withSourceDockerImage(SOURCE_IMAGE_NAME)
        .withSourceProtocolVersion(SOURCE_PROTOCOL_VERSION)
        .withDestinationDockerImage(DESTINATION_IMAGE_NAME)
        .withDestinationProtocolVersion(DESTINATION_PROTOCOL_VERSION)
        .withConfiguredAirbyteCatalog(STANDARD_SYNC.getCatalog())
        .withOperationSequence(List.of(STANDARD_SYNC_OPERATION))
        .withSyncResourceRequirements(expectedSyncResourceRequirements)
        .withIsSourceCustomConnector(false)
        .withIsDestinationCustomConnector(false)
        .withWorkspaceId(WORKSPACE_ID)
        .withSourceDefinitionVersionId(SOURCE_DEFINITION_VERSION.getVersionId())
        .withDestinationDefinitionVersionId(DESTINATION_DEFINITION_VERSION.getVersionId());

    final JobConfig expectedJobConfig = new JobConfig()
        .withConfigType(JobConfig.ConfigType.SYNC)
        .withSync(expectedJobSyncConfig);

    final String expectedScope = STANDARD_SYNC.getConnectionId().toString();

    verify(jobPersistence, times(1)).enqueueJob(expectedScope, expectedJobConfig);
  }

  @Test
  void testCreateSyncJobSourceAndDestinationResourceReqs() throws IOException {
    final ResourceRequirements sourceResourceRequirements = new ResourceRequirements()
        .withCpuLimit("0.7")
        .withCpuRequest("0.7")
        .withMemoryLimit("700Mi")
        .withMemoryRequest("700Mi");
    final ResourceRequirements destResourceRequirements = new ResourceRequirements()
        .withCpuLimit("0.8")
        .withCpuRequest("0.8")
        .withMemoryLimit("800Mi")
        .withMemoryRequest("800Mi");

    jobCreator.createSyncJob(
        SOURCE_CONNECTION,
        DESTINATION_CONNECTION,
        STANDARD_SYNC,
        SOURCE_IMAGE_NAME,
        SOURCE_PROTOCOL_VERSION,
        DESTINATION_IMAGE_NAME,
        DESTINATION_PROTOCOL_VERSION,
        List.of(STANDARD_SYNC_OPERATION),
        null,
        new StandardSourceDefinition().withResourceRequirements(new ActorDefinitionResourceRequirements().withDefault(sourceResourceRequirements)),
        new StandardDestinationDefinition().withResourceRequirements(new ActorDefinitionResourceRequirements().withJobSpecific(List.of(
            new JobTypeResourceLimit().withJobType(JobType.SYNC).withResourceRequirements(destResourceRequirements)))),
        SOURCE_DEFINITION_VERSION,
        DESTINATION_DEFINITION_VERSION,
        WORKSPACE_ID);

    final SyncResourceRequirements expectedSyncResourceRequirements = new SyncResourceRequirements()
        .withConfigKey(new SyncResourceRequirementsKey().withVariant(DEFAULT_VARIANT))
        .withDestination(destResourceRequirements)
        .withDestinationStdErr(workerResourceRequirements)
        .withDestinationStdIn(workerResourceRequirements)
        .withDestinationStdOut(workerResourceRequirements)
        .withOrchestrator(workerResourceRequirements)
        .withHeartbeat(workerResourceRequirements)
        .withSource(sourceResourceRequirements)
        .withSourceStdErr(workerResourceRequirements)
        .withSourceStdOut(workerResourceRequirements);

    final JobSyncConfig expectedJobSyncConfig = new JobSyncConfig()
        .withNamespaceDefinition(STANDARD_SYNC.getNamespaceDefinition())
        .withNamespaceFormat(STANDARD_SYNC.getNamespaceFormat())
        .withPrefix(STANDARD_SYNC.getPrefix())
        .withSourceDockerImage(SOURCE_IMAGE_NAME)
        .withSourceProtocolVersion(SOURCE_PROTOCOL_VERSION)
        .withDestinationDockerImage(DESTINATION_IMAGE_NAME)
        .withDestinationProtocolVersion(DESTINATION_PROTOCOL_VERSION)
        .withConfiguredAirbyteCatalog(STANDARD_SYNC.getCatalog())
        .withOperationSequence(List.of(STANDARD_SYNC_OPERATION))
        .withSyncResourceRequirements(expectedSyncResourceRequirements)
        .withIsSourceCustomConnector(false)
        .withIsDestinationCustomConnector(false)
        .withWorkspaceId(WORKSPACE_ID)
        .withSourceDefinitionVersionId(SOURCE_DEFINITION_VERSION.getVersionId())
        .withDestinationDefinitionVersionId(DESTINATION_DEFINITION_VERSION.getVersionId());

    final JobConfig expectedJobConfig = new JobConfig()
        .withConfigType(JobConfig.ConfigType.SYNC)
        .withSync(expectedJobSyncConfig);

    final String expectedScope = STANDARD_SYNC.getConnectionId().toString();

    verify(jobPersistence, times(1)).enqueueJob(expectedScope, expectedJobConfig);
  }

  @ParameterizedTest
  @MethodSource("resourceOverrideMatrix")
  void testDestinationResourceReqsOverrides(final String cpuReqOverride,
                                            final String cpuLimitOverride,
                                            final String memReqOverride,
                                            final String memLimitOverride)
      throws IOException {
    final var overrides = new HashMap<>();
    if (cpuReqOverride != null) {
      overrides.put("cpu_request", cpuReqOverride);
    }
    if (cpuLimitOverride != null) {
      overrides.put("cpu_limit", cpuLimitOverride);
    }
    if (memReqOverride != null) {
      overrides.put("memory_request", memReqOverride);
    }
    if (memLimitOverride != null) {
      overrides.put("memory_limit", memLimitOverride);
    }

    final ResourceRequirements originalReqs = new ResourceRequirements()
        .withCpuLimit("0.8")
        .withCpuRequest("0.8")
        .withMemoryLimit("800Mi")
        .withMemoryRequest("800Mi");

    final var jobCreator = new DefaultJobCreator(jobPersistence, resourceRequirementsProvider,
        new TestClient(Map.of(DestResourceOverrides.INSTANCE.getKey(), Jsons.serialize(overrides))));

    jobCreator.createSyncJob(
        SOURCE_CONNECTION,
        DESTINATION_CONNECTION,
        STANDARD_SYNC,
        SOURCE_IMAGE_NAME,
        SOURCE_PROTOCOL_VERSION,
        DESTINATION_IMAGE_NAME,
        DESTINATION_PROTOCOL_VERSION,
        List.of(STANDARD_SYNC_OPERATION),
        null,
        new StandardSourceDefinition().withResourceRequirements(new ActorDefinitionResourceRequirements().withDefault(sourceResourceRequirements)),
        new StandardDestinationDefinition().withResourceRequirements(new ActorDefinitionResourceRequirements().withJobSpecific(List.of(
            new JobTypeResourceLimit().withJobType(JobType.SYNC).withResourceRequirements(originalReqs)))),
        SOURCE_DEFINITION_VERSION,
        DESTINATION_DEFINITION_VERSION,
        WORKSPACE_ID);

    final ArgumentCaptor<JobConfig> configCaptor = ArgumentCaptor.forClass(JobConfig.class);
    verify(jobPersistence, times(1)).enqueueJob(any(), configCaptor.capture());
    final var destConfigValues = configCaptor.getValue().getSync().getSyncResourceRequirements().getDestination();

    final var expectedCpuReq = StringUtils.isNotBlank(cpuReqOverride) ? cpuReqOverride : originalReqs.getCpuRequest();
    assertEquals(expectedCpuReq, destConfigValues.getCpuRequest());

    final var expectedCpuLimit = StringUtils.isNotBlank(cpuLimitOverride) ? cpuLimitOverride : originalReqs.getCpuLimit();
    assertEquals(expectedCpuLimit, destConfigValues.getCpuLimit());

    final var expectedMemReq = StringUtils.isNotBlank(memReqOverride) ? memReqOverride : originalReqs.getMemoryRequest();
    assertEquals(expectedMemReq, destConfigValues.getMemoryRequest());

    final var expectedMemLimit = StringUtils.isNotBlank(memLimitOverride) ? memLimitOverride : originalReqs.getMemoryLimit();
    assertEquals(expectedMemLimit, destConfigValues.getMemoryLimit());
  }

  @ParameterizedTest
  @MethodSource("resourceOverrideMatrix")
  void testOrchestratorResourceReqsOverrides(final String cpuReqOverride,
                                             final String cpuLimitOverride,
                                             final String memReqOverride,
                                             final String memLimitOverride)
      throws IOException {
    final var overrides = new HashMap<>();
    if (cpuReqOverride != null) {
      overrides.put("cpu_request", cpuReqOverride);
    }
    if (cpuLimitOverride != null) {
      overrides.put("cpu_limit", cpuLimitOverride);
    }
    if (memReqOverride != null) {
      overrides.put("memory_request", memReqOverride);
    }
    if (memLimitOverride != null) {
      overrides.put("memory_limit", memLimitOverride);
    }

    final ResourceRequirements originalReqs = new ResourceRequirements()
        .withCpuLimit("0.8")
        .withCpuRequest("0.8")
        .withMemoryLimit("800Mi")
        .withMemoryRequest("800Mi");

    final var jobCreator = new DefaultJobCreator(jobPersistence, resourceRequirementsProvider,
        new TestClient(Map.of(OrchestratorResourceOverrides.INSTANCE.getKey(), Jsons.serialize(overrides))));

    final var standardSync = new StandardSync()
        .withConnectionId(UUID.randomUUID())
        .withName("presto to hudi")
        .withNamespaceDefinition(NamespaceDefinitionType.SOURCE)
        .withNamespaceFormat(null)
        .withPrefix("presto_to_hudi")
        .withStatus(StandardSync.Status.ACTIVE)
        .withCatalog(CONFIGURED_AIRBYTE_CATALOG)
        .withSourceId(UUID.randomUUID())
        .withDestinationId(UUID.randomUUID())
        .withOperationIds(List.of(UUID.randomUUID()))
        .withResourceRequirements(originalReqs);

    jobCreator.createSyncJob(
        SOURCE_CONNECTION,
        DESTINATION_CONNECTION,
        standardSync,
        SOURCE_IMAGE_NAME,
        SOURCE_PROTOCOL_VERSION,
        DESTINATION_IMAGE_NAME,
        DESTINATION_PROTOCOL_VERSION,
        List.of(STANDARD_SYNC_OPERATION),
        null,
        new StandardSourceDefinition().withResourceRequirements(new ActorDefinitionResourceRequirements().withDefault(sourceResourceRequirements)),
        new StandardDestinationDefinition().withResourceRequirements(new ActorDefinitionResourceRequirements().withDefault(destResourceRequirements)),
        SOURCE_DEFINITION_VERSION,
        DESTINATION_DEFINITION_VERSION,
        WORKSPACE_ID);

    final ArgumentCaptor<JobConfig> configCaptor = ArgumentCaptor.forClass(JobConfig.class);
    verify(jobPersistence, times(1)).enqueueJob(any(), configCaptor.capture());
    final var orchestratorConfigValues = configCaptor.getValue().getSync().getSyncResourceRequirements().getOrchestrator();

    final var expectedCpuReq = StringUtils.isNotBlank(cpuReqOverride) ? cpuReqOverride : originalReqs.getCpuRequest();
    assertEquals(expectedCpuReq, orchestratorConfigValues.getCpuRequest());

    final var expectedCpuLimit = StringUtils.isNotBlank(cpuLimitOverride) ? cpuLimitOverride : originalReqs.getCpuLimit();
    assertEquals(expectedCpuLimit, orchestratorConfigValues.getCpuLimit());

    final var expectedMemReq = StringUtils.isNotBlank(memReqOverride) ? memReqOverride : originalReqs.getMemoryRequest();
    assertEquals(expectedMemReq, orchestratorConfigValues.getMemoryRequest());

    final var expectedMemLimit = StringUtils.isNotBlank(memLimitOverride) ? memLimitOverride : originalReqs.getMemoryLimit();
    assertEquals(expectedMemLimit, orchestratorConfigValues.getMemoryLimit());
  }

  @ParameterizedTest
  @MethodSource("resourceOverrideMatrix")
  void testSourceResourceReqsOverrides(final String cpuReqOverride,
                                       final String cpuLimitOverride,
                                       final String memReqOverride,
                                       final String memLimitOverride)
      throws IOException {
    final var overrides = new HashMap<>();
    if (cpuReqOverride != null) {
      overrides.put("cpu_request", cpuReqOverride);
    }
    if (cpuLimitOverride != null) {
      overrides.put("cpu_limit", cpuLimitOverride);
    }
    if (memReqOverride != null) {
      overrides.put("memory_request", memReqOverride);
    }
    if (memLimitOverride != null) {
      overrides.put("memory_limit", memLimitOverride);
    }

    final ResourceRequirements originalReqs = new ResourceRequirements()
        .withCpuLimit("0.8")
        .withCpuRequest("0.8")
        .withMemoryLimit("800Mi")
        .withMemoryRequest("800Mi");

    final var jobCreator = new DefaultJobCreator(jobPersistence, resourceRequirementsProvider,
        new TestClient(Map.of(SourceResourceOverrides.INSTANCE.getKey(), Jsons.serialize(overrides))));

    jobCreator.createSyncJob(
        SOURCE_CONNECTION,
        DESTINATION_CONNECTION,
        STANDARD_SYNC,
        SOURCE_IMAGE_NAME,
        SOURCE_PROTOCOL_VERSION,
        DESTINATION_IMAGE_NAME,
        DESTINATION_PROTOCOL_VERSION,
        List.of(STANDARD_SYNC_OPERATION),
        null,
        new StandardSourceDefinition().withResourceRequirements(new ActorDefinitionResourceRequirements().withJobSpecific(List.of(
            new JobTypeResourceLimit().withJobType(JobType.SYNC).withResourceRequirements(originalReqs)))),
        new StandardDestinationDefinition().withResourceRequirements(new ActorDefinitionResourceRequirements().withDefault(destResourceRequirements)),
        SOURCE_DEFINITION_VERSION,
        DESTINATION_DEFINITION_VERSION,
        WORKSPACE_ID);

    final ArgumentCaptor<JobConfig> configCaptor = ArgumentCaptor.forClass(JobConfig.class);
    verify(jobPersistence, times(1)).enqueueJob(any(), configCaptor.capture());
    final var sourceConfigValues = configCaptor.getValue().getSync().getSyncResourceRequirements().getSource();

    final var expectedCpuReq = StringUtils.isNotBlank(cpuReqOverride) ? cpuReqOverride : originalReqs.getCpuRequest();
    assertEquals(expectedCpuReq, sourceConfigValues.getCpuRequest());

    final var expectedCpuLimit = StringUtils.isNotBlank(cpuLimitOverride) ? cpuLimitOverride : originalReqs.getCpuLimit();
    assertEquals(expectedCpuLimit, sourceConfigValues.getCpuLimit());

    final var expectedMemReq = StringUtils.isNotBlank(memReqOverride) ? memReqOverride : originalReqs.getMemoryRequest();
    assertEquals(expectedMemReq, sourceConfigValues.getMemoryRequest());

    final var expectedMemLimit = StringUtils.isNotBlank(memLimitOverride) ? memLimitOverride : originalReqs.getMemoryLimit();
    assertEquals(expectedMemLimit, sourceConfigValues.getMemoryLimit());
  }

  private static Stream<Arguments> resourceOverrideMatrix() {
    return Stream.of(
        Arguments.of("0.7", "0.4", "1000Mi", "2000Mi"),
        Arguments.of("0.3", null, "1000Mi", null),
        Arguments.of(null, null, null, null),
        Arguments.of(null, "0.4", null, null),
        Arguments.of("3", "3", "3000Mi", "3000Mi"),
        Arguments.of("4", "5", "6000Mi", "7000Mi"));
  }

  @ParameterizedTest
  @MethodSource("weirdnessOverrideMatrix")
  void ignoresOverridesIfJsonStringWeird(final String weirdness) throws IOException {
    final ResourceRequirements originalReqs = new ResourceRequirements()
        .withCpuLimit("0.8")
        .withCpuRequest("0.8")
        .withMemoryLimit("800Mi")
        .withMemoryRequest("800Mi");

    final var jobCreator = new DefaultJobCreator(jobPersistence, resourceRequirementsProvider,
        new TestClient(Map.of(DestResourceOverrides.INSTANCE.getKey(), Jsons.serialize(weirdness))));

    jobCreator.createSyncJob(
        SOURCE_CONNECTION,
        DESTINATION_CONNECTION,
        STANDARD_SYNC,
        SOURCE_IMAGE_NAME,
        SOURCE_PROTOCOL_VERSION,
        DESTINATION_IMAGE_NAME,
        DESTINATION_PROTOCOL_VERSION,
        List.of(STANDARD_SYNC_OPERATION),
        null,
        new StandardSourceDefinition().withResourceRequirements(new ActorDefinitionResourceRequirements().withDefault(sourceResourceRequirements)),
        new StandardDestinationDefinition().withResourceRequirements(new ActorDefinitionResourceRequirements().withJobSpecific(List.of(
            new JobTypeResourceLimit().withJobType(JobType.SYNC).withResourceRequirements(originalReqs)))),
        SOURCE_DEFINITION_VERSION,
        DESTINATION_DEFINITION_VERSION,
        WORKSPACE_ID);

    final ArgumentCaptor<JobConfig> configCaptor = ArgumentCaptor.forClass(JobConfig.class);
    verify(jobPersistence, times(1)).enqueueJob(any(), configCaptor.capture());
    final var destConfigValues = configCaptor.getValue().getSync().getSyncResourceRequirements().getDestination();

    assertEquals(originalReqs.getCpuRequest(), destConfigValues.getCpuRequest());
    assertEquals(originalReqs.getCpuLimit(), destConfigValues.getCpuLimit());
    assertEquals(originalReqs.getMemoryRequest(), destConfigValues.getMemoryRequest());
    assertEquals(originalReqs.getMemoryLimit(), destConfigValues.getMemoryLimit());
  }

  private static Stream<Arguments> weirdnessOverrideMatrix() {
    return Stream.of(
        Arguments.of("0.7"),
        Arguments.of("0.5, 1, 1000Mi, 2000Mi"),
        Arguments.of("cat burglar"),
        Arguments.of("{ \"cpu_limit\": \"2\", \"cpu_request\": \"1\"  "),
        Arguments.of("null"),
        Arguments.of("undefined"),
        Arguments.of(""),
        Arguments.of("{}"));
  }

  @Test
  void testCreateResetConnectionJob() throws IOException {
    final Optional<String> expectedSourceType = Optional.empty();
    when(resourceRequirementsProvider.getResourceRequirements(ResourceRequirementsType.ORCHESTRATOR, expectedSourceType, DEFAULT_VARIANT))
        .thenReturn(workerResourceRequirements);
    when(resourceRequirementsProvider.getResourceRequirements(ResourceRequirementsType.SOURCE, expectedSourceType, DEFAULT_VARIANT))
        .thenReturn(sourceResourceRequirements);
    when(resourceRequirementsProvider.getResourceRequirements(ResourceRequirementsType.DESTINATION, expectedSourceType, DEFAULT_VARIANT))
        .thenReturn(destResourceRequirements);
    // More explicit resource requirements to verify data mapping
    final ResourceRequirements destStderrResourceRequirements = new ResourceRequirements().withCpuLimit("10");
    when(resourceRequirementsProvider.getResourceRequirements(ResourceRequirementsType.DESTINATION_STDERR, expectedSourceType, DEFAULT_VARIANT))
        .thenReturn(destStderrResourceRequirements);
    final ResourceRequirements destStdinResourceRequirements = new ResourceRequirements().withCpuLimit("11");
    when(resourceRequirementsProvider.getResourceRequirements(ResourceRequirementsType.DESTINATION_STDIN, expectedSourceType, DEFAULT_VARIANT))
        .thenReturn(destStdinResourceRequirements);
    final ResourceRequirements destStdoutResourceRequirements = new ResourceRequirements().withCpuLimit("12");
    when(resourceRequirementsProvider.getResourceRequirements(ResourceRequirementsType.DESTINATION_STDOUT, expectedSourceType, DEFAULT_VARIANT))
        .thenReturn(destStdoutResourceRequirements);
    final ResourceRequirements heartbeatResourceRequirements = new ResourceRequirements().withCpuLimit("13");
    when(resourceRequirementsProvider.getResourceRequirements(ResourceRequirementsType.HEARTBEAT, expectedSourceType, DEFAULT_VARIANT))
        .thenReturn(heartbeatResourceRequirements);
    final ResourceRequirements srcStderrResourceRequirements = new ResourceRequirements().withCpuLimit("14");
    when(resourceRequirementsProvider.getResourceRequirements(ResourceRequirementsType.SOURCE_STDERR, expectedSourceType, DEFAULT_VARIANT))
        .thenReturn(srcStderrResourceRequirements);
    final ResourceRequirements srcStdoutResourceRequirements = new ResourceRequirements().withCpuLimit("14");
    when(resourceRequirementsProvider.getResourceRequirements(ResourceRequirementsType.SOURCE_STDOUT, expectedSourceType, DEFAULT_VARIANT))
        .thenReturn(srcStdoutResourceRequirements);

    final List<StreamDescriptor> streamsToReset = List.of(STREAM1_DESCRIPTOR, STREAM2_DESCRIPTOR);
    final ConfiguredAirbyteCatalog expectedCatalog = new ConfiguredAirbyteCatalog().withStreams(List.of(
        new ConfiguredAirbyteStream()
            .withStream(CatalogHelpers.createAirbyteStream(STREAM1_NAME, Field.of(FIELD_NAME, JsonSchemaType.STRING)))
            .withSyncMode(SyncMode.FULL_REFRESH)
            .withDestinationSyncMode(DestinationSyncMode.OVERWRITE),
        new ConfiguredAirbyteStream()
            .withStream(CatalogHelpers.createAirbyteStream(STREAM2_NAME, NAMESPACE, Field.of(FIELD_NAME, JsonSchemaType.STRING)))
            .withSyncMode(SyncMode.FULL_REFRESH)
            .withDestinationSyncMode(DestinationSyncMode.OVERWRITE)));

    final SyncResourceRequirements expectedSyncResourceRequirements = new SyncResourceRequirements()
        .withConfigKey(new SyncResourceRequirementsKey().withVariant(DEFAULT_VARIANT))
        .withDestination(destResourceRequirements)
        .withDestinationStdErr(destStderrResourceRequirements)
        .withDestinationStdIn(destStdinResourceRequirements)
        .withDestinationStdOut(destStdoutResourceRequirements)
        .withOrchestrator(workerResourceRequirements)
        .withHeartbeat(heartbeatResourceRequirements);

    final JobResetConnectionConfig jobResetConnectionConfig = new JobResetConnectionConfig()
        .withNamespaceDefinition(STANDARD_SYNC.getNamespaceDefinition())
        .withNamespaceFormat(STANDARD_SYNC.getNamespaceFormat())
        .withPrefix(STANDARD_SYNC.getPrefix())
        .withDestinationDockerImage(DESTINATION_IMAGE_NAME)
        .withDestinationProtocolVersion(DESTINATION_PROTOCOL_VERSION)
        .withConfiguredAirbyteCatalog(expectedCatalog)
        .withOperationSequence(List.of(STANDARD_SYNC_OPERATION))
        .withResourceRequirements(workerResourceRequirements)
        .withSyncResourceRequirements(expectedSyncResourceRequirements)
        .withResetSourceConfiguration(new ResetSourceConfiguration().withStreamsToReset(streamsToReset))
        .withIsSourceCustomConnector(false)
        .withIsDestinationCustomConnector(false)
        .withWorkspaceId(DESTINATION_CONNECTION.getWorkspaceId())
        .withDestinationDefinitionVersionId(DESTINATION_DEFINITION_VERSION.getVersionId());

    final JobConfig jobConfig = new JobConfig()
        .withConfigType(ConfigType.RESET_CONNECTION)
        .withResetConnection(jobResetConnectionConfig);

    final String expectedScope = STANDARD_SYNC.getConnectionId().toString();
    when(jobPersistence.enqueueJob(expectedScope, jobConfig)).thenReturn(Optional.of(JOB_ID));

    final Optional<Long> jobId = jobCreator.createResetConnectionJob(
        DESTINATION_CONNECTION,
        STANDARD_SYNC,
        new StandardDestinationDefinition(),
        DESTINATION_DEFINITION_VERSION,
        DESTINATION_IMAGE_NAME,
        DESTINATION_PROTOCOL_VERSION,
        false,
        List.of(STANDARD_SYNC_OPERATION),
        streamsToReset,
        WORKSPACE_ID);

    verify(jobPersistence).enqueueJob(expectedScope, jobConfig);
    assertTrue(jobId.isPresent());
    assertEquals(JOB_ID, jobId.get());
  }

  @Test
  void testCreateResetConnectionJobEnsureNoQueuing() throws IOException {
    final List<StreamDescriptor> streamsToReset = List.of(STREAM1_DESCRIPTOR, STREAM2_DESCRIPTOR);
    final ConfiguredAirbyteCatalog expectedCatalog = new ConfiguredAirbyteCatalog().withStreams(List.of(
        new ConfiguredAirbyteStream()
            .withStream(CatalogHelpers.createAirbyteStream(STREAM1_NAME, Field.of(FIELD_NAME, JsonSchemaType.STRING)))
            .withSyncMode(SyncMode.FULL_REFRESH)
            .withDestinationSyncMode(DestinationSyncMode.OVERWRITE),
        new ConfiguredAirbyteStream()
            .withStream(CatalogHelpers.createAirbyteStream(STREAM2_NAME, NAMESPACE, Field.of(FIELD_NAME, JsonSchemaType.STRING)))
            .withSyncMode(SyncMode.FULL_REFRESH)
            .withDestinationSyncMode(DestinationSyncMode.OVERWRITE)));

    final SyncResourceRequirements expectedSyncResourceRequirements = new SyncResourceRequirements()
        .withConfigKey(new SyncResourceRequirementsKey().withVariant(DEFAULT_VARIANT))
        .withDestination(workerResourceRequirements)
        .withDestinationStdErr(workerResourceRequirements)
        .withDestinationStdIn(workerResourceRequirements)
        .withDestinationStdOut(workerResourceRequirements)
        .withOrchestrator(workerResourceRequirements)
        .withHeartbeat(workerResourceRequirements);

    final JobResetConnectionConfig jobResetConnectionConfig = new JobResetConnectionConfig()
        .withNamespaceDefinition(STANDARD_SYNC.getNamespaceDefinition())
        .withNamespaceFormat(STANDARD_SYNC.getNamespaceFormat())
        .withPrefix(STANDARD_SYNC.getPrefix())
        .withDestinationDockerImage(DESTINATION_IMAGE_NAME)
        .withDestinationProtocolVersion(DESTINATION_PROTOCOL_VERSION)
        .withConfiguredAirbyteCatalog(expectedCatalog)
        .withOperationSequence(List.of(STANDARD_SYNC_OPERATION))
        .withResourceRequirements(workerResourceRequirements)
        .withSyncResourceRequirements(expectedSyncResourceRequirements)
        .withResetSourceConfiguration(new ResetSourceConfiguration().withStreamsToReset(streamsToReset))
        .withIsSourceCustomConnector(false)
        .withIsDestinationCustomConnector(false)
        .withWorkspaceId(DESTINATION_CONNECTION.getWorkspaceId())
        .withDestinationDefinitionVersionId(DESTINATION_DEFINITION_VERSION.getVersionId());

    final JobConfig jobConfig = new JobConfig()
        .withConfigType(ConfigType.RESET_CONNECTION)
        .withResetConnection(jobResetConnectionConfig);

    final String expectedScope = STANDARD_SYNC.getConnectionId().toString();
    when(jobPersistence.enqueueJob(expectedScope, jobConfig)).thenReturn(Optional.empty());

    final Optional<Long> jobId = jobCreator.createResetConnectionJob(
        DESTINATION_CONNECTION,
        STANDARD_SYNC,
        new StandardDestinationDefinition(),
        DESTINATION_DEFINITION_VERSION,
        DESTINATION_IMAGE_NAME,
        DESTINATION_PROTOCOL_VERSION,
        false,
        List.of(STANDARD_SYNC_OPERATION),
        streamsToReset,
        WORKSPACE_ID);

    verify(jobPersistence).enqueueJob(expectedScope, jobConfig);
    assertTrue(jobId.isEmpty());
  }

}
