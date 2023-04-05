/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.google.common.collect.Lists;
import io.airbyte.api.client.invoker.generated.ApiException;
import io.airbyte.api.model.generated.CatalogDiff;
import io.airbyte.api.model.generated.CheckConnectionRead;
import io.airbyte.api.model.generated.ConnectionIdRequestBody;
import io.airbyte.api.model.generated.ConnectionRead;
import io.airbyte.api.model.generated.ConnectionReadList;
import io.airbyte.api.model.generated.ConnectionStatus;
import io.airbyte.api.model.generated.ConnectionStream;
import io.airbyte.api.model.generated.ConnectionStreamRequestBody;
import io.airbyte.api.model.generated.ConnectionUpdate;
import io.airbyte.api.model.generated.DestinationCoreConfig;
import io.airbyte.api.model.generated.DestinationDefinitionIdWithWorkspaceId;
import io.airbyte.api.model.generated.DestinationDefinitionSpecificationRead;
import io.airbyte.api.model.generated.DestinationIdRequestBody;
import io.airbyte.api.model.generated.DestinationUpdate;
import io.airbyte.api.model.generated.FailureOrigin;
import io.airbyte.api.model.generated.FailureReason;
import io.airbyte.api.model.generated.FailureType;
import io.airbyte.api.model.generated.FieldTransform;
import io.airbyte.api.model.generated.JobConfigType;
import io.airbyte.api.model.generated.JobIdRequestBody;
import io.airbyte.api.model.generated.JobInfoRead;
import io.airbyte.api.model.generated.LogRead;
import io.airbyte.api.model.generated.NonBreakingChangesPreference;
import io.airbyte.api.model.generated.SourceCoreConfig;
import io.airbyte.api.model.generated.SourceDefinitionIdWithWorkspaceId;
import io.airbyte.api.model.generated.SourceDefinitionSpecificationRead;
import io.airbyte.api.model.generated.SourceDiscoverSchemaRead;
import io.airbyte.api.model.generated.SourceDiscoverSchemaRequestBody;
import io.airbyte.api.model.generated.SourceIdRequestBody;
import io.airbyte.api.model.generated.SourceUpdate;
import io.airbyte.api.model.generated.StreamTransform;
import io.airbyte.api.model.generated.StreamTransform.TransformTypeEnum;
import io.airbyte.api.model.generated.SynchronousJobRead;
import io.airbyte.commons.enums.Enums;
import io.airbyte.commons.features.EnvVariableFeatureFlags;
import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.lang.Exceptions;
import io.airbyte.commons.server.converters.ConfigurationUpdate;
import io.airbyte.commons.server.converters.JobConverter;
import io.airbyte.commons.server.errors.ValueConflictKnownException;
import io.airbyte.commons.server.handlers.helpers.CatalogConverter;
import io.airbyte.commons.server.helpers.DestinationHelpers;
import io.airbyte.commons.server.helpers.SourceHelpers;
import io.airbyte.commons.server.scheduler.EventRunner;
import io.airbyte.commons.server.scheduler.SynchronousJobMetadata;
import io.airbyte.commons.server.scheduler.SynchronousResponse;
import io.airbyte.commons.server.scheduler.SynchronousSchedulerClient;
import io.airbyte.commons.temporal.ErrorCode;
import io.airbyte.commons.temporal.JobMetadata;
import io.airbyte.commons.temporal.TemporalClient.ManualOperationResult;
import io.airbyte.commons.version.Version;
import io.airbyte.config.ActorCatalog;
import io.airbyte.config.ActorDefinitionVersion;
import io.airbyte.config.Configs.WorkerEnvironment;
import io.airbyte.config.ConnectorJobOutput;
import io.airbyte.config.DestinationConnection;
import io.airbyte.config.JobConfig.ConfigType;
import io.airbyte.config.SourceConnection;
import io.airbyte.config.StandardCheckConnectionOutput;
import io.airbyte.config.StandardDestinationDefinition;
import io.airbyte.config.StandardSourceDefinition;
import io.airbyte.config.helpers.LogConfigs;
import io.airbyte.config.persistence.ActorDefinitionVersionHelper;
import io.airbyte.config.persistence.ConfigNotFoundException;
import io.airbyte.config.persistence.ConfigRepository;
import io.airbyte.config.persistence.SecretsRepositoryWriter;
import io.airbyte.persistence.job.JobPersistence;
import io.airbyte.persistence.job.WebUrlHelper;
import io.airbyte.persistence.job.models.Job;
import io.airbyte.persistence.job.models.JobStatus;
import io.airbyte.protocol.models.AirbyteCatalog;
import io.airbyte.protocol.models.CatalogHelpers;
import io.airbyte.protocol.models.ConnectorSpecification;
import io.airbyte.protocol.models.Field;
import io.airbyte.protocol.models.JsonSchemaType;
import io.airbyte.protocol.models.StreamDescriptor;
import io.airbyte.validation.json.JsonSchemaValidator;
import io.airbyte.validation.json.JsonValidationException;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;

@SuppressWarnings("unchecked")
class SchedulerHandlerTest {

  private static final String SOURCE_DOCKER_REPO = "srcimage";
  private static final String SOURCE_DOCKER_TAG = "tag";
  private static final String SOURCE_DOCKER_IMAGE = SOURCE_DOCKER_REPO + ":" + SOURCE_DOCKER_TAG;
  private static final String SOURCE_PROTOCOL_VERSION = "0.4.5";

  private static final String DESTINATION_DOCKER_REPO = "dstimage";
  private static final String DESTINATION_DOCKER_TAG = "tag";
  private static final String DESTINATION_DOCKER_IMAGE = DESTINATION_DOCKER_REPO + ":" + DESTINATION_DOCKER_TAG;
  private static final String DESTINATION_PROTOCOL_VERSION = "0.7.9";
  private static final String NAME = "name";
  private static final String DOGS = "dogs";
  private static final String SHOES = "shoes";
  private static final String SKU = "sku";
  private static final String CONNECTION_URL = "connection_url";

  private static final UUID JOB_ID = UUID.randomUUID();
  private static final long CREATED_AT = System.currentTimeMillis() / 1000;
  private static final boolean CONNECTOR_CONFIG_UPDATED = false;
  private static final Path LOG_PATH = Path.of("log_path");
  private static final Optional<UUID> CONFIG_ID = Optional.of(UUID.randomUUID());
  private static final String FAILURE_ORIGIN = "source";
  private static final String FAILURE_TYPE = "system_error";
  private static final boolean RETRYABLE = true;
  private static final String EXTERNAL_MESSAGE = "Source did something wrong";
  private static final String INTERNAL_MESSAGE = "Internal message related to system error";
  private static final String STACKTRACE = "Stacktrace";

  private static final AirbyteCatalog airbyteCatalog = CatalogHelpers.createAirbyteCatalog(SHOES,
      Field.of(SKU, JsonSchemaType.STRING));

  private static final SourceConnection SOURCE = new SourceConnection()
      .withName("my postgres db")
      .withWorkspaceId(UUID.randomUUID())
      .withSourceDefinitionId(UUID.randomUUID())
      .withSourceId(UUID.randomUUID())
      .withConfiguration(Jsons.emptyObject())
      .withTombstone(false);

  private static final DestinationConnection DESTINATION = new DestinationConnection()
      .withName("my db2 instance")
      .withWorkspaceId(UUID.randomUUID())
      .withDestinationDefinitionId(UUID.randomUUID())
      .withDestinationId(UUID.randomUUID())
      .withConfiguration(Jsons.emptyObject())
      .withTombstone(false);

  private static final ConnectorSpecification CONNECTOR_SPECIFICATION = new ConnectorSpecification()
      .withDocumentationUrl(Exceptions.toRuntime(() -> new URI("https://google.com")))
      .withChangelogUrl(Exceptions.toRuntime(() -> new URI("https://google.com")))
      .withConnectionSpecification(Jsons.jsonNode(new HashMap<>()));

  private static final ConnectorSpecification CONNECTOR_SPECIFICATION_WITHOUT_DOCS_URL = new ConnectorSpecification()
      .withChangelogUrl(Exceptions.toRuntime(() -> new URI("https://google.com")))
      .withConnectionSpecification(Jsons.jsonNode(new HashMap<>()));

  private static final StreamDescriptor STREAM_DESCRIPTOR = new StreamDescriptor().withName("1");

  private SchedulerHandler schedulerHandler;
  private ConfigRepository configRepository;
  private SecretsRepositoryWriter secretsRepositoryWriter;
  private Job completedJob;
  private SynchronousSchedulerClient synchronousSchedulerClient;
  private SynchronousResponse<?> jobResponse;
  private ConfigurationUpdate configurationUpdate;
  private JsonSchemaValidator jsonSchemaValidator;
  private JobPersistence jobPersistence;
  private EventRunner eventRunner;
  private JobConverter jobConverter;
  private ConnectionsHandler connectionsHandler;
  private EnvVariableFeatureFlags envVariableFeatureFlags;
  private WebUrlHelper webUrlHelper;
  private ActorDefinitionVersionHelper actorDefinitionVersionHelper;

  @BeforeEach
  void setup() {
    completedJob = mock(Job.class, RETURNS_DEEP_STUBS);
    jobResponse = mock(SynchronousResponse.class, RETURNS_DEEP_STUBS);
    final SynchronousJobMetadata synchronousJobMetadata = mock(SynchronousJobMetadata.class);
    when(synchronousJobMetadata.getConfigType())
        .thenReturn(ConfigType.SYNC);
    when(jobResponse.getMetadata())
        .thenReturn(synchronousJobMetadata);
    configurationUpdate = mock(ConfigurationUpdate.class);
    jsonSchemaValidator = mock(JsonSchemaValidator.class);
    when(completedJob.getStatus()).thenReturn(JobStatus.SUCCEEDED);
    when(completedJob.getConfig().getConfigType()).thenReturn(ConfigType.SYNC);
    when(completedJob.getScope()).thenReturn("sync:123");

    synchronousSchedulerClient = mock(SynchronousSchedulerClient.class);
    configRepository = mock(ConfigRepository.class);
    secretsRepositoryWriter = mock(SecretsRepositoryWriter.class);
    jobPersistence = mock(JobPersistence.class);
    eventRunner = mock(EventRunner.class);
    connectionsHandler = mock(ConnectionsHandler.class);
    envVariableFeatureFlags = mock(EnvVariableFeatureFlags.class);
    webUrlHelper = mock(WebUrlHelper.class);
    actorDefinitionVersionHelper = mock(ActorDefinitionVersionHelper.class);

    jobConverter = spy(new JobConverter(WorkerEnvironment.DOCKER, LogConfigs.EMPTY));

    schedulerHandler = new SchedulerHandler(
        configRepository,
        secretsRepositoryWriter,
        synchronousSchedulerClient,
        configurationUpdate,
        jsonSchemaValidator,
        jobPersistence,
        eventRunner,
        jobConverter,
        connectionsHandler,
        envVariableFeatureFlags,
        webUrlHelper,
        actorDefinitionVersionHelper);
  }

  @Test
  void testCheckSourceConnectionFromSourceId() throws JsonValidationException, IOException, ConfigNotFoundException {
    final SourceConnection source = SourceHelpers.generateSource(UUID.randomUUID());
    final SourceIdRequestBody request = new SourceIdRequestBody().sourceId(source.getSourceId());
    final Version protocolVersion = new Version(SOURCE_PROTOCOL_VERSION);

    final StandardSourceDefinition sourceDefinition = new StandardSourceDefinition()
        .withProtocolVersion(SOURCE_PROTOCOL_VERSION)
        .withSourceDefinitionId(source.getSourceDefinitionId());
    when(configRepository.getStandardSourceDefinition(source.getSourceDefinitionId()))
        .thenReturn(sourceDefinition);
    when(actorDefinitionVersionHelper.getSourceVersion(sourceDefinition, source.getWorkspaceId(), source.getSourceId()))
        .thenReturn(new ActorDefinitionVersion()
            .withDockerRepository(SOURCE_DOCKER_REPO)
            .withDockerImageTag(SOURCE_DOCKER_TAG));
    when(configRepository.getSourceConnection(source.getSourceId())).thenReturn(source);
    when(synchronousSchedulerClient.createSourceCheckConnectionJob(source, SOURCE_DOCKER_IMAGE, protocolVersion, false))
        .thenReturn((SynchronousResponse<StandardCheckConnectionOutput>) jobResponse);

    schedulerHandler.checkSourceConnectionFromSourceId(request);

    verify(configRepository).getSourceConnection(source.getSourceId());
    verify(synchronousSchedulerClient).createSourceCheckConnectionJob(source, SOURCE_DOCKER_IMAGE, protocolVersion, false);
    verify(actorDefinitionVersionHelper).getSourceVersion(sourceDefinition, source.getWorkspaceId(), source.getSourceId());
  }

  @Test
  void testCheckSourceConnectionFromSourceCreate() throws JsonValidationException, IOException, ConfigNotFoundException {
    final SourceConnection source = new SourceConnection()
        .withWorkspaceId(SOURCE.getWorkspaceId())
        .withSourceDefinitionId(SOURCE.getSourceDefinitionId())
        .withConfiguration(SOURCE.getConfiguration());

    final SourceCoreConfig sourceCoreConfig = new SourceCoreConfig()
        .sourceDefinitionId(source.getSourceDefinitionId())
        .connectionConfiguration(source.getConfiguration())
        .workspaceId(source.getWorkspaceId());

    final Version protocolVersion = new Version(SOURCE_PROTOCOL_VERSION);

    final StandardSourceDefinition sourceDefinition = new StandardSourceDefinition()
        .withProtocolVersion(SOURCE_PROTOCOL_VERSION)
        .withSourceDefinitionId(source.getSourceDefinitionId());
    when(configRepository.getStandardSourceDefinition(source.getSourceDefinitionId()))
        .thenReturn(sourceDefinition);
    when(actorDefinitionVersionHelper.getSourceVersion(sourceDefinition, source.getWorkspaceId(), null)).thenReturn(new ActorDefinitionVersion()
        .withDockerRepository(SOURCE_DOCKER_REPO)
        .withDockerImageTag(SOURCE_DOCKER_TAG));
    when(secretsRepositoryWriter.statefulSplitEphemeralSecrets(
        eq(source.getConfiguration()),
        any())).thenReturn(source.getConfiguration());
    when(synchronousSchedulerClient.createSourceCheckConnectionJob(source, SOURCE_DOCKER_IMAGE, protocolVersion, false))
        .thenReturn((SynchronousResponse<StandardCheckConnectionOutput>) jobResponse);

    schedulerHandler.checkSourceConnectionFromSourceCreate(sourceCoreConfig);

    verify(synchronousSchedulerClient).createSourceCheckConnectionJob(source, SOURCE_DOCKER_IMAGE, protocolVersion, false);
    verify(actorDefinitionVersionHelper).getSourceVersion(sourceDefinition, source.getWorkspaceId(), null);
  }

  @Test
  void testCheckSourceConnectionFromUpdate() throws IOException, JsonValidationException, ConfigNotFoundException {
    final SourceConnection source = SourceHelpers.generateSource(UUID.randomUUID());
    final SourceUpdate sourceUpdate = new SourceUpdate()
        .name(source.getName())
        .sourceId(source.getSourceId())
        .connectionConfiguration(source.getConfiguration());
    final StandardSourceDefinition sourceDefinition = new StandardSourceDefinition()
        .withProtocolVersion(DESTINATION_PROTOCOL_VERSION)
        .withSourceDefinitionId(source.getSourceDefinitionId());
    final Version protocolVersion = new Version(DESTINATION_PROTOCOL_VERSION);
    when(configRepository.getStandardSourceDefinition(source.getSourceDefinitionId()))
        .thenReturn(sourceDefinition);
    when(actorDefinitionVersionHelper.getSourceVersion(sourceDefinition, source.getWorkspaceId(), source.getSourceId()))
        .thenReturn(new ActorDefinitionVersion()
            .withDockerRepository(DESTINATION_DOCKER_REPO)
            .withDockerImageTag(DESTINATION_DOCKER_TAG)
            .withSpec(CONNECTOR_SPECIFICATION));
    when(configRepository.getSourceConnection(source.getSourceId())).thenReturn(source);
    when(configurationUpdate.source(source.getSourceId(), source.getName(), sourceUpdate.getConnectionConfiguration())).thenReturn(source);
    final SourceConnection submittedSource = new SourceConnection()
        .withSourceId(source.getSourceId())
        .withSourceDefinitionId(source.getSourceDefinitionId())
        .withConfiguration(source.getConfiguration())
        .withWorkspaceId(source.getWorkspaceId());
    when(synchronousSchedulerClient.createSourceCheckConnectionJob(submittedSource, DESTINATION_DOCKER_IMAGE, protocolVersion, false))
        .thenReturn((SynchronousResponse<StandardCheckConnectionOutput>) jobResponse);
    when(secretsRepositoryWriter.statefulSplitEphemeralSecrets(
        eq(source.getConfiguration()),
        any())).thenReturn(source.getConfiguration());
    schedulerHandler.checkSourceConnectionFromSourceIdForUpdate(sourceUpdate);

    verify(jsonSchemaValidator).ensure(CONNECTOR_SPECIFICATION.getConnectionSpecification(), source.getConfiguration());
    verify(synchronousSchedulerClient).createSourceCheckConnectionJob(submittedSource, DESTINATION_DOCKER_IMAGE, protocolVersion, false);
    verify(actorDefinitionVersionHelper, times(2)).getSourceVersion(sourceDefinition, source.getWorkspaceId(), source.getSourceId());
  }

  @Test
  void testGetSourceSpec() throws JsonValidationException, IOException, ConfigNotFoundException {
    final SourceDefinitionIdWithWorkspaceId sourceDefinitionIdWithWorkspaceId =
        new SourceDefinitionIdWithWorkspaceId().sourceDefinitionId(UUID.randomUUID()).workspaceId(UUID.randomUUID());

    final StandardSourceDefinition sourceDefinition = new StandardSourceDefinition()
        .withName(NAME)
        .withDockerRepository(SOURCE_DOCKER_REPO)
        .withSourceDefinitionId(sourceDefinitionIdWithWorkspaceId.getSourceDefinitionId());
    when(configRepository.getStandardSourceDefinition(sourceDefinitionIdWithWorkspaceId.getSourceDefinitionId()))
        .thenReturn(sourceDefinition);
    when(actorDefinitionVersionHelper.getSourceVersion(sourceDefinition, sourceDefinitionIdWithWorkspaceId.getWorkspaceId()))
        .thenReturn(new ActorDefinitionVersion()
            .withDockerImageTag(SOURCE_DOCKER_TAG)
            .withSpec(CONNECTOR_SPECIFICATION));

    final SourceDefinitionSpecificationRead response = schedulerHandler.getSourceDefinitionSpecification(sourceDefinitionIdWithWorkspaceId);

    verify(configRepository).getStandardSourceDefinition(sourceDefinitionIdWithWorkspaceId.getSourceDefinitionId());
    verify(actorDefinitionVersionHelper).getSourceVersion(sourceDefinition, sourceDefinitionIdWithWorkspaceId.getWorkspaceId());
    assertEquals(CONNECTOR_SPECIFICATION.getConnectionSpecification(), response.getConnectionSpecification());
  }

  @Test
  void testGetSourceSpecForSourceId() throws JsonValidationException, IOException, ConfigNotFoundException {
    final UUID sourceId = UUID.randomUUID();
    final UUID workspaceId = UUID.randomUUID();
    final UUID sourceDefinitionId = UUID.randomUUID();

    final SourceIdRequestBody sourceIdRequestBody =
        new SourceIdRequestBody()
            .sourceId(sourceId);

    final StandardSourceDefinition sourceDefinition = new StandardSourceDefinition()
        .withName(NAME)
        .withDockerRepository(SOURCE_DOCKER_REPO)
        .withSourceDefinitionId(sourceDefinitionId);
    when(configRepository.getSourceConnection(sourceId)).thenReturn(
        new SourceConnection()
            .withSourceId(sourceId)
            .withWorkspaceId(workspaceId)
            .withSourceDefinitionId(sourceDefinitionId));
    when(configRepository.getStandardSourceDefinition(sourceDefinitionId))
        .thenReturn(sourceDefinition);
    when(actorDefinitionVersionHelper.getSourceVersion(sourceDefinition, workspaceId, sourceId))
        .thenReturn(new ActorDefinitionVersion()
            .withDockerImageTag(SOURCE_DOCKER_TAG)
            .withSpec(CONNECTOR_SPECIFICATION));

    final SourceDefinitionSpecificationRead response = schedulerHandler.getSpecificationForSourceId(sourceIdRequestBody);

    verify(actorDefinitionVersionHelper).getSourceVersion(sourceDefinition, workspaceId, sourceId);
    assertEquals(CONNECTOR_SPECIFICATION.getConnectionSpecification(), response.getConnectionSpecification());
  }

  @Test
  void testGetSourceSpecWithoutDocs() throws JsonValidationException, IOException, ConfigNotFoundException {
    final SourceDefinitionIdWithWorkspaceId sourceDefinitionIdWithWorkspaceId =
        new SourceDefinitionIdWithWorkspaceId().sourceDefinitionId(UUID.randomUUID()).workspaceId(UUID.randomUUID());

    final StandardSourceDefinition sourceDefinition = new StandardSourceDefinition()
        .withName(NAME)
        .withDockerRepository(SOURCE_DOCKER_REPO)
        .withSourceDefinitionId(sourceDefinitionIdWithWorkspaceId.getSourceDefinitionId());
    when(configRepository.getStandardSourceDefinition(sourceDefinitionIdWithWorkspaceId.getSourceDefinitionId()))
        .thenReturn(sourceDefinition);
    when(actorDefinitionVersionHelper.getSourceVersion(sourceDefinition, sourceDefinitionIdWithWorkspaceId.getWorkspaceId()))
        .thenReturn(new ActorDefinitionVersion()
            .withDockerImageTag(SOURCE_DOCKER_TAG)
            .withSpec(CONNECTOR_SPECIFICATION_WITHOUT_DOCS_URL));

    final SourceDefinitionSpecificationRead response = schedulerHandler.getSourceDefinitionSpecification(sourceDefinitionIdWithWorkspaceId);

    verify(configRepository).getStandardSourceDefinition(sourceDefinitionIdWithWorkspaceId.getSourceDefinitionId());
    verify(actorDefinitionVersionHelper).getSourceVersion(sourceDefinition, sourceDefinitionIdWithWorkspaceId.getWorkspaceId());
    assertEquals(CONNECTOR_SPECIFICATION_WITHOUT_DOCS_URL.getConnectionSpecification(), response.getConnectionSpecification());
  }

  @Test
  void testGetDestinationSpec() throws JsonValidationException, IOException, ConfigNotFoundException {
    final DestinationDefinitionIdWithWorkspaceId destinationDefinitionIdWithWorkspaceId =
        new DestinationDefinitionIdWithWorkspaceId().destinationDefinitionId(UUID.randomUUID()).workspaceId(UUID.randomUUID());

    final StandardDestinationDefinition destinationDefinition = new StandardDestinationDefinition()
        .withName(NAME)
        .withDockerRepository(DESTINATION_DOCKER_REPO)
        .withDestinationDefinitionId(destinationDefinitionIdWithWorkspaceId.getDestinationDefinitionId());
    when(configRepository.getStandardDestinationDefinition(destinationDefinitionIdWithWorkspaceId.getDestinationDefinitionId()))
        .thenReturn(destinationDefinition);
    when(actorDefinitionVersionHelper.getDestinationVersion(destinationDefinition,
        destinationDefinitionIdWithWorkspaceId.getWorkspaceId()))
            .thenReturn(new ActorDefinitionVersion()
                .withDockerImageTag(DESTINATION_DOCKER_TAG)
                .withSpec(CONNECTOR_SPECIFICATION));

    final DestinationDefinitionSpecificationRead response = schedulerHandler.getDestinationSpecification(destinationDefinitionIdWithWorkspaceId);

    verify(configRepository).getStandardDestinationDefinition(destinationDefinitionIdWithWorkspaceId.getDestinationDefinitionId());
    verify(actorDefinitionVersionHelper).getDestinationVersion(destinationDefinition,
        destinationDefinitionIdWithWorkspaceId.getWorkspaceId());
    assertEquals(CONNECTOR_SPECIFICATION.getConnectionSpecification(), response.getConnectionSpecification());
  }

  @Test
  void testGetDestinationSpecForDestinationId() throws JsonValidationException, IOException, ConfigNotFoundException {
    final UUID destinationId = UUID.randomUUID();
    final UUID workspaceId = UUID.randomUUID();
    final UUID destinationDefinitionId = UUID.randomUUID();
    final DestinationIdRequestBody destinationIdRequestBody =
        new DestinationIdRequestBody()
            .destinationId(destinationId);

    final StandardDestinationDefinition destinationDefinition = new StandardDestinationDefinition()
        .withName(NAME)
        .withDockerRepository(DESTINATION_DOCKER_REPO)
        .withDestinationDefinitionId(destinationDefinitionId);
    when(configRepository.getDestinationConnection(destinationId)).thenReturn(
        new DestinationConnection()
            .withDestinationId(destinationId)
            .withWorkspaceId(workspaceId)
            .withDestinationDefinitionId(destinationDefinitionId));
    when(configRepository.getStandardDestinationDefinition(destinationDefinitionId))
        .thenReturn(destinationDefinition);
    when(actorDefinitionVersionHelper.getDestinationVersion(destinationDefinition, workspaceId, destinationId))
        .thenReturn(new ActorDefinitionVersion()
            .withDockerImageTag(DESTINATION_DOCKER_TAG)
            .withSpec(CONNECTOR_SPECIFICATION));

    final DestinationDefinitionSpecificationRead response = schedulerHandler.getSpecificationForDestinationId(destinationIdRequestBody);

    verify(actorDefinitionVersionHelper).getDestinationVersion(destinationDefinition, workspaceId, destinationId);
    assertEquals(CONNECTOR_SPECIFICATION.getConnectionSpecification(), response.getConnectionSpecification());
  }

  @Test
  void testCheckDestinationConnectionFromDestinationId() throws IOException, JsonValidationException, ConfigNotFoundException {
    final DestinationConnection destination = DestinationHelpers.generateDestination(UUID.randomUUID());
    final DestinationIdRequestBody request = new DestinationIdRequestBody().destinationId(destination.getDestinationId());

    final StandardDestinationDefinition destinationDefinition = new StandardDestinationDefinition()
        .withProtocolVersion(DESTINATION_PROTOCOL_VERSION)
        .withDestinationDefinitionId(destination.getDestinationDefinitionId());
    when(configRepository.getStandardDestinationDefinition(destination.getDestinationDefinitionId()))
        .thenReturn(destinationDefinition);
    when(actorDefinitionVersionHelper.getDestinationVersion(destinationDefinition, destination.getWorkspaceId(), destination.getDestinationId()))
        .thenReturn(new ActorDefinitionVersion()
            .withDockerRepository(DESTINATION_DOCKER_REPO)
            .withDockerImageTag(DESTINATION_DOCKER_TAG));
    when(configRepository.getDestinationConnection(destination.getDestinationId())).thenReturn(destination);
    when(synchronousSchedulerClient.createDestinationCheckConnectionJob(destination, DESTINATION_DOCKER_IMAGE,
        new Version(DESTINATION_PROTOCOL_VERSION), false))
            .thenReturn((SynchronousResponse<StandardCheckConnectionOutput>) jobResponse);

    schedulerHandler.checkDestinationConnectionFromDestinationId(request);

    verify(configRepository).getDestinationConnection(destination.getDestinationId());
    verify(actorDefinitionVersionHelper).getDestinationVersion(destinationDefinition, destination.getWorkspaceId(), destination.getDestinationId());
    verify(synchronousSchedulerClient).createDestinationCheckConnectionJob(destination, DESTINATION_DOCKER_IMAGE,
        new Version(DESTINATION_PROTOCOL_VERSION), false);
  }

  @Test
  void testCheckDestinationConnectionFromDestinationCreate() throws JsonValidationException, IOException, ConfigNotFoundException {
    final DestinationConnection destination = new DestinationConnection()
        .withWorkspaceId(DESTINATION.getWorkspaceId())
        .withDestinationDefinitionId(DESTINATION.getDestinationDefinitionId())
        .withConfiguration(DESTINATION.getConfiguration());

    final DestinationCoreConfig destinationCoreConfig = new DestinationCoreConfig()
        .workspaceId(destination.getWorkspaceId())
        .destinationDefinitionId(destination.getDestinationDefinitionId())
        .connectionConfiguration(destination.getConfiguration());

    final StandardDestinationDefinition destinationDefinition = new StandardDestinationDefinition()
        .withProtocolVersion(DESTINATION_PROTOCOL_VERSION)
        .withDestinationDefinitionId(destination.getDestinationDefinitionId());
    when(configRepository.getStandardDestinationDefinition(destination.getDestinationDefinitionId()))
        .thenReturn(destinationDefinition);
    when(actorDefinitionVersionHelper.getDestinationVersion(destinationDefinition, destination.getWorkspaceId(), null))
        .thenReturn(new ActorDefinitionVersion()
            .withDockerRepository(DESTINATION_DOCKER_REPO)
            .withDockerImageTag(DESTINATION_DOCKER_TAG));

    when(synchronousSchedulerClient.createDestinationCheckConnectionJob(destination, DESTINATION_DOCKER_IMAGE,
        new Version(DESTINATION_PROTOCOL_VERSION), false))
            .thenReturn((SynchronousResponse<StandardCheckConnectionOutput>) jobResponse);
    when(secretsRepositoryWriter.statefulSplitEphemeralSecrets(
        eq(destination.getConfiguration()),
        any())).thenReturn(destination.getConfiguration());
    schedulerHandler.checkDestinationConnectionFromDestinationCreate(destinationCoreConfig);

    verify(synchronousSchedulerClient).createDestinationCheckConnectionJob(destination, DESTINATION_DOCKER_IMAGE,
        new Version(DESTINATION_PROTOCOL_VERSION), false);
    verify(actorDefinitionVersionHelper).getDestinationVersion(destinationDefinition, destination.getWorkspaceId(), null);
  }

  @Test
  void testCheckDestinationConnectionFromUpdate() throws IOException, JsonValidationException, ConfigNotFoundException {
    final DestinationConnection destination = DestinationHelpers.generateDestination(UUID.randomUUID());
    final DestinationUpdate destinationUpdate = new DestinationUpdate()
        .name(destination.getName())
        .destinationId(destination.getDestinationId())
        .connectionConfiguration(destination.getConfiguration());
    final StandardDestinationDefinition destinationDefinition = new StandardDestinationDefinition()
        .withProtocolVersion(DESTINATION_PROTOCOL_VERSION)
        .withDestinationDefinitionId(destination.getDestinationDefinitionId());
    when(configRepository.getStandardDestinationDefinition(destination.getDestinationDefinitionId()))
        .thenReturn(destinationDefinition);
    when(actorDefinitionVersionHelper.getDestinationVersion(destinationDefinition, destination.getWorkspaceId(), destination.getDestinationId()))
        .thenReturn(new ActorDefinitionVersion()
            .withDockerRepository(DESTINATION_DOCKER_REPO)
            .withDockerImageTag(DESTINATION_DOCKER_TAG)
            .withSpec(CONNECTOR_SPECIFICATION));
    when(configRepository.getDestinationConnection(destination.getDestinationId())).thenReturn(destination);
    when(synchronousSchedulerClient.createDestinationCheckConnectionJob(destination, DESTINATION_DOCKER_IMAGE,
        new Version(DESTINATION_PROTOCOL_VERSION), false))
            .thenReturn((SynchronousResponse<StandardCheckConnectionOutput>) jobResponse);
    when(configRepository.getDestinationConnection(destination.getDestinationId())).thenReturn(destination);
    when(configurationUpdate.destination(destination.getDestinationId(), destination.getName(), destinationUpdate.getConnectionConfiguration()))
        .thenReturn(destination);
    final DestinationConnection submittedDestination = new DestinationConnection()
        .withDestinationId(destination.getDestinationId())
        .withDestinationDefinitionId(destination.getDestinationDefinitionId())
        .withConfiguration(destination.getConfiguration())
        .withWorkspaceId(destination.getWorkspaceId());
    when(synchronousSchedulerClient.createDestinationCheckConnectionJob(submittedDestination, DESTINATION_DOCKER_IMAGE,
        new Version(DESTINATION_PROTOCOL_VERSION), false))
            .thenReturn((SynchronousResponse<StandardCheckConnectionOutput>) jobResponse);
    when(secretsRepositoryWriter.statefulSplitEphemeralSecrets(
        eq(destination.getConfiguration()),
        any())).thenReturn(destination.getConfiguration());
    schedulerHandler.checkDestinationConnectionFromDestinationIdForUpdate(destinationUpdate);

    verify(jsonSchemaValidator).ensure(CONNECTOR_SPECIFICATION.getConnectionSpecification(), destination.getConfiguration());
    verify(actorDefinitionVersionHelper, times(2)).getDestinationVersion(destinationDefinition, destination.getWorkspaceId(),
        destination.getDestinationId());
    verify(synchronousSchedulerClient).createDestinationCheckConnectionJob(submittedDestination, DESTINATION_DOCKER_IMAGE,
        new Version(DESTINATION_PROTOCOL_VERSION), false);
  }

  private static SynchronousJobRead baseSynchronousJobRead() {
    // return a new base every time this method is called so that we are not updating
    // fields on the existing one in subsequent tests
    return new SynchronousJobRead()
        .id(JOB_ID)
        .configId(String.valueOf(CONFIG_ID))
        .configType(JobConfigType.CHECK_CONNECTION_SOURCE)
        .createdAt(CREATED_AT)
        .endedAt(CREATED_AT)
        .connectorConfigurationUpdated(CONNECTOR_CONFIG_UPDATED)
        .logs(new LogRead().logLines(new ArrayList<>()));
  }

  private static final FailureReason mockFailureReasonFromTrace = new FailureReason()
      .failureOrigin(FailureOrigin.fromValue(FAILURE_ORIGIN))
      .failureType(FailureType.fromValue(FAILURE_TYPE))
      .retryable(RETRYABLE)
      .externalMessage(EXTERNAL_MESSAGE)
      .internalMessage(INTERNAL_MESSAGE)
      .stacktrace(STACKTRACE)
      .timestamp(CREATED_AT);

  private static final CheckConnectionRead jobSuccessStatusSuccess = new CheckConnectionRead()
      .jobInfo(baseSynchronousJobRead().succeeded(true))
      .status(CheckConnectionRead.StatusEnum.SUCCEEDED);

  private static final CheckConnectionRead jobSuccessStatusFailed = new CheckConnectionRead()
      .jobInfo(baseSynchronousJobRead().succeeded(true))
      .status(CheckConnectionRead.StatusEnum.FAILED)
      .message("Something went wrong - check connection failure");

  private static final CheckConnectionRead jobFailedWithFailureReason = new CheckConnectionRead()
      .jobInfo(baseSynchronousJobRead().succeeded(false).failureReason(mockFailureReasonFromTrace));

  private static final CheckConnectionRead jobFailedWithoutFailureReason = new CheckConnectionRead()
      .jobInfo(baseSynchronousJobRead().succeeded(false));

  private static Stream<Arguments> provideArguments() {
    return Stream.of(
        Arguments.of(Optional.of("succeeded"), false, jobSuccessStatusSuccess),
        Arguments.of(Optional.of("succeeded"), true, jobFailedWithFailureReason),
        Arguments.of(Optional.of("failed"), false, jobSuccessStatusFailed),
        Arguments.of(Optional.of("failed"), true, jobFailedWithFailureReason),
        Arguments.of(Optional.empty(), false, jobFailedWithoutFailureReason),
        Arguments.of(Optional.empty(), true, jobFailedWithFailureReason));
  }

  @ParameterizedTest
  @MethodSource("provideArguments")
  void testCheckConnectionReadFormat(final Optional<String> standardCheckConnectionOutputStatusEmittedBySource,
                                     final boolean traceMessageEmittedBySource,
                                     final CheckConnectionRead expectedCheckConnectionRead)
      throws JsonValidationException, IOException, ConfigNotFoundException {

    final io.airbyte.config.FailureReason failureReason = new io.airbyte.config.FailureReason()
        .withFailureOrigin(io.airbyte.config.FailureReason.FailureOrigin.fromValue(FAILURE_ORIGIN))
        .withFailureType(io.airbyte.config.FailureReason.FailureType.fromValue(FAILURE_TYPE))
        .withRetryable(RETRYABLE)
        .withExternalMessage(EXTERNAL_MESSAGE)
        .withInternalMessage(INTERNAL_MESSAGE)
        .withStacktrace(STACKTRACE)
        .withTimestamp(CREATED_AT);

    final StandardCheckConnectionOutput checkConnectionOutput;
    if (standardCheckConnectionOutputStatusEmittedBySource.isPresent()) {
      final StandardCheckConnectionOutput.Status status =
          StandardCheckConnectionOutput.Status.fromValue(standardCheckConnectionOutputStatusEmittedBySource.get());
      final String message = (status == StandardCheckConnectionOutput.Status.FAILED) ? "Something went wrong - check connection failure" : null;
      checkConnectionOutput = new StandardCheckConnectionOutput().withStatus(status).withMessage(message);
    } else {
      checkConnectionOutput = null;
    }

    // This replicates the behavior of the DefaultCheckConnectionWorker. It always adds both the
    // failureReason and the checkConnection if they are available.
    boolean exceptionWouldBeThrown = false;
    final ConnectorJobOutput connectorJobOutput = new ConnectorJobOutput().withOutputType(ConnectorJobOutput.OutputType.CHECK_CONNECTION);
    if (standardCheckConnectionOutputStatusEmittedBySource.isPresent()) {
      connectorJobOutput.setCheckConnection(checkConnectionOutput);
    }
    if (traceMessageEmittedBySource) {
      connectorJobOutput.setFailureReason(failureReason);
    }
    if (!standardCheckConnectionOutputStatusEmittedBySource.isPresent() && !traceMessageEmittedBySource) {
      exceptionWouldBeThrown = true;
    }

    // This replicates the behavior of the TemporalClient. If there is a failureReason,
    // it declares the job a failure.
    final boolean jobSucceeded = !exceptionWouldBeThrown && connectorJobOutput.getFailureReason() == null;
    final JobMetadata jobMetadata = new JobMetadata(jobSucceeded, LOG_PATH);
    final SynchronousJobMetadata synchronousJobMetadata = SynchronousJobMetadata.fromJobMetadata(jobMetadata, connectorJobOutput, JOB_ID,
        ConfigType.CHECK_CONNECTION_SOURCE, CONFIG_ID.get(), CREATED_AT, CREATED_AT);

    final SynchronousResponse<StandardCheckConnectionOutput> checkResponse = new SynchronousResponse<>(checkConnectionOutput, synchronousJobMetadata);

    // Below is just to mock checkSourceConnectionFromSourceCreate as a public interface that uses
    // reportConnectionStatus
    final SourceConnection source = new SourceConnection()
        .withWorkspaceId(SOURCE.getWorkspaceId())
        .withSourceDefinitionId(SOURCE.getSourceDefinitionId())
        .withConfiguration(SOURCE.getConfiguration());

    final SourceCoreConfig sourceCoreConfig = new SourceCoreConfig()
        .sourceDefinitionId(source.getSourceDefinitionId())
        .connectionConfiguration(source.getConfiguration())
        .workspaceId(source.getWorkspaceId());

    final Version protocolVersion = new Version(SOURCE_PROTOCOL_VERSION);

    final StandardSourceDefinition sourceDefinition = new StandardSourceDefinition()
        .withProtocolVersion(SOURCE_PROTOCOL_VERSION)
        .withSourceDefinitionId(source.getSourceDefinitionId());
    when(configRepository.getStandardSourceDefinition(source.getSourceDefinitionId()))
        .thenReturn(sourceDefinition);
    when(actorDefinitionVersionHelper.getSourceVersion(sourceDefinition, source.getWorkspaceId(), source.getSourceId()))
        .thenReturn(new ActorDefinitionVersion()
            .withDockerRepository(SOURCE_DOCKER_REPO)
            .withDockerImageTag(SOURCE_DOCKER_TAG));
    when(secretsRepositoryWriter.statefulSplitEphemeralSecrets(
        eq(source.getConfiguration()),
        any())).thenReturn(source.getConfiguration());
    when(synchronousSchedulerClient.createSourceCheckConnectionJob(source, SOURCE_DOCKER_IMAGE, protocolVersion, false))
        .thenReturn(checkResponse);

    final CheckConnectionRead checkConnectionRead = schedulerHandler.checkSourceConnectionFromSourceCreate(sourceCoreConfig);
    assertEquals(expectedCheckConnectionRead, checkConnectionRead);

  }

  @Test
  void testDiscoverSchemaForSourceFromSourceId() throws IOException, JsonValidationException, ConfigNotFoundException {
    final SourceConnection source = SourceHelpers.generateSource(UUID.randomUUID());
    final SourceDiscoverSchemaRequestBody request = new SourceDiscoverSchemaRequestBody().sourceId(source.getSourceId());

    final SynchronousResponse<UUID> discoverResponse = (SynchronousResponse<UUID>) jobResponse;
    final SynchronousJobMetadata metadata = mock(SynchronousJobMetadata.class);
    when(discoverResponse.isSuccess()).thenReturn(true);
    when(discoverResponse.getOutput()).thenReturn(UUID.randomUUID());
    final ActorCatalog actorCatalog = new ActorCatalog()
        .withCatalog(Jsons.jsonNode(airbyteCatalog))
        .withCatalogHash("")
        .withId(UUID.randomUUID());
    when(configRepository.getActorCatalogById(any())).thenReturn(actorCatalog);
    when(discoverResponse.getMetadata()).thenReturn(metadata);
    when(metadata.isSucceeded()).thenReturn(true);

    final ConnectionRead connectionRead = new ConnectionRead();
    final ConnectionReadList connectionReadList = new ConnectionReadList().connections(List.of(connectionRead));
    when(connectionsHandler.listConnectionsForSource(source.getSourceId(), false)).thenReturn(connectionReadList);

    final StandardSourceDefinition sourceDefinition = new StandardSourceDefinition()
        .withProtocolVersion(SOURCE_PROTOCOL_VERSION)
        .withSourceDefinitionId(source.getSourceDefinitionId());
    when(configRepository.getStandardSourceDefinition(source.getSourceDefinitionId()))
        .thenReturn(sourceDefinition);
    when(actorDefinitionVersionHelper.getSourceVersion(sourceDefinition, source.getWorkspaceId(), source.getSourceId()))
        .thenReturn(new ActorDefinitionVersion()
            .withDockerRepository(SOURCE_DOCKER_REPO)
            .withDockerImageTag(SOURCE_DOCKER_TAG));
    when(configRepository.getSourceConnection(source.getSourceId())).thenReturn(source);
    when(configRepository.getActorCatalog(any(), any(), any())).thenReturn(Optional.empty());
    when(synchronousSchedulerClient.createDiscoverSchemaJob(source, SOURCE_DOCKER_IMAGE, SOURCE_DOCKER_TAG, new Version(SOURCE_PROTOCOL_VERSION),
        false))
            .thenReturn(discoverResponse);

    final SourceDiscoverSchemaRead actual = schedulerHandler.discoverSchemaForSourceFromSourceId(request);

    assertNotNull(actual.getCatalog());
    assertEquals(actual.getCatalogId(), discoverResponse.getOutput());
    assertNotNull(actual.getJobInfo());
    assertTrue(actual.getJobInfo().getSucceeded());
    verify(configRepository).getSourceConnection(source.getSourceId());
    verify(configRepository).getActorCatalog(eq(request.getSourceId()), eq(SOURCE_DOCKER_TAG), any());
    verify(actorDefinitionVersionHelper).getSourceVersion(sourceDefinition, source.getWorkspaceId(), source.getSourceId());
    verify(synchronousSchedulerClient).createDiscoverSchemaJob(source, SOURCE_DOCKER_IMAGE, SOURCE_DOCKER_TAG, new Version(SOURCE_PROTOCOL_VERSION),
        false);
  }

  @Test
  void testDiscoverSchemaForSourceFromSourceIdCachedCatalog() throws IOException, JsonValidationException, ConfigNotFoundException {
    final SourceConnection source = SourceHelpers.generateSource(UUID.randomUUID());
    final SourceDiscoverSchemaRequestBody request = new SourceDiscoverSchemaRequestBody().sourceId(source.getSourceId());

    final SynchronousResponse<UUID> discoverResponse = (SynchronousResponse<UUID>) jobResponse;
    final SynchronousJobMetadata metadata = mock(SynchronousJobMetadata.class);
    final UUID thisCatalogId = UUID.randomUUID();
    when(discoverResponse.isSuccess()).thenReturn(true);
    when(discoverResponse.getOutput()).thenReturn(thisCatalogId);
    when(discoverResponse.getMetadata()).thenReturn(metadata);
    when(metadata.isSucceeded()).thenReturn(true);

    final StandardSourceDefinition sourceDefinition = new StandardSourceDefinition()
        .withDockerRepository(SOURCE_DOCKER_REPO)
        .withSourceDefinitionId(source.getSourceDefinitionId());
    when(configRepository.getStandardSourceDefinition(source.getSourceDefinitionId()))
        .thenReturn(sourceDefinition);
    when(actorDefinitionVersionHelper.getSourceVersion(sourceDefinition, source.getWorkspaceId(), source.getSourceId()))
        .thenReturn(new ActorDefinitionVersion().withDockerImageTag(SOURCE_DOCKER_TAG));
    when(configRepository.getSourceConnection(source.getSourceId())).thenReturn(source);
    final ActorCatalog actorCatalog = new ActorCatalog()
        .withCatalog(Jsons.jsonNode(airbyteCatalog))
        .withCatalogHash("")
        .withId(thisCatalogId);
    when(configRepository.getActorCatalog(any(), any(), any())).thenReturn(Optional.of(actorCatalog));
    when(synchronousSchedulerClient.createDiscoverSchemaJob(source, SOURCE_DOCKER_IMAGE, SOURCE_DOCKER_TAG, new Version(SOURCE_PROTOCOL_VERSION),
        false))
            .thenReturn(discoverResponse);

    final SourceDiscoverSchemaRead actual = schedulerHandler.discoverSchemaForSourceFromSourceId(request);

    assertNotNull(actual.getCatalog());
    assertNotNull(actual.getJobInfo());
    assertEquals(actual.getCatalogId(), discoverResponse.getOutput());
    assertTrue(actual.getJobInfo().getSucceeded());
    verify(configRepository).getSourceConnection(source.getSourceId());
    verify(configRepository).getActorCatalog(eq(request.getSourceId()), any(), any());
    verify(configRepository, never()).writeActorCatalogFetchEvent(any(), any(), any(), any());
    verify(actorDefinitionVersionHelper).getSourceVersion(sourceDefinition, source.getWorkspaceId(), source.getSourceId());
    verify(synchronousSchedulerClient, never()).createDiscoverSchemaJob(source, SOURCE_DOCKER_IMAGE, SOURCE_DOCKER_TAG,
        new Version(SOURCE_PROTOCOL_VERSION), false);
  }

  @Test
  void testDiscoverSchemaForSourceFromSourceIdDisableCache() throws IOException, JsonValidationException, ConfigNotFoundException {
    final SourceConnection source = SourceHelpers.generateSource(UUID.randomUUID());
    final SourceDiscoverSchemaRequestBody request = new SourceDiscoverSchemaRequestBody().sourceId(source.getSourceId()).disableCache(true);

    final SynchronousResponse<UUID> discoverResponse = (SynchronousResponse<UUID>) jobResponse;
    final SynchronousJobMetadata metadata = mock(SynchronousJobMetadata.class);
    when(discoverResponse.isSuccess()).thenReturn(true);
    final UUID discoveredCatalogId = UUID.randomUUID();
    when(discoverResponse.getOutput()).thenReturn(discoveredCatalogId);
    when(discoverResponse.getMetadata()).thenReturn(metadata);
    when(metadata.isSucceeded()).thenReturn(true);

    final StandardSourceDefinition sourceDefinition = new StandardSourceDefinition()
        .withProtocolVersion(SOURCE_PROTOCOL_VERSION)
        .withSourceDefinitionId(source.getSourceDefinitionId());
    when(configRepository.getStandardSourceDefinition(source.getSourceDefinitionId()))
        .thenReturn(sourceDefinition);
    when(actorDefinitionVersionHelper.getSourceVersion(sourceDefinition, source.getWorkspaceId(), source.getSourceId()))
        .thenReturn(new ActorDefinitionVersion()
            .withDockerRepository(SOURCE_DOCKER_REPO)
            .withDockerImageTag(SOURCE_DOCKER_TAG));
    when(configRepository.getSourceConnection(source.getSourceId())).thenReturn(source);
    final ActorCatalog actorCatalog = new ActorCatalog()
        .withCatalog(Jsons.jsonNode(airbyteCatalog))
        .withCatalogHash("")
        .withId(discoveredCatalogId);
    when(configRepository.getActorCatalogById(discoveredCatalogId)).thenReturn(actorCatalog);
    when(synchronousSchedulerClient.createDiscoverSchemaJob(source, SOURCE_DOCKER_IMAGE, SOURCE_DOCKER_TAG, new Version(SOURCE_PROTOCOL_VERSION),
        false))
            .thenReturn(discoverResponse);

    final SourceDiscoverSchemaRead actual = schedulerHandler.discoverSchemaForSourceFromSourceId(request);

    assertNotNull(actual.getCatalog());
    assertNotNull(actual.getJobInfo());
    assertTrue(actual.getJobInfo().getSucceeded());
    verify(configRepository).getSourceConnection(source.getSourceId());
    verify(configRepository).getActorCatalog(eq(request.getSourceId()), any(), any());
    verify(actorDefinitionVersionHelper).getSourceVersion(sourceDefinition, source.getWorkspaceId(), source.getSourceId());
    verify(synchronousSchedulerClient).createDiscoverSchemaJob(source, SOURCE_DOCKER_IMAGE, SOURCE_DOCKER_TAG, new Version(SOURCE_PROTOCOL_VERSION),
        false);
  }

  @Test
  void testDiscoverSchemaForSourceFromSourceIdFailed() throws IOException, JsonValidationException, ConfigNotFoundException {
    final SourceConnection source = SourceHelpers.generateSource(UUID.randomUUID());
    final SourceDiscoverSchemaRequestBody request = new SourceDiscoverSchemaRequestBody().sourceId(source.getSourceId());

    final StandardSourceDefinition sourceDefinition = new StandardSourceDefinition()
        .withProtocolVersion(SOURCE_PROTOCOL_VERSION)
        .withSourceDefinitionId(source.getSourceDefinitionId());
    when(configRepository.getStandardSourceDefinition(source.getSourceDefinitionId()))
        .thenReturn(sourceDefinition);
    when(actorDefinitionVersionHelper.getSourceVersion(sourceDefinition, source.getWorkspaceId(), source.getSourceId()))
        .thenReturn(new ActorDefinitionVersion()
            .withDockerRepository(SOURCE_DOCKER_REPO)
            .withDockerImageTag(SOURCE_DOCKER_TAG));
    when(configRepository.getSourceConnection(source.getSourceId())).thenReturn(source);
    when(synchronousSchedulerClient.createDiscoverSchemaJob(source, SOURCE_DOCKER_IMAGE, SOURCE_DOCKER_TAG, new Version(SOURCE_PROTOCOL_VERSION),
        false))
            .thenReturn((SynchronousResponse<UUID>) jobResponse);
    when(completedJob.getSuccessOutput()).thenReturn(Optional.empty());
    when(completedJob.getStatus()).thenReturn(JobStatus.FAILED);

    final SourceDiscoverSchemaRead actual = schedulerHandler.discoverSchemaForSourceFromSourceId(request);

    assertNull(actual.getCatalog());
    assertNotNull(actual.getJobInfo());
    assertFalse(actual.getJobInfo().getSucceeded());
    verify(configRepository).getSourceConnection(source.getSourceId());
    verify(actorDefinitionVersionHelper).getSourceVersion(sourceDefinition, source.getWorkspaceId(), source.getSourceId());
    verify(synchronousSchedulerClient).createDiscoverSchemaJob(source, SOURCE_DOCKER_IMAGE, SOURCE_DOCKER_TAG, new Version(SOURCE_PROTOCOL_VERSION),
        false);
  }

  @Test
  void testDiscoverSchemaFromSourceIdWithConnectionIdNonBreaking()
      throws IOException, JsonValidationException, ConfigNotFoundException, InterruptedException, ApiException {
    final SourceConnection source = SourceHelpers.generateSource(UUID.randomUUID());
    final UUID connectionId = UUID.randomUUID();
    final UUID discoveredCatalogId = UUID.randomUUID();
    final SynchronousResponse<UUID> discoverResponse = (SynchronousResponse<UUID>) jobResponse;
    final SourceDiscoverSchemaRequestBody request =
        new SourceDiscoverSchemaRequestBody().sourceId(source.getSourceId()).connectionId(connectionId).disableCache(true).notifySchemaChange(true);
    final StreamTransform streamTransform = new StreamTransform().transformType(TransformTypeEnum.REMOVE_STREAM)
        .streamDescriptor(new io.airbyte.api.model.generated.StreamDescriptor().name(DOGS));
    final CatalogDiff catalogDiff = new CatalogDiff().addTransformsItem(streamTransform);
    final StandardSourceDefinition sourceDef = new StandardSourceDefinition()
        .withProtocolVersion(SOURCE_PROTOCOL_VERSION)
        .withSourceDefinitionId(source.getSourceDefinitionId());
    when(configRepository.getStandardSourceDefinition(source.getSourceDefinitionId()))
        .thenReturn(sourceDef);
    when(actorDefinitionVersionHelper.getSourceVersion(sourceDef, source.getWorkspaceId(), source.getSourceId()))
        .thenReturn(new ActorDefinitionVersion()
            .withDockerRepository(SOURCE_DOCKER_REPO)
            .withDockerImageTag(SOURCE_DOCKER_TAG));
    when(configRepository.getSourceConnection(source.getSourceId())).thenReturn(source);
    when(synchronousSchedulerClient.createDiscoverSchemaJob(source, SOURCE_DOCKER_IMAGE, SOURCE_DOCKER_TAG, new Version(SOURCE_PROTOCOL_VERSION),
        false))
            .thenReturn(discoverResponse);
    when(webUrlHelper.getConnectionUrl(source.getWorkspaceId(), connectionId)).thenReturn(CONNECTION_URL);

    when(discoverResponse.isSuccess()).thenReturn(true);
    when(discoverResponse.getOutput()).thenReturn(discoveredCatalogId);

    final AirbyteCatalog airbyteCatalogCurrent = new AirbyteCatalog().withStreams(Lists.newArrayList(
        CatalogHelpers.createAirbyteStream(SHOES, Field.of(SKU, JsonSchemaType.STRING)),
        CatalogHelpers.createAirbyteStream(DOGS, Field.of(NAME, JsonSchemaType.STRING))));

    final ConnectionRead connectionRead =
        new ConnectionRead().syncCatalog(CatalogConverter.toApi(airbyteCatalogCurrent, sourceDef)).connectionId(connectionId)
            .notifySchemaChanges(true);
    when(connectionsHandler.getConnection(request.getConnectionId())).thenReturn(connectionRead);
    when(connectionsHandler.getDiff(any(), any(), any())).thenReturn(catalogDiff);
    final ConnectionReadList connectionReadList = new ConnectionReadList().connections(List.of(connectionRead));
    when(connectionsHandler.listConnectionsForSource(source.getSourceId(), false)).thenReturn(connectionReadList);

    final ActorCatalog actorCatalog = new ActorCatalog()
        .withCatalog(Jsons.jsonNode(airbyteCatalog))
        .withCatalogHash("")
        .withId(discoveredCatalogId);
    when(configRepository.getActorCatalogById(discoveredCatalogId)).thenReturn(actorCatalog);

    final AirbyteCatalog persistenceCatalog = Jsons.object(actorCatalog.getCatalog(),
        io.airbyte.protocol.models.AirbyteCatalog.class);
    final io.airbyte.api.model.generated.AirbyteCatalog expectedActorCatalog = CatalogConverter.toApi(persistenceCatalog, sourceDef);

    final SourceDiscoverSchemaRead actual = schedulerHandler.discoverSchemaForSourceFromSourceId(request);
    assertEquals(actual.getCatalogDiff(), catalogDiff);
    assertEquals(actual.getCatalog(), expectedActorCatalog);
    verify(eventRunner).sendSchemaChangeNotification(connectionId, CONNECTION_URL);
    verify(actorDefinitionVersionHelper).getSourceVersion(sourceDef, source.getWorkspaceId(), source.getSourceId());
  }

  @Test
  void testDiscoverSchemaFromSourceIdWithConnectionIdNonBreakingDisableConnectionPreferenceNoFeatureFlag()
      throws IOException, JsonValidationException, ConfigNotFoundException {
    final SourceConnection source = SourceHelpers.generateSource(UUID.randomUUID());
    final UUID connectionId = UUID.randomUUID();
    final UUID discoveredCatalogId = UUID.randomUUID();
    final SynchronousResponse<UUID> discoverResponse = (SynchronousResponse<UUID>) jobResponse;
    final SourceDiscoverSchemaRequestBody request =
        new SourceDiscoverSchemaRequestBody().sourceId(source.getSourceId()).connectionId(connectionId).disableCache(true).notifySchemaChange(true);
    final StreamTransform streamTransform = new StreamTransform().transformType(TransformTypeEnum.REMOVE_STREAM)
        .streamDescriptor(new io.airbyte.api.model.generated.StreamDescriptor().name(DOGS));
    final CatalogDiff catalogDiff = new CatalogDiff().addTransformsItem(streamTransform);
    final StandardSourceDefinition sourceDef = new StandardSourceDefinition()
        .withProtocolVersion(SOURCE_PROTOCOL_VERSION)
        .withSourceDefinitionId(source.getSourceDefinitionId());
    when(envVariableFeatureFlags.autoDetectSchema()).thenReturn(false);
    when(configRepository.getStandardSourceDefinition(source.getSourceDefinitionId()))
        .thenReturn(sourceDef);
    when(actorDefinitionVersionHelper.getSourceVersion(sourceDef, source.getWorkspaceId(), source.getSourceId()))
        .thenReturn(new ActorDefinitionVersion()
            .withDockerRepository(SOURCE_DOCKER_REPO)
            .withDockerImageTag(SOURCE_DOCKER_TAG));
    when(configRepository.getSourceConnection(source.getSourceId())).thenReturn(source);
    when(synchronousSchedulerClient.createDiscoverSchemaJob(source, SOURCE_DOCKER_IMAGE, SOURCE_DOCKER_TAG, new Version(SOURCE_PROTOCOL_VERSION),
        false))
            .thenReturn(discoverResponse);
    when(webUrlHelper.getConnectionUrl(source.getWorkspaceId(), connectionId)).thenReturn(CONNECTION_URL);

    when(discoverResponse.isSuccess()).thenReturn(true);
    when(discoverResponse.getOutput()).thenReturn(discoveredCatalogId);

    final AirbyteCatalog airbyteCatalogCurrent = new AirbyteCatalog().withStreams(Lists.newArrayList(
        CatalogHelpers.createAirbyteStream(SHOES, Field.of(SKU, JsonSchemaType.STRING)),
        CatalogHelpers.createAirbyteStream(DOGS, Field.of(NAME, JsonSchemaType.STRING))));

    final ConnectionRead connectionRead =
        new ConnectionRead().syncCatalog(CatalogConverter.toApi(airbyteCatalogCurrent, sourceDef)).nonBreakingChangesPreference(
            NonBreakingChangesPreference.DISABLE).status(ConnectionStatus.ACTIVE).connectionId(connectionId).notifySchemaChanges(true);
    when(connectionsHandler.getConnection(request.getConnectionId())).thenReturn(connectionRead);
    when(connectionsHandler.getDiff(any(), any(), any())).thenReturn(catalogDiff);
    final ConnectionReadList connectionReadList = new ConnectionReadList().connections(List.of(connectionRead));
    when(connectionsHandler.listConnectionsForSource(source.getSourceId(), false)).thenReturn(connectionReadList);

    final ActorCatalog actorCatalog = new ActorCatalog()
        .withCatalog(Jsons.jsonNode(airbyteCatalog))
        .withCatalogHash("")
        .withId(discoveredCatalogId);
    when(configRepository.getActorCatalogById(discoveredCatalogId)).thenReturn(actorCatalog);

    final AirbyteCatalog persistenceCatalog = Jsons.object(actorCatalog.getCatalog(),
        io.airbyte.protocol.models.AirbyteCatalog.class);
    final io.airbyte.api.model.generated.AirbyteCatalog expectedActorCatalog = CatalogConverter.toApi(persistenceCatalog, sourceDef);

    final SourceDiscoverSchemaRead actual = schedulerHandler.discoverSchemaForSourceFromSourceId(request);
    assertEquals(actual.getCatalogDiff(), catalogDiff);
    assertEquals(actual.getCatalog(), expectedActorCatalog);
    assertEquals(actual.getConnectionStatus(), ConnectionStatus.ACTIVE);
    verify(eventRunner).sendSchemaChangeNotification(connectionId, CONNECTION_URL);
  }

  @Test
  void testDiscoverSchemaFromSourceIdWithConnectionIdNonBreakingDisableConnectionPreferenceFeatureFlag()
      throws IOException, JsonValidationException, ConfigNotFoundException {
    final SourceConnection source = SourceHelpers.generateSource(UUID.randomUUID());
    final UUID connectionId = UUID.randomUUID();
    final UUID discoveredCatalogId = UUID.randomUUID();
    final SynchronousResponse<UUID> discoverResponse = (SynchronousResponse<UUID>) jobResponse;
    final SourceDiscoverSchemaRequestBody request =
        new SourceDiscoverSchemaRequestBody().sourceId(source.getSourceId()).connectionId(connectionId).disableCache(true).notifySchemaChange(true);
    final StreamTransform streamTransform = new StreamTransform().transformType(TransformTypeEnum.REMOVE_STREAM)
        .streamDescriptor(new io.airbyte.api.model.generated.StreamDescriptor().name(DOGS));
    final CatalogDiff catalogDiff = new CatalogDiff().addTransformsItem(streamTransform);
    final StandardSourceDefinition sourceDef = new StandardSourceDefinition()
        .withProtocolVersion(SOURCE_PROTOCOL_VERSION)
        .withSourceDefinitionId(source.getSourceDefinitionId());
    when(envVariableFeatureFlags.autoDetectSchema()).thenReturn(true);
    when(configRepository.getStandardSourceDefinition(source.getSourceDefinitionId()))
        .thenReturn(sourceDef);
    when(actorDefinitionVersionHelper.getSourceVersion(sourceDef, source.getWorkspaceId(), source.getSourceId()))
        .thenReturn(new ActorDefinitionVersion()
            .withDockerRepository(SOURCE_DOCKER_REPO)
            .withDockerImageTag(SOURCE_DOCKER_TAG));
    when(configRepository.getSourceConnection(source.getSourceId())).thenReturn(source);
    when(synchronousSchedulerClient.createDiscoverSchemaJob(source, SOURCE_DOCKER_IMAGE, SOURCE_DOCKER_TAG, new Version(SOURCE_PROTOCOL_VERSION),
        false))
            .thenReturn(discoverResponse);

    when(discoverResponse.isSuccess()).thenReturn(true);
    when(discoverResponse.getOutput()).thenReturn(discoveredCatalogId);

    final AirbyteCatalog airbyteCatalogCurrent = new AirbyteCatalog().withStreams(Lists.newArrayList(
        CatalogHelpers.createAirbyteStream(SHOES, Field.of(SKU, JsonSchemaType.STRING)),
        CatalogHelpers.createAirbyteStream(DOGS, Field.of(NAME, JsonSchemaType.STRING))));

    final ConnectionRead connectionRead =
        new ConnectionRead().syncCatalog(CatalogConverter.toApi(airbyteCatalogCurrent, sourceDef)).nonBreakingChangesPreference(
            NonBreakingChangesPreference.DISABLE).connectionId(connectionId).notifySchemaChanges(false);
    when(connectionsHandler.getConnection(request.getConnectionId())).thenReturn(connectionRead);
    when(connectionsHandler.getDiff(any(), any(), any())).thenReturn(catalogDiff);
    final ConnectionReadList connectionReadList = new ConnectionReadList().connections(List.of(connectionRead));
    when(connectionsHandler.listConnectionsForSource(source.getSourceId(), false)).thenReturn(connectionReadList);

    final ActorCatalog actorCatalog = new ActorCatalog()
        .withCatalog(Jsons.jsonNode(airbyteCatalog))
        .withCatalogHash("")
        .withId(discoveredCatalogId);
    when(configRepository.getActorCatalogById(discoveredCatalogId)).thenReturn(actorCatalog);

    final AirbyteCatalog persistenceCatalog = Jsons.object(actorCatalog.getCatalog(),
        io.airbyte.protocol.models.AirbyteCatalog.class);
    final io.airbyte.api.model.generated.AirbyteCatalog expectedActorCatalog = CatalogConverter.toApi(persistenceCatalog, sourceDef);

    final SourceDiscoverSchemaRead actual = schedulerHandler.discoverSchemaForSourceFromSourceId(request);
    assertEquals(actual.getCatalogDiff(), catalogDiff);
    assertEquals(actual.getCatalog(), expectedActorCatalog);
    assertEquals(actual.getConnectionStatus(), ConnectionStatus.INACTIVE);
    verifyNoInteractions(eventRunner);
  }

  @Test
  void testDiscoverSchemaFromSourceIdWithConnectionIdBreaking()
      throws IOException, JsonValidationException, ConfigNotFoundException, InterruptedException, ApiException {
    final SourceConnection source = SourceHelpers.generateSource(UUID.randomUUID());
    final UUID connectionId = UUID.randomUUID();
    final UUID discoveredCatalogId = UUID.randomUUID();
    final SynchronousResponse<UUID> discoverResponse = (SynchronousResponse<UUID>) jobResponse;
    final SourceDiscoverSchemaRequestBody request =
        new SourceDiscoverSchemaRequestBody().sourceId(source.getSourceId()).connectionId(connectionId).disableCache(true).notifySchemaChange(true);
    final StreamTransform streamTransform = new StreamTransform().transformType(TransformTypeEnum.UPDATE_STREAM)
        .streamDescriptor(new io.airbyte.api.model.generated.StreamDescriptor().name(DOGS)).addUpdateStreamItem(new FieldTransform().transformType(
            FieldTransform.TransformTypeEnum.REMOVE_FIELD).breaking(true));
    final CatalogDiff catalogDiff = new CatalogDiff().addTransformsItem(streamTransform);
    final StandardSourceDefinition sourceDef = new StandardSourceDefinition()
        .withProtocolVersion(SOURCE_PROTOCOL_VERSION)
        .withSourceDefinitionId(source.getSourceDefinitionId());
    when(configRepository.getStandardSourceDefinition(source.getSourceDefinitionId()))
        .thenReturn(sourceDef);
    when(actorDefinitionVersionHelper.getSourceVersion(sourceDef, source.getWorkspaceId(), source.getSourceId()))
        .thenReturn(new ActorDefinitionVersion()
            .withDockerRepository(SOURCE_DOCKER_REPO)
            .withDockerImageTag(SOURCE_DOCKER_TAG));
    when(configRepository.getSourceConnection(source.getSourceId())).thenReturn(source);
    when(synchronousSchedulerClient.createDiscoverSchemaJob(source, SOURCE_DOCKER_IMAGE, SOURCE_DOCKER_TAG, new Version(SOURCE_PROTOCOL_VERSION),
        false))
            .thenReturn(discoverResponse);
    when(webUrlHelper.getConnectionUrl(source.getWorkspaceId(), connectionId)).thenReturn(CONNECTION_URL);

    when(discoverResponse.isSuccess()).thenReturn(true);
    when(discoverResponse.getOutput()).thenReturn(discoveredCatalogId);

    final AirbyteCatalog airbyteCatalogCurrent = new AirbyteCatalog().withStreams(Lists.newArrayList(
        CatalogHelpers.createAirbyteStream(SHOES, Field.of(SKU, JsonSchemaType.STRING)),
        CatalogHelpers.createAirbyteStream(DOGS, Field.of(NAME, JsonSchemaType.STRING))));

    final ConnectionRead connectionRead =
        new ConnectionRead().syncCatalog(CatalogConverter.toApi(airbyteCatalogCurrent, sourceDef)).status(ConnectionStatus.ACTIVE)
            .connectionId(connectionId)
            .notifySchemaChanges(true);
    when(connectionsHandler.getConnection(request.getConnectionId())).thenReturn(connectionRead);
    when(connectionsHandler.getDiff(any(), any(), any())).thenReturn(catalogDiff);
    final ConnectionReadList connectionReadList = new ConnectionReadList().connections(List.of(connectionRead));
    when(connectionsHandler.listConnectionsForSource(source.getSourceId(), false)).thenReturn(connectionReadList);

    final ActorCatalog actorCatalog = new ActorCatalog()
        .withCatalog(Jsons.jsonNode(airbyteCatalog))
        .withCatalogHash("")
        .withId(discoveredCatalogId);
    when(configRepository.getActorCatalogById(discoveredCatalogId)).thenReturn(actorCatalog);

    final AirbyteCatalog persistenceCatalog = Jsons.object(actorCatalog.getCatalog(),
        io.airbyte.protocol.models.AirbyteCatalog.class);
    final io.airbyte.api.model.generated.AirbyteCatalog expectedActorCatalog = CatalogConverter.toApi(persistenceCatalog, sourceDef);
    final ConnectionUpdate expectedConnectionUpdate =
        new ConnectionUpdate().connectionId(connectionId).breakingChange(true).status(ConnectionStatus.ACTIVE);

    final SourceDiscoverSchemaRead actual = schedulerHandler.discoverSchemaForSourceFromSourceId(request);
    assertEquals(actual.getCatalogDiff(), catalogDiff);
    assertEquals(actual.getCatalog(), expectedActorCatalog);
    assertEquals(actual.getConnectionStatus(), ConnectionStatus.ACTIVE);
    verify(connectionsHandler).updateConnection(expectedConnectionUpdate);
    verify(eventRunner).sendSchemaChangeNotification(connectionId, CONNECTION_URL);
  }

  @Test
  void testDiscoverSchemaFromSourceIdWithConnectionIdBreakingFeatureFlagOn()
      throws IOException, JsonValidationException, ConfigNotFoundException, InterruptedException, ApiException {
    final SourceConnection source = SourceHelpers.generateSource(UUID.randomUUID());
    final UUID connectionId = UUID.randomUUID();
    final UUID discoveredCatalogId = UUID.randomUUID();
    final SynchronousResponse<UUID> discoverResponse = (SynchronousResponse<UUID>) jobResponse;
    final SourceDiscoverSchemaRequestBody request =
        new SourceDiscoverSchemaRequestBody().sourceId(source.getSourceId()).connectionId(connectionId).disableCache(true).notifySchemaChange(true);
    final StreamTransform streamTransform = new StreamTransform().transformType(TransformTypeEnum.UPDATE_STREAM)
        .streamDescriptor(new io.airbyte.api.model.generated.StreamDescriptor().name(DOGS)).addUpdateStreamItem(new FieldTransform().transformType(
            FieldTransform.TransformTypeEnum.REMOVE_FIELD).breaking(true));
    final CatalogDiff catalogDiff = new CatalogDiff().addTransformsItem(streamTransform);
    final StandardSourceDefinition sourceDef = new StandardSourceDefinition()
        .withProtocolVersion(SOURCE_PROTOCOL_VERSION)
        .withSourceDefinitionId(source.getSourceDefinitionId());
    when(envVariableFeatureFlags.autoDetectSchema()).thenReturn(true);
    when(configRepository.getStandardSourceDefinition(source.getSourceDefinitionId()))
        .thenReturn(sourceDef);
    when(actorDefinitionVersionHelper.getSourceVersion(sourceDef, source.getWorkspaceId(), source.getSourceId()))
        .thenReturn(new ActorDefinitionVersion()
            .withDockerRepository(SOURCE_DOCKER_REPO)
            .withDockerImageTag(SOURCE_DOCKER_TAG));
    when(configRepository.getSourceConnection(source.getSourceId())).thenReturn(source);
    when(synchronousSchedulerClient.createDiscoverSchemaJob(source, SOURCE_DOCKER_IMAGE, SOURCE_DOCKER_TAG, new Version(SOURCE_PROTOCOL_VERSION),
        false))
            .thenReturn(discoverResponse);
    when(webUrlHelper.getConnectionUrl(source.getWorkspaceId(), connectionId)).thenReturn(CONNECTION_URL);

    when(discoverResponse.isSuccess()).thenReturn(true);
    when(discoverResponse.getOutput()).thenReturn(discoveredCatalogId);

    final AirbyteCatalog airbyteCatalogCurrent = new AirbyteCatalog().withStreams(Lists.newArrayList(
        CatalogHelpers.createAirbyteStream(SHOES, Field.of(SKU, JsonSchemaType.STRING)),
        CatalogHelpers.createAirbyteStream(DOGS, Field.of(NAME, JsonSchemaType.STRING))));

    final ConnectionRead connectionRead =
        new ConnectionRead().syncCatalog(CatalogConverter.toApi(airbyteCatalogCurrent, sourceDef)).connectionId(connectionId)
            .notifySchemaChanges(true);
    when(connectionsHandler.getConnection(request.getConnectionId())).thenReturn(connectionRead);
    when(connectionsHandler.getDiff(any(), any(), any())).thenReturn(catalogDiff);
    final ConnectionReadList connectionReadList = new ConnectionReadList().connections(List.of(connectionRead));
    when(connectionsHandler.listConnectionsForSource(source.getSourceId(), false)).thenReturn(connectionReadList);

    final ActorCatalog actorCatalog = new ActorCatalog()
        .withCatalog(Jsons.jsonNode(airbyteCatalog))
        .withCatalogHash("")
        .withId(discoveredCatalogId);
    when(configRepository.getActorCatalogById(discoveredCatalogId)).thenReturn(actorCatalog);

    final AirbyteCatalog persistenceCatalog = Jsons.object(actorCatalog.getCatalog(),
        io.airbyte.protocol.models.AirbyteCatalog.class);
    final io.airbyte.api.model.generated.AirbyteCatalog expectedActorCatalog = CatalogConverter.toApi(persistenceCatalog, sourceDef);
    final ConnectionUpdate expectedConnectionUpdate =
        new ConnectionUpdate().connectionId(connectionId).breakingChange(true).status(ConnectionStatus.INACTIVE);

    final SourceDiscoverSchemaRead actual = schedulerHandler.discoverSchemaForSourceFromSourceId(request);
    assertEquals(actual.getCatalogDiff(), catalogDiff);
    assertEquals(actual.getCatalog(), expectedActorCatalog);
    assertEquals(actual.getConnectionStatus(), ConnectionStatus.INACTIVE);
    verify(connectionsHandler).updateConnection(expectedConnectionUpdate);
    verify(eventRunner).sendSchemaChangeNotification(connectionId, CONNECTION_URL);
  }

  @Test
  void testDiscoverSchemaFromSourceIdWithConnectionIdNonBreakingDisableConnectionPreferenceFeatureFlagNoDiff()
      throws IOException, JsonValidationException, ConfigNotFoundException {
    final SourceConnection source = SourceHelpers.generateSource(UUID.randomUUID());
    final UUID connectionId = UUID.randomUUID();
    final UUID discoveredCatalogId = UUID.randomUUID();
    final SynchronousResponse<UUID> discoverResponse = (SynchronousResponse<UUID>) jobResponse;
    final SourceDiscoverSchemaRequestBody request =
        new SourceDiscoverSchemaRequestBody().sourceId(source.getSourceId()).connectionId(connectionId).disableCache(true).notifySchemaChange(true);
    final CatalogDiff catalogDiff = new CatalogDiff();
    final StandardSourceDefinition sourceDef = new StandardSourceDefinition()
        .withProtocolVersion(SOURCE_PROTOCOL_VERSION)
        .withSourceDefinitionId(source.getSourceDefinitionId());
    when(envVariableFeatureFlags.autoDetectSchema()).thenReturn(true);
    when(configRepository.getStandardSourceDefinition(source.getSourceDefinitionId()))
        .thenReturn(sourceDef);
    when(actorDefinitionVersionHelper.getSourceVersion(sourceDef, source.getWorkspaceId(), source.getSourceId()))
        .thenReturn(new ActorDefinitionVersion()
            .withDockerRepository(SOURCE_DOCKER_REPO)
            .withDockerImageTag(SOURCE_DOCKER_TAG));
    when(configRepository.getSourceConnection(source.getSourceId())).thenReturn(source);
    when(synchronousSchedulerClient.createDiscoverSchemaJob(source, SOURCE_DOCKER_IMAGE, SOURCE_DOCKER_TAG, new Version(SOURCE_PROTOCOL_VERSION),
        false))
            .thenReturn(discoverResponse);

    when(discoverResponse.isSuccess()).thenReturn(true);
    when(discoverResponse.getOutput()).thenReturn(discoveredCatalogId);

    final AirbyteCatalog airbyteCatalogCurrent = new AirbyteCatalog().withStreams(Lists.newArrayList(
        CatalogHelpers.createAirbyteStream(SHOES, Field.of(SKU, JsonSchemaType.STRING)),
        CatalogHelpers.createAirbyteStream(DOGS, Field.of(NAME, JsonSchemaType.STRING))));

    final ConnectionRead connectionRead =
        new ConnectionRead().syncCatalog(CatalogConverter.toApi(airbyteCatalogCurrent, sourceDef)).nonBreakingChangesPreference(
            NonBreakingChangesPreference.DISABLE).status(ConnectionStatus.INACTIVE).connectionId(connectionId).notifySchemaChanges(false);
    when(connectionsHandler.getConnection(request.getConnectionId())).thenReturn(connectionRead);
    when(connectionsHandler.getDiff(any(), any(), any())).thenReturn(catalogDiff);
    final ConnectionReadList connectionReadList = new ConnectionReadList().connections(List.of(connectionRead));
    when(connectionsHandler.listConnectionsForSource(source.getSourceId(), false)).thenReturn(connectionReadList);

    final ActorCatalog actorCatalog = new ActorCatalog()
        .withCatalog(Jsons.jsonNode(airbyteCatalog))
        .withCatalogHash("")
        .withId(discoveredCatalogId);
    when(configRepository.getActorCatalogById(discoveredCatalogId)).thenReturn(actorCatalog);

    final AirbyteCatalog persistenceCatalog = Jsons.object(actorCatalog.getCatalog(),
        io.airbyte.protocol.models.AirbyteCatalog.class);
    final io.airbyte.api.model.generated.AirbyteCatalog expectedActorCatalog = CatalogConverter.toApi(persistenceCatalog, sourceDef);

    final SourceDiscoverSchemaRead actual = schedulerHandler.discoverSchemaForSourceFromSourceId(request);
    assertEquals(actual.getCatalogDiff(), catalogDiff);
    assertEquals(actual.getCatalog(), expectedActorCatalog);
    assertEquals(actual.getConnectionStatus(), ConnectionStatus.INACTIVE);
    // notification preferences are turned on, but there is no schema diff detected
    verifyNoInteractions(eventRunner);
  }

  @Test
  void testDiscoverSchemaForSourceMultipleConnectionsFeatureFlagOn()
      throws IOException, JsonValidationException, ConfigNotFoundException, InterruptedException, ApiException {
    final SourceConnection source = SourceHelpers.generateSource(UUID.randomUUID());
    final UUID connectionId = UUID.randomUUID();
    final UUID connectionId2 = UUID.randomUUID();
    final UUID connectionId3 = UUID.randomUUID();
    final UUID discoveredCatalogId = UUID.randomUUID();
    final SynchronousResponse<UUID> discoverResponse = (SynchronousResponse<UUID>) jobResponse;
    final SourceDiscoverSchemaRequestBody request =
        new SourceDiscoverSchemaRequestBody().sourceId(source.getSourceId()).connectionId(connectionId).disableCache(true).notifySchemaChange(true);

    // 3 connections use the same source. 2 will generate catalog diffs that are non-breaking, 1 will
    // generate a breaking catalog diff
    final StreamTransform nonBreakingStreamTransform = new StreamTransform().transformType(TransformTypeEnum.UPDATE_STREAM)
        .streamDescriptor(new io.airbyte.api.model.generated.StreamDescriptor().name(DOGS)).addUpdateStreamItem(new FieldTransform().transformType(
            FieldTransform.TransformTypeEnum.REMOVE_FIELD).breaking(false));
    final StreamTransform breakingStreamTransform = new StreamTransform().transformType(TransformTypeEnum.UPDATE_STREAM)
        .streamDescriptor(new io.airbyte.api.model.generated.StreamDescriptor().name(DOGS)).addUpdateStreamItem(new FieldTransform().transformType(
            FieldTransform.TransformTypeEnum.REMOVE_FIELD).breaking(true));

    final CatalogDiff catalogDiff1 = new CatalogDiff().addTransformsItem(nonBreakingStreamTransform);
    final CatalogDiff catalogDiff2 = new CatalogDiff().addTransformsItem(nonBreakingStreamTransform);
    final CatalogDiff catalogDiff3 = new CatalogDiff().addTransformsItem(breakingStreamTransform);
    final StandardSourceDefinition sourceDef = new StandardSourceDefinition()
        .withProtocolVersion(SOURCE_PROTOCOL_VERSION)
        .withSourceDefinitionId(source.getSourceDefinitionId());

    when(envVariableFeatureFlags.autoDetectSchema()).thenReturn(true);
    when(configRepository.getStandardSourceDefinition(source.getSourceDefinitionId()))
        .thenReturn(sourceDef);
    when(actorDefinitionVersionHelper.getSourceVersion(sourceDef, source.getWorkspaceId(), source.getSourceId()))
        .thenReturn(new ActorDefinitionVersion()
            .withDockerRepository(SOURCE_DOCKER_REPO)
            .withDockerImageTag(SOURCE_DOCKER_TAG));
    when(configRepository.getSourceConnection(source.getSourceId())).thenReturn(source);
    when(synchronousSchedulerClient.createDiscoverSchemaJob(source, SOURCE_DOCKER_IMAGE, SOURCE_DOCKER_TAG, new Version(SOURCE_PROTOCOL_VERSION),
        false))
            .thenReturn(discoverResponse);
    when(webUrlHelper.getConnectionUrl(source.getWorkspaceId(), connectionId)).thenReturn(CONNECTION_URL);
    when(webUrlHelper.getConnectionUrl(source.getWorkspaceId(), connectionId2)).thenReturn(CONNECTION_URL);
    when(webUrlHelper.getConnectionUrl(source.getWorkspaceId(), connectionId3)).thenReturn(CONNECTION_URL);

    when(discoverResponse.isSuccess()).thenReturn(true);
    when(discoverResponse.getOutput()).thenReturn(discoveredCatalogId);

    final AirbyteCatalog airbyteCatalogCurrent = new AirbyteCatalog().withStreams(Lists.newArrayList(
        CatalogHelpers.createAirbyteStream(SHOES, Field.of(SKU, JsonSchemaType.STRING)),
        CatalogHelpers.createAirbyteStream(DOGS, Field.of(NAME, JsonSchemaType.STRING))));

    final ConnectionRead connectionRead =
        new ConnectionRead().syncCatalog(CatalogConverter.toApi(airbyteCatalogCurrent, sourceDef)).nonBreakingChangesPreference(
            NonBreakingChangesPreference.IGNORE).status(ConnectionStatus.ACTIVE).connectionId(connectionId).notifySchemaChanges(true);

    final ConnectionRead connectionRead2 =
        new ConnectionRead().syncCatalog(CatalogConverter.toApi(airbyteCatalogCurrent, sourceDef)).nonBreakingChangesPreference(
            NonBreakingChangesPreference.IGNORE).status(ConnectionStatus.ACTIVE).connectionId(connectionId2).notifySchemaChanges(true);

    final ConnectionRead connectionRead3 =
        new ConnectionRead().syncCatalog(CatalogConverter.toApi(airbyteCatalogCurrent, sourceDef)).nonBreakingChangesPreference(
            NonBreakingChangesPreference.DISABLE).status(ConnectionStatus.ACTIVE).connectionId(connectionId3).notifySchemaChanges(false);

    when(connectionsHandler.getConnection(request.getConnectionId())).thenReturn(connectionRead, connectionRead2, connectionRead3);
    when(connectionsHandler.getDiff(any(), any(), any())).thenReturn(catalogDiff1, catalogDiff2, catalogDiff3);
    final ConnectionReadList connectionReadList = new ConnectionReadList().connections(List.of(connectionRead, connectionRead2, connectionRead3));
    when(connectionsHandler.listConnectionsForSource(source.getSourceId(), false)).thenReturn(connectionReadList);

    final ActorCatalog actorCatalog = new ActorCatalog()
        .withCatalog(Jsons.jsonNode(airbyteCatalog))
        .withCatalogHash("")
        .withId(discoveredCatalogId);
    when(configRepository.getActorCatalogById(discoveredCatalogId)).thenReturn(actorCatalog);

    final AirbyteCatalog persistenceCatalog = Jsons.object(actorCatalog.getCatalog(),
        io.airbyte.protocol.models.AirbyteCatalog.class);
    final io.airbyte.api.model.generated.AirbyteCatalog expectedActorCatalog = CatalogConverter.toApi(persistenceCatalog, sourceDef);

    final SourceDiscoverSchemaRead actual = schedulerHandler.discoverSchemaForSourceFromSourceId(request);
    assertEquals(catalogDiff1, actual.getCatalogDiff());
    assertEquals(expectedActorCatalog, actual.getCatalog());
    assertEquals(ConnectionStatus.ACTIVE, actual.getConnectionStatus());

    final ArgumentCaptor<ConnectionUpdate> expectedArgumentCaptor = ArgumentCaptor.forClass(ConnectionUpdate.class);
    verify(connectionsHandler, times(3)).updateConnection(expectedArgumentCaptor.capture());
    final List<ConnectionUpdate> connectionUpdateValues = expectedArgumentCaptor.getAllValues();
    assertEquals(ConnectionStatus.ACTIVE, connectionUpdateValues.get(0).getStatus());
    assertEquals(ConnectionStatus.ACTIVE, connectionUpdateValues.get(1).getStatus());
    assertEquals(ConnectionStatus.INACTIVE, connectionUpdateValues.get(2).getStatus());
    verify(eventRunner).sendSchemaChangeNotification(connectionId, CONNECTION_URL);
    verify(eventRunner).sendSchemaChangeNotification(connectionId2, CONNECTION_URL);
    verify(eventRunner, times(0)).sendSchemaChangeNotification(connectionId3, CONNECTION_URL);
  }

  @Test
  void testDiscoverSchemaFromSourceIdWithConnectionUpdateNonSuccessResponse() throws IOException, JsonValidationException, ConfigNotFoundException {
    final SourceConnection source = SourceHelpers.generateSource(UUID.randomUUID());
    final SourceDiscoverSchemaRequestBody request = new SourceDiscoverSchemaRequestBody().sourceId(source.getSourceId())
        .connectionId(UUID.randomUUID()).notifySchemaChange(true);

    // Mock the source definition.
    final StandardSourceDefinition sourceDefinition = new StandardSourceDefinition()
        .withProtocolVersion(SOURCE_PROTOCOL_VERSION)
        .withSourceDefinitionId(source.getSourceDefinitionId());
    when(configRepository.getStandardSourceDefinition(source.getSourceDefinitionId()))
        .thenReturn(sourceDefinition);
    when(actorDefinitionVersionHelper.getSourceVersion(sourceDefinition, source.getWorkspaceId(), source.getSourceId()))
        .thenReturn(new ActorDefinitionVersion()
            .withDockerRepository(SOURCE_DOCKER_REPO)
            .withDockerImageTag(SOURCE_DOCKER_TAG));
    // Mock the source itself.
    when(configRepository.getSourceConnection(source.getSourceId())).thenReturn(source);
    // Mock the Discover job results.
    final SynchronousResponse<UUID> discoverResponse = (SynchronousResponse<UUID>) jobResponse;
    final SynchronousJobMetadata metadata = mock(SynchronousJobMetadata.class);
    when(discoverResponse.isSuccess()).thenReturn(false);
    when(discoverResponse.getMetadata()).thenReturn(metadata);
    when(metadata.isSucceeded()).thenReturn(false);
    when(synchronousSchedulerClient.createDiscoverSchemaJob(source, SOURCE_DOCKER_IMAGE, SOURCE_DOCKER_TAG, new Version(SOURCE_PROTOCOL_VERSION),
        false))
            .thenReturn(discoverResponse);

    final SourceDiscoverSchemaRead actual = schedulerHandler.discoverSchemaForSourceFromSourceId(request);

    assertNull(actual.getCatalog());
    assertNotNull(actual.getJobInfo());
    assertFalse(actual.getJobInfo().getSucceeded());
    verify(synchronousSchedulerClient).createDiscoverSchemaJob(source, SOURCE_DOCKER_IMAGE, SOURCE_DOCKER_TAG, new Version(SOURCE_PROTOCOL_VERSION),
        false);
  }

  @Test
  void testDiscoverSchemaForSourceFromSourceCreate() throws JsonValidationException, IOException, ConfigNotFoundException {
    final SourceConnection source = new SourceConnection()
        .withSourceDefinitionId(SOURCE.getSourceDefinitionId())
        .withConfiguration(SOURCE.getConfiguration())
        .withWorkspaceId(SOURCE.getWorkspaceId());

    final SynchronousResponse<UUID> discoverResponse = (SynchronousResponse<UUID>) jobResponse;
    final SynchronousJobMetadata metadata = mock(SynchronousJobMetadata.class);
    when(discoverResponse.isSuccess()).thenReturn(true);
    when(discoverResponse.getOutput()).thenReturn(UUID.randomUUID());
    when(discoverResponse.getMetadata()).thenReturn(metadata);
    when(metadata.isSucceeded()).thenReturn(true);

    final SourceCoreConfig sourceCoreConfig = new SourceCoreConfig()
        .sourceDefinitionId(source.getSourceDefinitionId())
        .connectionConfiguration(source.getConfiguration())
        .workspaceId(source.getWorkspaceId());
    final ActorCatalog actorCatalog = new ActorCatalog()
        .withCatalog(Jsons.jsonNode(airbyteCatalog))
        .withCatalogHash("")
        .withId(UUID.randomUUID());
    when(configRepository.getActorCatalogById(any())).thenReturn(actorCatalog);

    final StandardSourceDefinition sourceDefinition = new StandardSourceDefinition()
        .withProtocolVersion(SOURCE_PROTOCOL_VERSION)
        .withSourceDefinitionId(source.getSourceDefinitionId());
    when(configRepository.getStandardSourceDefinition(source.getSourceDefinitionId()))
        .thenReturn(sourceDefinition);
    when(actorDefinitionVersionHelper.getSourceVersion(sourceDefinition, source.getWorkspaceId(), null))
        .thenReturn(new ActorDefinitionVersion()
            .withDockerRepository(SOURCE_DOCKER_REPO)
            .withDockerImageTag(SOURCE_DOCKER_TAG));
    when(synchronousSchedulerClient.createDiscoverSchemaJob(source, SOURCE_DOCKER_IMAGE, SOURCE_DOCKER_TAG, new Version(SOURCE_PROTOCOL_VERSION),
        false))
            .thenReturn(discoverResponse);
    when(secretsRepositoryWriter.statefulSplitEphemeralSecrets(
        eq(source.getConfiguration()),
        any())).thenReturn(source.getConfiguration());

    final SourceDiscoverSchemaRead actual = schedulerHandler.discoverSchemaForSourceFromSourceCreate(sourceCoreConfig);

    assertNotNull(actual.getCatalog());
    assertNotNull(actual.getJobInfo());
    assertEquals(actual.getCatalogId(), discoverResponse.getOutput());
    assertTrue(actual.getJobInfo().getSucceeded());
    verify(synchronousSchedulerClient).createDiscoverSchemaJob(source, SOURCE_DOCKER_IMAGE, SOURCE_DOCKER_TAG, new Version(SOURCE_PROTOCOL_VERSION),
        false);
    verify(actorDefinitionVersionHelper).getSourceVersion(sourceDefinition, source.getWorkspaceId(), null);
  }

  @Test
  void testDiscoverSchemaForSourceFromSourceCreateFailed() throws JsonValidationException, IOException, ConfigNotFoundException {
    final SourceConnection source = new SourceConnection()
        .withSourceDefinitionId(SOURCE.getSourceDefinitionId())
        .withConfiguration(SOURCE.getConfiguration());

    final SourceCoreConfig sourceCoreConfig = new SourceCoreConfig()
        .sourceDefinitionId(source.getSourceDefinitionId())
        .connectionConfiguration(source.getConfiguration())
        .workspaceId(source.getWorkspaceId());

    final StandardSourceDefinition sourceDefinition = new StandardSourceDefinition()
        .withProtocolVersion(SOURCE_PROTOCOL_VERSION)
        .withSourceDefinitionId(source.getSourceDefinitionId());
    when(configRepository.getStandardSourceDefinition(source.getSourceDefinitionId()))
        .thenReturn(sourceDefinition);
    when(actorDefinitionVersionHelper.getSourceVersion(sourceDefinition, source.getWorkspaceId(), null))
        .thenReturn(new ActorDefinitionVersion()
            .withDockerRepository(SOURCE_DOCKER_REPO)
            .withDockerImageTag(SOURCE_DOCKER_TAG));
    when(synchronousSchedulerClient.createDiscoverSchemaJob(source, SOURCE_DOCKER_IMAGE, SOURCE_DOCKER_TAG, new Version(SOURCE_PROTOCOL_VERSION),
        false))
            .thenReturn((SynchronousResponse<UUID>) jobResponse);
    when(secretsRepositoryWriter.statefulSplitEphemeralSecrets(
        eq(source.getConfiguration()),
        any())).thenReturn(source.getConfiguration());
    when(completedJob.getSuccessOutput()).thenReturn(Optional.empty());
    when(completedJob.getStatus()).thenReturn(JobStatus.FAILED);

    final SourceDiscoverSchemaRead actual = schedulerHandler.discoverSchemaForSourceFromSourceCreate(sourceCoreConfig);

    assertNull(actual.getCatalog());
    assertNotNull(actual.getJobInfo());
    assertFalse(actual.getJobInfo().getSucceeded());
    verify(synchronousSchedulerClient).createDiscoverSchemaJob(source, SOURCE_DOCKER_IMAGE, SOURCE_DOCKER_TAG, new Version(SOURCE_PROTOCOL_VERSION),
        false);
  }

  @Test
  void testEnumConversion() {
    assertTrue(Enums.isCompatible(StandardCheckConnectionOutput.Status.class, CheckConnectionRead.StatusEnum.class));
    assertTrue(Enums.isCompatible(JobStatus.class, io.airbyte.api.model.generated.JobStatus.class));
  }

  @Test
  void testSyncConnection() throws IOException, JsonValidationException, ConfigNotFoundException {
    final UUID connectionId = UUID.randomUUID();

    final long jobId = 123L;
    final ManualOperationResult manualOperationResult = ManualOperationResult
        .builder()
        .failingReason(Optional.empty())
        .jobId(Optional.of(jobId))
        .build();

    when(eventRunner.startNewManualSync(connectionId))
        .thenReturn(manualOperationResult);

    doReturn(new JobInfoRead())
        .when(jobConverter).getJobInfoRead(any());

    schedulerHandler.syncConnection(new ConnectionIdRequestBody().connectionId(connectionId));

    verify(eventRunner).startNewManualSync(connectionId);
  }

  @Test
  void testSyncConnectionFailWithOtherSyncRunning() throws IOException {
    final UUID connectionId = UUID.randomUUID();

    final ManualOperationResult manualOperationResult = ManualOperationResult
        .builder()
        .failingReason(Optional.of("another sync running"))
        .jobId(Optional.empty())
        .errorCode(Optional.of(ErrorCode.WORKFLOW_RUNNING))
        .build();

    when(eventRunner.startNewManualSync(connectionId))
        .thenReturn(manualOperationResult);

    assertThrows(ValueConflictKnownException.class,
        () -> schedulerHandler.syncConnection(new ConnectionIdRequestBody().connectionId(connectionId)));

  }

  @Test
  void testResetConnection() throws IOException, JsonValidationException, ConfigNotFoundException {
    final UUID connectionId = UUID.randomUUID();

    final long jobId = 123L;
    final ManualOperationResult manualOperationResult = ManualOperationResult
        .builder()
        .failingReason(Optional.empty())
        .jobId(Optional.of(jobId))
        .build();

    final List<StreamDescriptor> streamDescriptors = List.of(STREAM_DESCRIPTOR);
    when(configRepository.getAllStreamsForConnection(connectionId))
        .thenReturn(streamDescriptors);

    when(eventRunner.resetConnection(connectionId, streamDescriptors, false))
        .thenReturn(manualOperationResult);

    doReturn(new JobInfoRead())
        .when(jobConverter).getJobInfoRead(any());

    schedulerHandler.resetConnection(new ConnectionIdRequestBody().connectionId(connectionId));

    verify(eventRunner).resetConnection(connectionId, streamDescriptors, false);
  }

  @Test
  void testResetConnectionStream() throws IOException {
    final UUID connectionId = UUID.randomUUID();
    final String streamName = "name";
    final String streamNamespace = "namespace";

    final long jobId = 123L;
    final ManualOperationResult manualOperationResult = ManualOperationResult
        .builder()
        .failingReason(Optional.empty())
        .jobId(Optional.of(jobId))
        .build();
    final List<StreamDescriptor> streamDescriptors = List.of(new StreamDescriptor().withName(streamName).withNamespace(streamNamespace));
    final ConnectionStreamRequestBody connectionStreamRequestBody = new ConnectionStreamRequestBody()
        .connectionId(connectionId)
        .streams(List.of(new ConnectionStream().streamName(streamName).streamNamespace(streamNamespace)));

    when(eventRunner.resetConnection(connectionId, streamDescriptors, false))
        .thenReturn(manualOperationResult);

    doReturn(new JobInfoRead())
        .when(jobConverter).getJobInfoRead(any());

    schedulerHandler
        .resetConnectionStream(connectionStreamRequestBody);

    verify(eventRunner).resetConnection(connectionId, streamDescriptors, false);
  }

  @Test
  void testCancelJob() throws IOException {
    final UUID connectionId = UUID.randomUUID();
    final long jobId = 123L;
    final Job job = mock(Job.class);
    when(job.getScope()).thenReturn(connectionId.toString());
    when(jobPersistence.getJob(jobId)).thenReturn(job);

    final ManualOperationResult manualOperationResult = ManualOperationResult
        .builder()
        .failingReason(Optional.empty())
        .jobId(Optional.of(jobId))
        .build();

    when(eventRunner.startNewCancellation(connectionId))
        .thenReturn(manualOperationResult);

    doReturn(new JobInfoRead())
        .when(jobConverter).getJobInfoRead(any());

    schedulerHandler.cancelJob(new JobIdRequestBody().id(jobId));

    verify(eventRunner).startNewCancellation(connectionId);
  }

}
