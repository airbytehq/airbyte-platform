/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.scheduler;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.server.handlers.helpers.ContextBuilder;
import io.airbyte.commons.temporal.JobMetadata;
import io.airbyte.commons.temporal.TemporalClient;
import io.airbyte.commons.temporal.TemporalResponse;
import io.airbyte.commons.temporal.TemporalTaskQueueUtils;
import io.airbyte.commons.version.Version;
import io.airbyte.config.ActorContext;
import io.airbyte.config.ActorDefinitionVersion;
import io.airbyte.config.ActorType;
import io.airbyte.config.ConnectorJobOutput;
import io.airbyte.config.DestinationConnection;
import io.airbyte.config.FailureReason;
import io.airbyte.config.JobCheckConnectionConfig;
import io.airbyte.config.JobConfig.ConfigType;
import io.airbyte.config.JobDiscoverCatalogConfig;
import io.airbyte.config.JobGetSpecConfig;
import io.airbyte.config.ReleaseStage;
import io.airbyte.config.SourceConnection;
import io.airbyte.config.StandardCheckConnectionOutput;
import io.airbyte.config.WorkloadPriority;
import io.airbyte.config.persistence.ConfigInjector;
import io.airbyte.config.secrets.ConfigWithSecretReferences;
import io.airbyte.domain.services.secrets.SecretReferenceService;
import io.airbyte.persistence.job.errorreporter.ConnectorJobReportingContext;
import io.airbyte.persistence.job.errorreporter.JobErrorReporter;
import io.airbyte.persistence.job.factory.OAuthConfigSupplier;
import io.airbyte.persistence.job.tracker.JobTracker;
import io.airbyte.persistence.job.tracker.JobTracker.JobState;
import io.airbyte.protocol.models.v0.ConnectorSpecification;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import java.util.function.Supplier;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

// the goal here is to test the "execute" part of this class and all of the various exceptional
// cases. then separately test submission of each job type without having to re-test all of the
// execution exception cases again.
class DefaultSynchronousSchedulerClientTest {

  private static final Path LOG_PATH = Path.of("/tmp");
  private static final String DOCKER_REPOSITORY = "airbyte/source-foo";
  private static final String DOCKER_IMAGE_TAG = "1.2.3";
  private static final String DOCKER_IMAGE = DOCKER_REPOSITORY + ":" + DOCKER_IMAGE_TAG;
  private static final Version PROTOCOL_VERSION = new Version("0.2.3");
  private static final UUID WORKSPACE_ID = UUID.randomUUID();
  private static final UUID ACTOR_ID = UUID.randomUUID();
  private static final UUID UUID1 = UUID.randomUUID();
  private static final UUID UUID2 = UUID.randomUUID();
  private static final String UNCHECKED = "unchecked";
  private static final String CHECK_TASK_QUEUE = TemporalTaskQueueUtils.INSTANCE.getDEFAULT_CHECK_TASK_QUEUE();
  private static final String DISCOVER_TASK_QUEUE = TemporalTaskQueueUtils.INSTANCE.getDEFAULT_DISCOVER_TASK_QUEUE();
  private static final JsonNode CONFIGURATION = Jsons.jsonNode(ImmutableMap.builder()
      .put("username", "airbyte")
      .put("password", "abc")
      .build());

  private static final ConfigWithSecretReferences CONFIG_WITH_REFS = new ConfigWithSecretReferences(CONFIGURATION, Map.of());
  private static final SourceConnection SOURCE_CONNECTION = new SourceConnection()
      .withWorkspaceId(WORKSPACE_ID)
      .withSourceId(UUID1)
      .withSourceDefinitionId(UUID2)
      .withConfiguration(CONFIGURATION);
  private static final DestinationConnection DESTINATION_CONNECTION = new DestinationConnection()
      .withWorkspaceId(WORKSPACE_ID)
      .withDestinationId(UUID1)
      .withDestinationDefinitionId(UUID2)
      .withConfiguration(CONFIGURATION);
  private static final ActorDefinitionVersion ACTOR_DEFINITION_VERSION = new ActorDefinitionVersion()
      .withDockerRepository(DOCKER_REPOSITORY)
      .withDockerImageTag(DOCKER_IMAGE_TAG)
      .withProtocolVersion(PROTOCOL_VERSION.serialize())
      .withReleaseStage(ReleaseStage.BETA);
  private static final String SOURCE_DOCKER_IMAGE = "source-airbyte:1.2.3";
  private static final ReleaseStage SOURCE_RELEASE_STAGE = ReleaseStage.BETA;
  private static final Long SOURCE_INTERNAL_SUPPORT_LEVEL = null;

  private TemporalClient temporalClient;
  private JobTracker jobTracker;
  private JobErrorReporter jobErrorReporter;
  private OAuthConfigSupplier oAuthConfigSupplier;
  private ConfigInjector configInjector;
  private SecretReferenceService secretReferenceService;
  private DefaultSynchronousSchedulerClient schedulerClient;
  private ContextBuilder contextBuilder;

  @BeforeEach
  void setup() throws IOException {
    temporalClient = mock(TemporalClient.class);
    jobTracker = mock(JobTracker.class);
    jobErrorReporter = mock(JobErrorReporter.class);
    oAuthConfigSupplier = mock(OAuthConfigSupplier.class);
    configInjector = mock(ConfigInjector.class);
    contextBuilder = mock(ContextBuilder.class);
    secretReferenceService = mock(SecretReferenceService.class);
    schedulerClient =
        new DefaultSynchronousSchedulerClient(temporalClient, jobTracker, jobErrorReporter, oAuthConfigSupplier, configInjector,
            contextBuilder, secretReferenceService);

    when(oAuthConfigSupplier.injectSourceOAuthParameters(any(), any(), any(), eq(CONFIGURATION))).thenReturn(CONFIGURATION);
    when(oAuthConfigSupplier.injectDestinationOAuthParameters(any(), any(), any(), eq(CONFIGURATION))).thenReturn(CONFIGURATION);

    when(configInjector.injectConfig(any(), any())).thenAnswer(i -> i.getArguments()[0]);

    when(secretReferenceService.getConfigWithSecretReferences(any(), eq(CONFIGURATION), any()))
        .thenReturn(CONFIG_WITH_REFS);

    when(contextBuilder.fromDestination(any())).thenReturn(new ActorContext());
    when(contextBuilder.fromSource(any())).thenReturn(new ActorContext());
  }

  private static JobMetadata createMetadata(final boolean succeeded) {
    return new JobMetadata(
        succeeded,
        LOG_PATH);
  }

  @Nested
  @DisplayName("Test execute method.")
  class ExecuteSynchronousJob {

    @SuppressWarnings(UNCHECKED)
    @Test
    void testExecuteJobSuccess() {
      final UUID sourceDefinitionId = UUID.randomUUID();
      final UUID discoveredCatalogId = UUID.randomUUID();
      final Supplier<TemporalResponse<ConnectorJobOutput>> function = mock(Supplier.class);
      final Function<ConnectorJobOutput, UUID> mapperFunction = ConnectorJobOutput::getDiscoverCatalogId;
      final ConnectorJobOutput jobOutput = new ConnectorJobOutput().withDiscoverCatalogId(discoveredCatalogId);
      when(function.get()).thenReturn(new TemporalResponse<>(jobOutput, createMetadata(true)));

      final ConnectorJobReportingContext jobContext =
          new ConnectorJobReportingContext(UUID.randomUUID(), SOURCE_DOCKER_IMAGE, SOURCE_RELEASE_STAGE, SOURCE_INTERNAL_SUPPORT_LEVEL);
      final SynchronousResponse<UUID> response = schedulerClient
          .execute(ConfigType.DISCOVER_SCHEMA, jobContext, sourceDefinitionId, function, mapperFunction, WORKSPACE_ID, ACTOR_ID);

      assertNotNull(response);
      assertEquals(discoveredCatalogId, response.getOutput());
      assertEquals(ConfigType.DISCOVER_SCHEMA, response.getMetadata().getConfigType());
      assertTrue(response.getMetadata().getConfigId().isPresent());
      assertEquals(sourceDefinitionId, response.getMetadata().getConfigId().get());
      assertTrue(response.getMetadata().isSucceeded());
      assertEquals(LOG_PATH, response.getMetadata().getLogPath());

      verify(jobTracker).trackDiscover(any(UUID.class), eq(sourceDefinitionId), eq(WORKSPACE_ID), eq(ACTOR_ID), eq(JobState.STARTED), eq(null));
      verify(jobTracker).trackDiscover(any(UUID.class), eq(sourceDefinitionId), eq(WORKSPACE_ID), eq(ACTOR_ID), eq(JobState.SUCCEEDED),
          eq(jobOutput));
      verifyNoInteractions(jobErrorReporter);
    }

    @SuppressWarnings(UNCHECKED)
    @Test
    void testExecuteJobFailure() {
      final UUID sourceDefinitionId = UUID.randomUUID();
      final Supplier<TemporalResponse<ConnectorJobOutput>> function = mock(Supplier.class);
      final Function<ConnectorJobOutput, UUID> mapperFunction = ConnectorJobOutput::getDiscoverCatalogId;
      final FailureReason failureReason = new FailureReason();
      final ConnectorJobOutput failedJobOutput = new ConnectorJobOutput().withFailureReason(failureReason);
      when(function.get()).thenReturn(new TemporalResponse<>(failedJobOutput, createMetadata(false)));

      final ConnectorJobReportingContext jobContext =
          new ConnectorJobReportingContext(UUID.randomUUID(), SOURCE_DOCKER_IMAGE, SOURCE_RELEASE_STAGE, SOURCE_INTERNAL_SUPPORT_LEVEL);
      final SynchronousResponse<UUID> response = schedulerClient
          .execute(ConfigType.DISCOVER_SCHEMA, jobContext, sourceDefinitionId, function, mapperFunction, WORKSPACE_ID, ACTOR_ID);

      assertNotNull(response);
      assertNull(response.getOutput());
      assertEquals(ConfigType.DISCOVER_SCHEMA, response.getMetadata().getConfigType());
      assertTrue(response.getMetadata().getConfigId().isPresent());
      assertEquals(sourceDefinitionId, response.getMetadata().getConfigId().get());
      assertFalse(response.getMetadata().isSucceeded());
      assertEquals(LOG_PATH, response.getMetadata().getLogPath());

      verify(jobTracker).trackDiscover(any(UUID.class), eq(sourceDefinitionId), eq(WORKSPACE_ID), eq(ACTOR_ID), eq(JobState.STARTED), eq(null));
      verify(jobTracker).trackDiscover(any(UUID.class), eq(sourceDefinitionId), eq(WORKSPACE_ID), eq(ACTOR_ID), eq(JobState.FAILED),
          eq(failedJobOutput));
    }

    @SuppressWarnings(UNCHECKED)
    @Test
    void testExecuteRuntimeException() {
      final UUID sourceDefinitionId = UUID.randomUUID();
      final Supplier<TemporalResponse<ConnectorJobOutput>> function = mock(Supplier.class);
      final Function<ConnectorJobOutput, UUID> mapperFunction = ConnectorJobOutput::getDiscoverCatalogId;
      when(function.get()).thenThrow(new RuntimeException());
      final ConnectorJobReportingContext jobContext =
          new ConnectorJobReportingContext(UUID.randomUUID(), SOURCE_DOCKER_IMAGE, SOURCE_RELEASE_STAGE, SOURCE_INTERNAL_SUPPORT_LEVEL);
      assertThrows(
          RuntimeException.class,
          () -> schedulerClient.execute(ConfigType.DISCOVER_SCHEMA, jobContext, sourceDefinitionId, function,
              mapperFunction, WORKSPACE_ID, ACTOR_ID));

      verify(jobTracker).trackDiscover(any(UUID.class), eq(sourceDefinitionId), eq(WORKSPACE_ID), eq(ACTOR_ID), eq(JobState.STARTED), eq(null));
      verify(jobTracker).trackDiscover(any(UUID.class), eq(sourceDefinitionId), eq(WORKSPACE_ID), eq(ACTOR_ID), eq(JobState.FAILED), eq(null));
      verifyNoInteractions(jobErrorReporter);
    }

  }

  @Nested
  @DisplayName("Test job creation for each configuration type.")
  class TestJobCreation {

    @Test
    void testCreateSourceCheckConnectionJob() throws IOException {
      final JobCheckConnectionConfig jobCheckConnectionConfig = new JobCheckConnectionConfig()
          .withActorType(ActorType.SOURCE)
          .withActorId(SOURCE_CONNECTION.getSourceId())
          .withConnectionConfiguration(CONFIG_WITH_REFS)
          .withDockerImage(DOCKER_IMAGE)
          .withProtocolVersion(PROTOCOL_VERSION).withIsCustomConnector(false);

      final StandardCheckConnectionOutput mockOutput = mock(StandardCheckConnectionOutput.class);
      final ConnectorJobOutput jobOutput = new ConnectorJobOutput().withCheckConnection(mockOutput);
      when(temporalClient.submitCheckConnection(any(UUID.class), eq(0), eq(WORKSPACE_ID), eq(CHECK_TASK_QUEUE), eq(jobCheckConnectionConfig), any(
          ActorContext.class)))
              .thenReturn(new TemporalResponse<>(jobOutput, createMetadata(true)));
      final SynchronousResponse<StandardCheckConnectionOutput> response =
          schedulerClient.createSourceCheckConnectionJob(SOURCE_CONNECTION, ACTOR_DEFINITION_VERSION, false, null);
      assertEquals(mockOutput, response.getOutput());
      verify(configInjector).injectConfig(any(), eq(SOURCE_CONNECTION.getSourceDefinitionId()));
    }

    @Test
    void testCreateSourceCheckConnectionJobWithConfigInjection() throws IOException {
      final JsonNode configAfterInjection = new ObjectMapper().readTree("{\"injected\": true }");
      final ConfigWithSecretReferences configWithRefsAfterInjection = new ConfigWithSecretReferences(configAfterInjection, Map.of());
      final JobCheckConnectionConfig jobCheckConnectionConfig = new JobCheckConnectionConfig()
          .withActorType(ActorType.SOURCE)
          .withActorId(SOURCE_CONNECTION.getSourceId())
          .withConnectionConfiguration(configWithRefsAfterInjection)
          .withDockerImage(DOCKER_IMAGE)
          .withProtocolVersion(PROTOCOL_VERSION).withIsCustomConnector(false);

      when(configInjector.injectConfig(SOURCE_CONNECTION.getConfiguration(), SOURCE_CONNECTION.getSourceDefinitionId()))
          .thenReturn(configAfterInjection);
      when(
          secretReferenceService.getConfigWithSecretReferences(SOURCE_CONNECTION.getSourceId(), configAfterInjection,
              SOURCE_CONNECTION.getWorkspaceId()))
                  .thenReturn(configWithRefsAfterInjection);

      final StandardCheckConnectionOutput mockOutput = mock(StandardCheckConnectionOutput.class);
      final ConnectorJobOutput jobOutput = new ConnectorJobOutput().withCheckConnection(mockOutput);
      when(temporalClient.submitCheckConnection(any(UUID.class), eq(0), eq(WORKSPACE_ID), eq(CHECK_TASK_QUEUE), eq(jobCheckConnectionConfig),
          any(ActorContext.class)))
              .thenReturn(new TemporalResponse<>(jobOutput, createMetadata(true)));
      final SynchronousResponse<StandardCheckConnectionOutput> response =
          schedulerClient.createSourceCheckConnectionJob(SOURCE_CONNECTION, ACTOR_DEFINITION_VERSION, false, null);
      assertEquals(mockOutput, response.getOutput());
    }

    @Test
    void testCreateDestinationCheckConnectionJob() throws IOException {
      final JobCheckConnectionConfig jobCheckConnectionConfig = new JobCheckConnectionConfig()
          .withActorType(ActorType.DESTINATION)
          .withActorId(DESTINATION_CONNECTION.getDestinationId())
          .withConnectionConfiguration(CONFIG_WITH_REFS)
          .withDockerImage(DOCKER_IMAGE)
          .withProtocolVersion(PROTOCOL_VERSION)
          .withIsCustomConnector(false);

      final StandardCheckConnectionOutput mockOutput = mock(StandardCheckConnectionOutput.class);
      final ConnectorJobOutput jobOutput = new ConnectorJobOutput().withCheckConnection(mockOutput);
      when(temporalClient.submitCheckConnection(any(UUID.class), eq(0), eq(WORKSPACE_ID), eq(CHECK_TASK_QUEUE), eq(jobCheckConnectionConfig), any(
          ActorContext.class)))
              .thenReturn(new TemporalResponse<>(jobOutput, createMetadata(true)));
      final SynchronousResponse<StandardCheckConnectionOutput> response =
          schedulerClient.createDestinationCheckConnectionJob(DESTINATION_CONNECTION, ACTOR_DEFINITION_VERSION, false, null);
      assertEquals(mockOutput, response.getOutput());
      verify(configInjector).injectConfig(any(), eq(DESTINATION_CONNECTION.getDestinationDefinitionId()));
    }

    @Test
    void testCreateDiscoverSchemaJob() throws IOException {
      final UUID expectedCatalogId = UUID.randomUUID();
      final ConnectorJobOutput jobOutput = new ConnectorJobOutput().withDiscoverCatalogId(expectedCatalogId);
      when(
          temporalClient.submitDiscoverSchema(any(UUID.class), eq(0), eq(WORKSPACE_ID), eq(DISCOVER_TASK_QUEUE), any(JobDiscoverCatalogConfig.class),
              any(ActorContext.class), any()))
                  .thenReturn(new TemporalResponse<>(jobOutput, createMetadata(true)));
      final SynchronousResponse<UUID> response =
          schedulerClient.createDiscoverSchemaJob(SOURCE_CONNECTION, ACTOR_DEFINITION_VERSION, false, null, WorkloadPriority.HIGH);
      assertEquals(expectedCatalogId, response.getOutput());
      verify(configInjector).injectConfig(any(), eq(SOURCE_CONNECTION.getSourceDefinitionId()));
    }

    @Test
    void testCreateGetSpecJob() throws IOException {
      final JobGetSpecConfig jobSpecConfig = new JobGetSpecConfig().withDockerImage(DOCKER_IMAGE).withIsCustomConnector(false);

      final ConnectorSpecification mockOutput = mock(ConnectorSpecification.class);
      final ConnectorJobOutput jobOutput = new ConnectorJobOutput().withSpec(mockOutput);
      when(temporalClient.submitGetSpec(any(UUID.class), eq(0), eq(WORKSPACE_ID), eq(jobSpecConfig)))
          .thenReturn(new TemporalResponse<>(jobOutput, createMetadata(true)));
      final SynchronousResponse<ConnectorSpecification> response = schedulerClient.createGetSpecJob(DOCKER_IMAGE, false, WORKSPACE_ID);
      assertEquals(mockOutput, response.getOutput());
      verify(configInjector, never()).injectConfig(any(), any());
    }

  }

}
