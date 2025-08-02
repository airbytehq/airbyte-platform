/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.scheduler

import com.fasterxml.jackson.databind.ObjectMapper
import com.google.common.collect.ImmutableMap
import io.airbyte.commons.json.Jsons.jsonNode
import io.airbyte.commons.server.handlers.helpers.ContextBuilder
import io.airbyte.commons.temporal.JobMetadata
import io.airbyte.commons.temporal.TemporalClient
import io.airbyte.commons.temporal.TemporalResponse
import io.airbyte.commons.temporal.TemporalTaskQueueUtils.DEFAULT_CHECK_TASK_QUEUE
import io.airbyte.commons.temporal.TemporalTaskQueueUtils.DEFAULT_DISCOVER_TASK_QUEUE
import io.airbyte.commons.version.Version
import io.airbyte.config.ActorContext
import io.airbyte.config.ActorDefinitionVersion
import io.airbyte.config.ActorType
import io.airbyte.config.ConnectorJobOutput
import io.airbyte.config.DestinationConnection
import io.airbyte.config.FailureReason
import io.airbyte.config.JobCheckConnectionConfig
import io.airbyte.config.JobConfig.ConfigType
import io.airbyte.config.JobGetSpecConfig
import io.airbyte.config.ReleaseStage
import io.airbyte.config.SourceConnection
import io.airbyte.config.StandardCheckConnectionOutput
import io.airbyte.config.StandardDestinationDefinition
import io.airbyte.config.WorkloadPriority
import io.airbyte.config.persistence.ConfigInjector
import io.airbyte.config.secrets.ConfigWithSecretReferences
import io.airbyte.config.secrets.SecretReferenceConfig
import io.airbyte.domain.models.ActorId
import io.airbyte.domain.models.WorkspaceId
import io.airbyte.domain.services.secrets.SecretReferenceService
import io.airbyte.persistence.job.errorreporter.ConnectorJobReportingContext
import io.airbyte.persistence.job.errorreporter.JobErrorReporter
import io.airbyte.persistence.job.factory.OAuthConfigSupplier
import io.airbyte.persistence.job.tracker.JobTracker
import io.airbyte.protocol.models.v0.ConnectorSpecification
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import java.io.IOException
import java.nio.file.Path
import java.util.UUID
import java.util.function.Function
import java.util.function.Supplier

// the goal here is to test the "execute" part of this class and all of the various exceptional
// cases. then separately test submission of each job type without having to re-test all of the
// execution exception cases again.
internal class DefaultSynchronousSchedulerClientTest {
  private lateinit var temporalClient: TemporalClient
  private lateinit var jobTracker: JobTracker
  private lateinit var jobErrorReporter: JobErrorReporter
  private lateinit var oAuthConfigSupplier: OAuthConfigSupplier
  private lateinit var configInjector: ConfigInjector
  private lateinit var secretReferenceService: SecretReferenceService
  private lateinit var schedulerClient: DefaultSynchronousSchedulerClient
  private lateinit var contextBuilder: ContextBuilder

  @BeforeEach
  fun setup() {
    temporalClient = mockk<TemporalClient>(relaxed = true)
    jobTracker = mockk<JobTracker>(relaxed = true)
    jobErrorReporter = mockk<JobErrorReporter>(relaxed = true)
    oAuthConfigSupplier = mockk<OAuthConfigSupplier>(relaxed = true)
    configInjector = mockk<ConfigInjector>(relaxed = true)
    contextBuilder = mockk<ContextBuilder>(relaxed = true)
    secretReferenceService = mockk<SecretReferenceService>(relaxed = true)

    schedulerClient =
      DefaultSynchronousSchedulerClient(
        temporalClient,
        jobTracker,
        jobErrorReporter,
        oAuthConfigSupplier,
        configInjector,
        contextBuilder,
        secretReferenceService,
      )

    every {
      oAuthConfigSupplier.injectSourceOAuthParameters(
        any(),
        any(),
        any(),
        CONFIGURATION,
      )
    } returns CONFIGURATION
    every {
      oAuthConfigSupplier.injectDestinationOAuthParameters(
        any(),
        any(),
        any(),
        CONFIGURATION,
      )
    } returns CONFIGURATION

    every { configInjector.injectConfig(any(), any()) } answers { firstArg() }

    every { secretReferenceService.getConfigWithSecretReferences(any(), any(), any()) } answers {
      ConfigWithSecretReferences(secondArg(), emptyMap())
    }

    every { contextBuilder.fromDestination(any()) } returns ActorContext()
    every { contextBuilder.fromSource(any()) } returns ActorContext()
  }

  @Nested
  @DisplayName("Test execute method.")
  internal inner class ExecuteSynchronousJob {
    @Test
    fun testExecuteJobSuccess() {
      val sourceDefinitionId = UUID.randomUUID()
      val discoveredCatalogId = UUID.randomUUID()
      val function: Supplier<TemporalResponse<ConnectorJobOutput>> = mockk()
      val mapperFunction: Function<ConnectorJobOutput, UUID?> = Function { obj: ConnectorJobOutput -> obj.discoverCatalogId }
      val jobOutput = ConnectorJobOutput().withDiscoverCatalogId(discoveredCatalogId)
      every { function.get() } returns TemporalResponse(jobOutput, createMetadata(true))

      val jobContext =
        ConnectorJobReportingContext(UUID.randomUUID(), SOURCE_DOCKER_IMAGE, SOURCE_RELEASE_STAGE, SOURCE_INTERNAL_SUPPORT_LEVEL)
      val response =
        schedulerClient
          .execute<UUID?>(
            ConfigType.DISCOVER_SCHEMA,
            jobContext,
            sourceDefinitionId,
            function,
            mapperFunction,
            WORKSPACE_ID,
            ACTOR_ID,
            ActorType.SOURCE,
          )

      Assertions.assertNotNull(response)
      Assertions.assertEquals(discoveredCatalogId, response.output)
      Assertions.assertEquals(ConfigType.DISCOVER_SCHEMA, response.metadata.configType)
      Assertions.assertNotNull(response.metadata.getConfigId())
      Assertions.assertEquals(sourceDefinitionId, response.metadata.getConfigId().get())
      Assertions.assertTrue(response.metadata.isSucceeded)
      Assertions.assertEquals(LOG_PATH, response.metadata.logPath)

      verify {
        jobTracker.trackDiscover(
          any(),
          sourceDefinitionId,
          WORKSPACE_ID,
          ACTOR_ID,
          ActorType.SOURCE,
          JobTracker.JobState.STARTED,
          null,
        )
      }

      verify {
        jobTracker.trackDiscover(
          any(),
          sourceDefinitionId,
          WORKSPACE_ID,
          ACTOR_ID,
          ActorType.SOURCE,
          JobTracker.JobState.SUCCEEDED,
          jobOutput,
        )
      }
    }

    @Test
    fun testExecuteJobFailure() {
      val sourceDefinitionId = UUID.randomUUID()
      val function: Supplier<TemporalResponse<ConnectorJobOutput>> = mockk()
      val mapperFunction: Function<ConnectorJobOutput, UUID?> = Function { obj: ConnectorJobOutput? -> obj!!.discoverCatalogId }
      val failureReason = FailureReason()
      val failedJobOutput = ConnectorJobOutput().withFailureReason(failureReason)
      every { function.get() } returns TemporalResponse(failedJobOutput, createMetadata(false))

      val jobContext =
        ConnectorJobReportingContext(UUID.randomUUID(), SOURCE_DOCKER_IMAGE, SOURCE_RELEASE_STAGE, SOURCE_INTERNAL_SUPPORT_LEVEL)
      val response =
        schedulerClient
          .execute<UUID?>(
            ConfigType.DISCOVER_SCHEMA,
            jobContext,
            sourceDefinitionId,
            function,
            mapperFunction,
            WORKSPACE_ID,
            ACTOR_ID,
            ActorType.SOURCE,
          )

      Assertions.assertNotNull(response)
      Assertions.assertNull(response.output)
      Assertions.assertEquals(ConfigType.DISCOVER_SCHEMA, response.metadata.configType)
      Assertions.assertNotNull(response.metadata.getConfigId())
      Assertions.assertEquals(sourceDefinitionId, response.metadata.getConfigId().get())
      Assertions.assertFalse(response.metadata.isSucceeded)
      Assertions.assertEquals(LOG_PATH, response.metadata.logPath)

      verify {
        jobTracker.trackDiscover(
          any(),
          sourceDefinitionId,
          WORKSPACE_ID,
          ACTOR_ID,
          ActorType.SOURCE,
          JobTracker.JobState.STARTED,
          null,
        )
      }

      verify {
        jobTracker.trackDiscover(
          any(),
          sourceDefinitionId,
          WORKSPACE_ID,
          ACTOR_ID,
          ActorType.SOURCE,
          JobTracker.JobState.FAILED,
          failedJobOutput,
        )
      }
    }

    @Test
    fun testExecuteRuntimeException() {
      val sourceDefinitionId = UUID.randomUUID()
      val function: Supplier<TemporalResponse<ConnectorJobOutput>> = mockk()
      val mapperFunction: Function<ConnectorJobOutput, UUID?> = Function { obj: ConnectorJobOutput? -> obj!!.discoverCatalogId }
      every { function.get() } throws RuntimeException()
      val jobContext =
        ConnectorJobReportingContext(UUID.randomUUID(), SOURCE_DOCKER_IMAGE, SOURCE_RELEASE_STAGE, SOURCE_INTERNAL_SUPPORT_LEVEL)
      Assertions.assertThrows(RuntimeException::class.java) {
        schedulerClient.execute(
          ConfigType.DISCOVER_SCHEMA,
          jobContext,
          sourceDefinitionId,
          function,
          mapperFunction,
          WORKSPACE_ID,
          ACTOR_ID,
          ActorType.SOURCE,
        )
      }

      verify {
        jobTracker.trackDiscover(
          any(),
          sourceDefinitionId,
          WORKSPACE_ID,
          ACTOR_ID,
          ActorType.SOURCE,
          JobTracker.JobState.STARTED,
          null,
        )
      }

      verify {
        jobTracker.trackDiscover(
          any(),
          sourceDefinitionId,
          WORKSPACE_ID,
          ACTOR_ID,
          ActorType.SOURCE,
          JobTracker.JobState.FAILED,
          null,
        )
      }
    }
  }

  @Nested
  @DisplayName("Test job creation for each configuration type.")
  internal inner class TestJobCreation {
    @Test
    @Throws(IOException::class)
    fun testCreateSourceCheckConnectionJob() {
      val jobCheckConnectionConfig =
        JobCheckConnectionConfig()
          .withActorType(ActorType.SOURCE)
          .withActorId(SOURCE_CONNECTION.sourceId)
          .withConnectionConfiguration(CONFIG_WITH_REFS)
          .withDockerImage(DOCKER_IMAGE)
          .withProtocolVersion(PROTOCOL_VERSION)
          .withIsCustomConnector(false)

      val mockOutput = mockk<StandardCheckConnectionOutput>()
      val jobOutput = ConnectorJobOutput().withCheckConnection(mockOutput)
      every {
        temporalClient.submitCheckConnection(
          any(),
          0,
          WORKSPACE_ID,
          CHECK_TASK_QUEUE,
          jobCheckConnectionConfig,
          any(),
        )
      } returns TemporalResponse(jobOutput, createMetadata(true))
      val response: SynchronousResponse<StandardCheckConnectionOutput> =
        schedulerClient.createSourceCheckConnectionJob(SOURCE_CONNECTION, ACTOR_DEFINITION_VERSION, false, null)
      Assertions.assertEquals(mockOutput, response.output)
      verify {
        configInjector.injectConfig(
          any(),
          SOURCE_CONNECTION.sourceDefinitionId,
        )
      }
    }

    @Test
    @Throws(IOException::class)
    fun testCreateSourceCheckConnectionJobWithConfigInjection() {
      val configAfterInjection = ObjectMapper().readTree("{\"injected\": true }")
      val configWithRefsAfterInjection = ConfigWithSecretReferences(configAfterInjection, mapOf<String, SecretReferenceConfig>())
      val jobCheckConnectionConfig =
        JobCheckConnectionConfig()
          .withActorType(ActorType.SOURCE)
          .withActorId(SOURCE_CONNECTION.sourceId)
          .withConnectionConfiguration(configWithRefsAfterInjection)
          .withDockerImage(DOCKER_IMAGE)
          .withProtocolVersion(PROTOCOL_VERSION)
          .withIsCustomConnector(false)

      every {
        configInjector.injectConfig(SOURCE_CONNECTION.configuration, SOURCE_CONNECTION.sourceDefinitionId)
      } returns configAfterInjection
      every {
        secretReferenceService.getConfigWithSecretReferences(
          ActorId(SOURCE_CONNECTION.sourceId),
          configAfterInjection,
          WorkspaceId(SOURCE_CONNECTION.workspaceId),
        )
      } returns configWithRefsAfterInjection

      val mockOutput = mockk<StandardCheckConnectionOutput>()
      val jobOutput = ConnectorJobOutput().withCheckConnection(mockOutput)
      every {
        temporalClient.submitCheckConnection(
          any(),
          0,
          WORKSPACE_ID,
          CHECK_TASK_QUEUE,
          jobCheckConnectionConfig,
          any(),
        )
      } returns TemporalResponse(jobOutput, createMetadata(true))
      val response: SynchronousResponse<StandardCheckConnectionOutput> =
        schedulerClient.createSourceCheckConnectionJob(SOURCE_CONNECTION, ACTOR_DEFINITION_VERSION, false, null)
      Assertions.assertEquals(mockOutput, response.output)
    }

    @Test
    @Throws(IOException::class)
    fun testCreateDestinationCheckConnectionJob() {
      val jobCheckConnectionConfig =
        JobCheckConnectionConfig()
          .withActorType(ActorType.DESTINATION)
          .withActorId(DESTINATION_CONNECTION.destinationId)
          .withConnectionConfiguration(CONFIG_WITH_REFS)
          .withDockerImage(DOCKER_IMAGE)
          .withProtocolVersion(PROTOCOL_VERSION)
          .withIsCustomConnector(false)

      val mockOutput = mockk<StandardCheckConnectionOutput>()
      val jobOutput = ConnectorJobOutput().withCheckConnection(mockOutput)
      every {
        temporalClient.submitCheckConnection(
          any(),
          0,
          WORKSPACE_ID,
          CHECK_TASK_QUEUE,
          jobCheckConnectionConfig,
          any(),
        )
      } returns TemporalResponse(jobOutput, createMetadata(true))
      val response: SynchronousResponse<StandardCheckConnectionOutput> =
        schedulerClient.createDestinationCheckConnectionJob(DESTINATION_CONNECTION, ACTOR_DEFINITION_VERSION, false, null)
      Assertions.assertEquals(mockOutput, response.output)
      verify {
        configInjector.injectConfig(
          any(),
          DESTINATION_CONNECTION.destinationDefinitionId,
        )
      }
    }

    @Test
    @Throws(IOException::class)
    fun testCreateDiscoverSchemaJob() {
      val expectedCatalogId = UUID.randomUUID()
      val jobOutput = ConnectorJobOutput().withDiscoverCatalogId(expectedCatalogId)
      every {
        temporalClient.submitDiscoverSchema(
          any(),
          0,
          WORKSPACE_ID,
          DISCOVER_TASK_QUEUE,
          any(),
          any(),
          any(),
        )
      } returns TemporalResponse(jobOutput, createMetadata(true))
      val response: SynchronousResponse<UUID> =
        schedulerClient.createDiscoverSchemaJob(SOURCE_CONNECTION, ACTOR_DEFINITION_VERSION, false, null, WorkloadPriority.HIGH)
      Assertions.assertEquals(expectedCatalogId, response.output)
      verify {
        configInjector.injectConfig(
          any(),
          SOURCE_CONNECTION.sourceDefinitionId,
        )
      }
    }

    @Test
    @Throws(IOException::class)
    fun testCreateDestinationDiscoverJob() {
      val expectedCatalogId = UUID.randomUUID()
      val jobOutput = ConnectorJobOutput().withDiscoverCatalogId(expectedCatalogId)
      every {
        temporalClient.submitDiscoverSchema(
          any(),
          0,
          WORKSPACE_ID,
          DISCOVER_TASK_QUEUE,
          any(),
          any(),
          any(),
        )
      } returns TemporalResponse(jobOutput, createMetadata(true))
      val response: SynchronousResponse<UUID> =
        schedulerClient.createDestinationDiscoverJob(DESTINATION_CONNECTION, DESTINATION_DEFINITION, ACTOR_DEFINITION_VERSION)
      Assertions.assertEquals(expectedCatalogId, response.output)
    }

    @Test
    @Throws(IOException::class)
    fun testCreateGetSpecJob() {
      val jobSpecConfig = JobGetSpecConfig().withDockerImage(DOCKER_IMAGE).withIsCustomConnector(false)

      val mockOutput: ConnectorSpecification = mockk<ConnectorSpecification>()
      val jobOutput = ConnectorJobOutput().withSpec(mockOutput)
      every {
        temporalClient.submitGetSpec(
          any(),
          0,
          WORKSPACE_ID,
          jobSpecConfig,
        )
      } returns TemporalResponse(jobOutput, createMetadata(true))
      val response: SynchronousResponse<ConnectorSpecification> = schedulerClient.createGetSpecJob(DOCKER_IMAGE, false, WORKSPACE_ID)
      Assertions.assertEquals(mockOutput, response.output)
      verify(exactly = 0) {
        configInjector.injectConfig(
          any(),
          any(),
        )
      }
    }
  }

  companion object {
    private val LOG_PATH: Path = Path.of("/tmp")
    private const val DOCKER_REPOSITORY = "airbyte/source-foo"
    private const val DOCKER_IMAGE_TAG = "1.2.3"
    private val DOCKER_IMAGE: String = DOCKER_REPOSITORY + ":" + DOCKER_IMAGE_TAG
    private val PROTOCOL_VERSION = Version("0.2.3")
    private val WORKSPACE_ID: UUID = UUID.randomUUID()
    private val ACTOR_ID: UUID = UUID.randomUUID()
    private val UUID1: UUID = UUID.randomUUID()
    private val UUID2: UUID = UUID.randomUUID()
    private const val UNCHECKED = "unchecked"
    private val CHECK_TASK_QUEUE = DEFAULT_CHECK_TASK_QUEUE
    private val DISCOVER_TASK_QUEUE = DEFAULT_DISCOVER_TASK_QUEUE
    private val CONFIGURATION =
      jsonNode(
        ImmutableMap
          .builder<Any, Any>()
          .put("username", "airbyte")
          .put("password", "abc")
          .build(),
      )

    private val CONFIG_WITH_REFS = ConfigWithSecretReferences(CONFIGURATION, mapOf<String, SecretReferenceConfig>())
    private val SOURCE_CONNECTION: SourceConnection =
      SourceConnection()
        .withWorkspaceId(WORKSPACE_ID)
        .withSourceId(UUID1)
        .withSourceDefinitionId(UUID2)
        .withConfiguration(CONFIGURATION)
    private val DESTINATION_CONNECTION: DestinationConnection =
      DestinationConnection()
        .withWorkspaceId(WORKSPACE_ID)
        .withDestinationId(UUID1)
        .withDestinationDefinitionId(UUID2)
        .withConfiguration(CONFIGURATION)
    private val DESTINATION_DEFINITION: StandardDestinationDefinition =
      StandardDestinationDefinition()
        .withDestinationDefinitionId(UUID2)
        .withCustom(false)
    private val ACTOR_DEFINITION_VERSION: ActorDefinitionVersion =
      ActorDefinitionVersion()
        .withDockerRepository(DOCKER_REPOSITORY)
        .withDockerImageTag(DOCKER_IMAGE_TAG)
        .withProtocolVersion(PROTOCOL_VERSION.serialize())
        .withReleaseStage(ReleaseStage.BETA)
    private const val SOURCE_DOCKER_IMAGE = "source-airbyte:1.2.3"
    private val SOURCE_RELEASE_STAGE = ReleaseStage.BETA
    private val SOURCE_INTERNAL_SUPPORT_LEVEL: Long? = null

    private fun createMetadata(succeeded: Boolean): JobMetadata =
      JobMetadata(
        succeeded,
        LOG_PATH,
      )
  }
}
