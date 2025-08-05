/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.api.model.generated.ConnectionIdRequestBody
import io.airbyte.api.model.generated.ConnectionState
import io.airbyte.api.model.generated.ConnectionStateType
import io.airbyte.api.model.generated.SaveAttemptSyncConfigRequestBody
import io.airbyte.api.model.generated.SyncInput
import io.airbyte.commons.constants.WorkerConstants
import io.airbyte.commons.json.Jsons.emptyObject
import io.airbyte.commons.json.Jsons.jsonNode
import io.airbyte.commons.server.converters.ApiPojoConverters
import io.airbyte.commons.server.handlers.helpers.CatalogConverter
import io.airbyte.commons.server.handlers.helpers.ContextBuilder
import io.airbyte.config.ActorDefinitionVersion
import io.airbyte.config.AttemptSyncConfig
import io.airbyte.config.ConfiguredAirbyteCatalog
import io.airbyte.config.ConfiguredAirbyteStream
import io.airbyte.config.ConnectionContext
import io.airbyte.config.DestinationConnection
import io.airbyte.config.Job
import io.airbyte.config.JobConfig
import io.airbyte.config.JobResetConnectionConfig
import io.airbyte.config.JobStatus
import io.airbyte.config.JobSyncConfig
import io.airbyte.config.MapperConfig
import io.airbyte.config.SourceConnection
import io.airbyte.config.StandardDestinationDefinition
import io.airbyte.config.StandardSourceDefinition
import io.airbyte.config.StandardSync
import io.airbyte.config.StandardSyncInput
import io.airbyte.config.State
import io.airbyte.config.helpers.FieldGenerator
import io.airbyte.config.persistence.ActorDefinitionVersionHelper
import io.airbyte.config.persistence.ConfigInjector
import io.airbyte.config.secrets.ConfigWithSecretReferences
import io.airbyte.config.secrets.SecretCoordinate.AirbyteManagedSecretCoordinate
import io.airbyte.config.secrets.SecretReferenceConfig
import io.airbyte.config.secrets.toInlined
import io.airbyte.data.ConfigNotFoundException
import io.airbyte.data.services.ConnectionService
import io.airbyte.data.services.DestinationService
import io.airbyte.data.services.ScopedConfigurationService
import io.airbyte.data.services.SourceService
import io.airbyte.domain.models.ActorId
import io.airbyte.domain.models.WorkspaceId
import io.airbyte.domain.services.secrets.SecretReferenceService
import io.airbyte.featureflag.TestClient
import io.airbyte.mappers.transformations.Mapper
import io.airbyte.persistence.job.JobPersistence
import io.airbyte.persistence.job.factory.OAuthConfigSupplier
import io.airbyte.persistence.job.models.IntegrationLauncherConfig
import io.airbyte.persistence.job.models.JobRunConfig
import io.airbyte.validation.json.JsonValidationException
import io.airbyte.workers.models.JobInput
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.io.IOException
import java.util.List
import java.util.Map
import java.util.UUID

/**
 * Test the JobInputHandler.
 */
internal class JobInputHandlerTest {
  private lateinit var attemptHandler: AttemptHandler
  private lateinit var stateHandler: StateHandler
  private lateinit var jobInputHandler: JobInputHandler
  private lateinit var contextBuilder: ContextBuilder
  private lateinit var sourceService: SourceService
  private lateinit var destinatinonService: DestinationService
  private lateinit var connectionService: ConnectionService
  private lateinit var scopedConfigurationService: ScopedConfigurationService
  private lateinit var secretReferenceService: SecretReferenceService
  private lateinit var jobPersistence: JobPersistence
  private lateinit var configInjector: ConfigInjector
  private lateinit var oAuthConfigSupplier: OAuthConfigSupplier

  private lateinit var actorDefinitionVersionHelper: ActorDefinitionVersionHelper

  private val apiPojoConverters = ApiPojoConverters(CatalogConverter(FieldGenerator(), mutableListOf<Mapper<out MapperConfig>>()))

  @BeforeEach
  @Throws(IOException::class, JsonValidationException::class, ConfigNotFoundException::class)
  fun init() {
    jobPersistence = mockk()
    configInjector = mockk()
    oAuthConfigSupplier = mockk()
    attemptHandler = mockk()
    stateHandler = mockk()
    actorDefinitionVersionHelper = mockk()
    contextBuilder = mockk()
    sourceService = mockk()
    destinatinonService = mockk()
    connectionService = mockk()
    scopedConfigurationService = mockk()
    secretReferenceService = mockk()
    every { scopedConfigurationService.getScopedConfigurations(any(), any()) } returns emptyList()
    every { attemptHandler.saveSyncConfig(any()) } returns mockk()

    jobInputHandler =
      JobInputHandler(
        jobPersistence,
        TestClient(mapOf()),
        oAuthConfigSupplier,
        configInjector,
        attemptHandler,
        stateHandler,
        actorDefinitionVersionHelper,
        contextBuilder,
        connectionService,
        sourceService,
        destinatinonService,
        apiPojoConverters,
        scopedConfigurationService,
        secretReferenceService,
      )

    every { configInjector.injectConfig(any(), any()) } answers { firstArg() }

    every { secretReferenceService.getConfigWithSecretReferences(any(), any(), any()) } answers {
      ConfigWithSecretReferences(secondArg(), emptyMap())
    }

    val destinationConnection =
      DestinationConnection()
        .withDestinationId(DESTINATION_ID)
        .withWorkspaceId(WORKSPACE_ID)
        .withDestinationDefinitionId(DESTINATION_DEFINITION_ID)
        .withConfiguration(DESTINATION_CONFIGURATION)

    every { destinatinonService.getDestinationConnection(DESTINATION_ID) } returns destinationConnection
    val destinationDefinition =
      mockk<StandardDestinationDefinition> {
        every { resourceRequirements } returns null
      }
    every { destinatinonService.getStandardDestinationDefinition(DESTINATION_DEFINITION_ID) } returns destinationDefinition
    every { actorDefinitionVersionHelper.getDestinationVersion(destinationDefinition, WORKSPACE_ID, DESTINATION_ID) } returns
      mockk<ActorDefinitionVersion> {
        every { allowedHosts } returns null
      }

    val sourceDefinition =
      mockk<StandardSourceDefinition> {
        every { resourceRequirements } returns null
      }
    every { sourceService.getStandardSourceDefinition(any()) } returns sourceDefinition
    every { actorDefinitionVersionHelper.getSourceVersion(any<StandardSourceDefinition>(), any(), any()) } returns
      mockk<ActorDefinitionVersion> {
        every { allowedHosts } returns null
      }

    every {
      oAuthConfigSupplier.injectDestinationOAuthParameters(
        DESTINATION_DEFINITION_ID,
        DESTINATION_ID,
        WORKSPACE_ID,
        DESTINATION_CONFIGURATION,
      )
    } returns DESTINATION_CONFIG_WITH_OAUTH

    val standardSync =
      StandardSync()
        .withSourceId(SOURCE_ID)
        .withDestinationId(DESTINATION_ID)
    every { connectionService.getStandardSync(CONNECTION_ID) } returns standardSync

    every { contextBuilder.fromConnectionId(any()) } returns ConnectionContext()
  }

  @Test
  @Throws(JsonValidationException::class, ConfigNotFoundException::class, IOException::class)
  fun testGetSyncWorkflowInput() {
    val syncInput = SyncInput().jobId(JOB_ID).attemptNumber(ATTEMPT_NUMBER)

    val sourceDefinitionId = UUID.randomUUID()
    val sourceConnection =
      SourceConnection()
        .withSourceId(SOURCE_ID)
        .withSourceDefinitionId(sourceDefinitionId)
        .withWorkspaceId(WORKSPACE_ID)
        .withConfiguration(SOURCE_CONFIGURATION)
    every { sourceService.getSourceConnection(SOURCE_ID) } returns sourceConnection
    every {
      oAuthConfigSupplier.injectSourceOAuthParameters(
        sourceDefinitionId,
        SOURCE_ID,
        WORKSPACE_ID,
        SOURCE_CONFIGURATION,
      )
    } returns SOURCE_CONFIG_WITH_OAUTH
    every { configInjector.injectConfig(SOURCE_CONFIG_WITH_OAUTH, sourceDefinitionId) } returns SOURCE_CONFIG_WITH_OAUTH_AND_INJECTED_CONFIG
    every {
      secretReferenceService.getConfigWithSecretReferences(
        ActorId(SOURCE_ID),
        SOURCE_CONFIG_WITH_OAUTH_AND_INJECTED_CONFIG,
        WorkspaceId(WORKSPACE_ID),
      )
    } returns SOURCE_CONFIG_WITH_REFS
    every {
      secretReferenceService.getConfigWithSecretReferences(
        ActorId(DESTINATION_ID),
        DESTINATION_CONFIG_WITH_OAUTH,
        WorkspaceId(WORKSPACE_ID),
      )
    } returns DESTINATION_CONFIG_WITH_REFS

    every { sourceService.getStandardSourceDefinition(sourceDefinitionId) } returns
      mockk<StandardSourceDefinition> {
        every { resourceRequirements } returns null
      }

    every {
      actorDefinitionVersionHelper.getSourceVersion(
        any<StandardSourceDefinition>(),
        any<UUID>(),
        any<UUID>(),
      )
    } returns
      mockk<ActorDefinitionVersion> {
        every { allowedHosts } returns null
      }

    every { stateHandler.getState(ConnectionIdRequestBody().connectionId(CONNECTION_ID)) } returns
      ConnectionState()
        .stateType(ConnectionStateType.LEGACY)
        .state(STATE.getState())
        .connectionId(CONNECTION_ID)

    val jobSyncConfig: JobSyncConfig =
      JobSyncConfig()
        .withWorkspaceId(WORKSPACE_ID)
        .withDestinationDockerImage("destinationDockerImage")
        .withSourceDockerImage("sourceDockerImage")
        .withConfiguredAirbyteCatalog(
          mockk<ConfiguredAirbyteCatalog> {
            every { streams } returns emptyList()
          },
        )

    val jobConfig =
      JobConfig()
        .withConfigType(JobConfig.ConfigType.SYNC)
        .withSync(jobSyncConfig)

    val job =
      Job(
        JOB_ID,
        JobConfig.ConfigType.RESET_CONNECTION,
        CONNECTION_ID.toString(),
        jobConfig,
        mutableListOf(),
        JobStatus.PENDING,
        1001L,
        1000L,
        1002L,
        true,
      )
    every { jobPersistence.getJob(JOB_ID) } returns job

    val expectedStandardSyncInput =
      StandardSyncInput()
        .withConnectionId(CONNECTION_ID)
        .withWorkspaceId(jobSyncConfig.getWorkspaceId())
        .withSourceId(SOURCE_ID)
        .withDestinationId(DESTINATION_ID)
        .withSourceConfiguration(INLINED_SOURCE_CONFIG_WITH_REFS)
        .withDestinationConfiguration(INLINED_DESTINATION_CONFIG_WITH_REFS)
        .withIsReset(false)
        .withUseAsyncReplicate(true)
        .withUseAsyncActivities(true)
        .withIncludesFiles(false)
        .withConnectionContext(ConnectionContext())

    val expectedJobRunConfig =
      JobRunConfig()
        .withJobId(JOB_ID.toString())
        .withAttemptId(ATTEMPT_NUMBER.toLong())

    val expectedSourceLauncherConfig =
      IntegrationLauncherConfig()
        .withJobId(JOB_ID.toString())
        .withAttemptId(ATTEMPT_NUMBER.toLong())
        .withConnectionId(CONNECTION_ID)
        .withWorkspaceId(WORKSPACE_ID)
        .withDockerImage(jobSyncConfig.getSourceDockerImage())

    val expectedDestinationLauncherConfig =
      IntegrationLauncherConfig()
        .withJobId(JOB_ID.toString())
        .withAttemptId(ATTEMPT_NUMBER.toLong())
        .withConnectionId(CONNECTION_ID)
        .withWorkspaceId(jobSyncConfig.getWorkspaceId())
        .withDockerImage(jobSyncConfig.getDestinationDockerImage())
        .withAdditionalEnvironmentVariables(mutableMapOf<String?, String?>())

    val expectedJobInput =
      JobInput(
        expectedJobRunConfig,
        expectedSourceLauncherConfig,
        expectedDestinationLauncherConfig,
        expectedStandardSyncInput,
      )

    val generatedJobInput = jobInputHandler.getJobInput(syncInput)
    Assertions.assertEquals(expectedJobInput, generatedJobInput)

    val expectedAttemptSyncConfig =
      AttemptSyncConfig()
        .withSourceConfiguration(INLINED_SOURCE_CONFIG_WITH_REFS)
        .withDestinationConfiguration(INLINED_DESTINATION_CONFIG_WITH_REFS)
        .withState(STATE)

    verify { oAuthConfigSupplier.injectSourceOAuthParameters(sourceDefinitionId, SOURCE_ID, WORKSPACE_ID, SOURCE_CONFIGURATION) }
    verify {
      oAuthConfigSupplier.injectDestinationOAuthParameters(
        DESTINATION_DEFINITION_ID,
        DESTINATION_ID,
        WORKSPACE_ID,
        DESTINATION_CONFIGURATION,
      )
    }
    verify { secretReferenceService.getConfigWithSecretReferences(ActorId(DESTINATION_ID), DESTINATION_CONFIG_WITH_OAUTH, WorkspaceId(WORKSPACE_ID)) }
    verify {
      secretReferenceService.getConfigWithSecretReferences(
        ActorId(SOURCE_ID),
        SOURCE_CONFIG_WITH_OAUTH_AND_INJECTED_CONFIG,
        WorkspaceId(WORKSPACE_ID),
      )
    }

    verify {
      attemptHandler.saveSyncConfig(
        SaveAttemptSyncConfigRequestBody()
          .jobId(JOB_ID)
          .attemptNumber(ATTEMPT_NUMBER)
          .syncConfig(apiPojoConverters.attemptSyncConfigToApi(expectedAttemptSyncConfig, CONNECTION_ID)),
      )
    }
  }

  @Test
  @Throws(IOException::class)
  fun testGetResetSyncWorkflowInput() {
    val syncInput = SyncInput().jobId(JOB_ID).attemptNumber(ATTEMPT_NUMBER)

    every { stateHandler.getState(ConnectionIdRequestBody().connectionId(CONNECTION_ID)) } returns
      ConnectionState()
        .stateType(ConnectionStateType.LEGACY)
        .state(STATE.getState())
        .connectionId(CONNECTION_ID)

    val jobResetConfig =
      JobResetConnectionConfig()
        .withWorkspaceId(WORKSPACE_ID)
        .withDestinationDockerImage("destinationDockerImage")
        .withConfiguredAirbyteCatalog(
          mockk<ConfiguredAirbyteCatalog> {
            every { streams } returns emptyList()
          },
        )

    val jobConfig =
      JobConfig()
        .withConfigType(JobConfig.ConfigType.RESET_CONNECTION)
        .withResetConnection(jobResetConfig)

    val job =
      Job(
        JOB_ID,
        JobConfig.ConfigType.RESET_CONNECTION,
        CONNECTION_ID.toString(),
        jobConfig,
        mutableListOf(),
        JobStatus.PENDING,
        1001L,
        1000L,
        1002L,
        true,
      )
    every { jobPersistence.getJob(JOB_ID) } returns job

    val expectedStandardSyncInput =
      StandardSyncInput()
        .withConnectionId(CONNECTION_ID)
        .withWorkspaceId(jobResetConfig.getWorkspaceId())
        .withSourceId(SOURCE_ID)
        .withDestinationId(DESTINATION_ID)
        .withSourceConfiguration(emptyObject())
        .withDestinationConfiguration(DESTINATION_CONFIG_WITH_OAUTH)
        .withWebhookOperationConfigs(jobResetConfig.getWebhookOperationConfigs())
        .withIsReset(true)
        .withUseAsyncReplicate(true)
        .withUseAsyncActivities(true)
        .withIncludesFiles(false)
        .withConnectionContext(ConnectionContext())

    val expectedJobRunConfig =
      JobRunConfig()
        .withJobId(JOB_ID.toString())
        .withAttemptId(ATTEMPT_NUMBER.toLong())

    val expectedSourceLauncherConfig =
      IntegrationLauncherConfig()
        .withJobId(JOB_ID.toString())
        .withAttemptId(ATTEMPT_NUMBER.toLong())
        .withConnectionId(CONNECTION_ID)
        .withWorkspaceId(jobResetConfig.getWorkspaceId())
        .withDockerImage(WorkerConstants.RESET_JOB_SOURCE_DOCKER_IMAGE_STUB)

    val expectedDestinationLauncherConfig =
      IntegrationLauncherConfig()
        .withJobId(JOB_ID.toString())
        .withAttemptId(ATTEMPT_NUMBER.toLong())
        .withConnectionId(CONNECTION_ID)
        .withWorkspaceId(jobResetConfig.getWorkspaceId())
        .withDockerImage(jobResetConfig.getDestinationDockerImage())
        .withAdditionalEnvironmentVariables(mutableMapOf<String?, String?>())

    val expectedJobInput =
      JobInput(
        expectedJobRunConfig,
        expectedSourceLauncherConfig,
        expectedDestinationLauncherConfig,
        expectedStandardSyncInput,
      )

    val generatedJobInput = jobInputHandler.getJobInput(syncInput)
    Assertions.assertEquals(expectedJobInput, generatedJobInput)

    val expectedAttemptSyncConfig =
      AttemptSyncConfig()
        .withSourceConfiguration(emptyObject())
        .withDestinationConfiguration(DESTINATION_CONFIG_WITH_OAUTH)
        .withState(STATE)

    verify {
      oAuthConfigSupplier.injectDestinationOAuthParameters(
        DESTINATION_DEFINITION_ID,
        DESTINATION_ID,
        WORKSPACE_ID,
        DESTINATION_CONFIGURATION,
      )
    }

    verify {
      attemptHandler.saveSyncConfig(
        SaveAttemptSyncConfigRequestBody()
          .jobId(JOB_ID)
          .attemptNumber(ATTEMPT_NUMBER)
          .syncConfig(apiPojoConverters.attemptSyncConfigToApi(expectedAttemptSyncConfig, CONNECTION_ID)),
      )
    }
  }

  @Test
  fun testIncludesFilesIsTrueIfConnectorsSupportFilesAndFileIsConfigured() {
    val streamWithFilesEnabled =
      mockk<ConfiguredAirbyteStream> {
        every { includeFiles } returns true
      }

    val sourceAdv =
      mockk<ActorDefinitionVersion> {
        every { allowedHosts } returns null
      }
    val destinationAdv =
      mockk<ActorDefinitionVersion> {
        every { allowedHosts } returns null
      }
    val jobSyncConfig =
      JobSyncConfig()
        .withConfiguredAirbyteCatalog(
          ConfiguredAirbyteCatalog().withStreams(
            List.of<ConfiguredAirbyteStream>(
              mockk<ConfiguredAirbyteStream> {
                every { includeFiles } returns false
              },
              streamWithFilesEnabled,
            ),
          ),
        )
    Assertions.assertTrue(jobInputHandler.shouldIncludeFiles(jobSyncConfig, sourceAdv, destinationAdv))
  }

  @Test
  fun testIncludesFilesIsFalseIfConnectorsSupportFilesAndFileIsNotConfigured() {
    val sourceAdv =
      mockk<ActorDefinitionVersion> {
        every { allowedHosts } returns null
      }
    val destinationAdv =
      mockk<ActorDefinitionVersion> {
        every { allowedHosts } returns null
      }
    val jobSyncConfig =
      JobSyncConfig()
        .withConfiguredAirbyteCatalog(
          ConfiguredAirbyteCatalog().withStreams(
            List.of<ConfiguredAirbyteStream>(
              mockk<ConfiguredAirbyteStream> {
                every { includeFiles } returns false
              },
              mockk<ConfiguredAirbyteStream> {
                every { includeFiles } returns false
              },
            ),
          ),
        )
    Assertions.assertFalse(jobInputHandler.shouldIncludeFiles(jobSyncConfig, sourceAdv, destinationAdv))
  }

  companion object {
    private val SECRET_REF_ID: UUID = UUID.randomUUID()
    private val SOURCE_CONFIGURATION =
      jsonNode(
        Map.of<String?, Any?>(
          "source_key",
          "source_value",
          "source_secret",
          Map.of<String?, UUID?>("_secret_reference_id", SECRET_REF_ID),
        ),
      )
    private val SOURCE_CONFIG_WITH_OAUTH =
      jsonNode(
        Map.of<String?, Any?>(
          "source_key",
          "source_value",
          "source_secret",
          Map.of<String?, UUID?>("_secret_reference_id", SECRET_REF_ID),
          "oauth",
          "oauth_value",
        ),
      )
    private val SOURCE_CONFIG_WITH_OAUTH_AND_INJECTED_CONFIG =
      jsonNode(
        Map.of<String?, Any?>(
          "source_key",
          "source_value",
          "source_secret",
          Map.of<String?, UUID?>("_secret_reference_id", SECRET_REF_ID),
          "oauth",
          "oauth_value",
          "injected",
          "value",
        ),
      )
    private val SOURCE_CONFIG_WITH_REFS =
      ConfigWithSecretReferences(
        SOURCE_CONFIG_WITH_OAUTH_AND_INJECTED_CONFIG,
        Map.of<String, SecretReferenceConfig>("$.source_secret", SecretReferenceConfig(AirbyteManagedSecretCoordinate(), null, null)),
      )

    private val INLINED_SOURCE_CONFIG_WITH_REFS: JsonNode = SOURCE_CONFIG_WITH_REFS.toInlined().value
    private val DESTINATION_CONFIGURATION =
      jsonNode(
        Map.of<String?, Any?>(
          "destination_key",
          "destination_value",
          "destination_secret",
          Map.of<String?, UUID?>("_secret_reference_id", SECRET_REF_ID),
        ),
      )
    private val DESTINATION_CONFIG_WITH_OAUTH =
      jsonNode(
        Map.of<String?, Any?>(
          "destination_key",
          "destination_value",
          "destination_secret",
          Map.of<String?, UUID?>("_secret_reference_id", SECRET_REF_ID),
          "oauth",
          "oauth_value",
        ),
      )
    private val DESTINATION_CONFIG_WITH_REFS =
      ConfigWithSecretReferences(
        DESTINATION_CONFIG_WITH_OAUTH,
        Map.of<String, SecretReferenceConfig>("$.destination_secret", SecretReferenceConfig(AirbyteManagedSecretCoordinate(), null, null)),
      )
    private val INLINED_DESTINATION_CONFIG_WITH_REFS: JsonNode = DESTINATION_CONFIG_WITH_REFS.toInlined().value
    private val STATE: State = State().withState(jsonNode<MutableMap<String?, String?>?>(Map.of<String?, String?>("state_key", "state_value")))

    private val WORKSPACE_ID: UUID = UUID.randomUUID()
    private const val JOB_ID: Long = 1
    private const val ATTEMPT_NUMBER = 1
    private val SOURCE_ID: UUID = UUID.randomUUID()
    private val DESTINATION_DEFINITION_ID: UUID = UUID.randomUUID()
    private val DESTINATION_ID: UUID = UUID.randomUUID()
    private val CONNECTION_ID: UUID = UUID.randomUUID()
  }
}
