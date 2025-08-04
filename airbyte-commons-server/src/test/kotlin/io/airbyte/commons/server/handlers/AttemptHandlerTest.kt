/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers

import io.airbyte.api.model.generated.ConnectionState
import io.airbyte.api.model.generated.ConnectionStateType
import io.airbyte.api.model.generated.GlobalState
import io.airbyte.api.model.generated.InternalOperationResult
import io.airbyte.api.model.generated.SaveAttemptSyncConfigRequestBody
import io.airbyte.api.model.generated.SaveStreamAttemptMetadataRequestBody
import io.airbyte.api.model.generated.StreamAttemptMetadata
import io.airbyte.commons.json.Jsons.emptyObject
import io.airbyte.commons.json.Jsons.jsonNode
import io.airbyte.commons.server.converters.ApiPojoConverters
import io.airbyte.commons.server.converters.JobConverter
import io.airbyte.commons.server.errors.BadRequestException
import io.airbyte.commons.server.errors.IdNotFoundKnownException
import io.airbyte.commons.server.errors.UnprocessableContentException
import io.airbyte.commons.server.handlers.helpers.CatalogConverter
import io.airbyte.commons.server.handlers.helpers.JobCreationAndStatusUpdateHelper
import io.airbyte.commons.temporal.TemporalUtils
import io.airbyte.config.ActorDefinitionVersion
import io.airbyte.config.AirbyteStream
import io.airbyte.config.Attempt
import io.airbyte.config.AttemptFailureSummary
import io.airbyte.config.AttemptSyncConfig
import io.airbyte.config.ConfiguredAirbyteCatalog
import io.airbyte.config.ConfiguredAirbyteStream
import io.airbyte.config.DestinationConnection
import io.airbyte.config.DestinationSyncMode
import io.airbyte.config.FailureReason
import io.airbyte.config.Job
import io.airbyte.config.JobConfig
import io.airbyte.config.JobConfig.ConfigType
import io.airbyte.config.JobOutput
import io.airbyte.config.JobResetConnectionConfig
import io.airbyte.config.JobStatus
import io.airbyte.config.JobSyncConfig
import io.airbyte.config.MapperConfig
import io.airbyte.config.RefreshConfig
import io.airbyte.config.ResetSourceConfiguration
import io.airbyte.config.StandardDestinationDefinition
import io.airbyte.config.StandardSync
import io.airbyte.config.StandardSyncOutput
import io.airbyte.config.StandardSyncSummary
import io.airbyte.config.StateType
import io.airbyte.config.StateWrapper
import io.airbyte.config.StreamDescriptor
import io.airbyte.config.SyncMode
import io.airbyte.config.SyncStats
import io.airbyte.config.helpers.FieldGenerator
import io.airbyte.config.persistence.ActorDefinitionVersionHelper
import io.airbyte.config.persistence.ConfigNotFoundException
import io.airbyte.config.persistence.StatePersistence
import io.airbyte.config.persistence.domain.StreamRefresh
import io.airbyte.config.persistence.helper.GenerationBumper
import io.airbyte.data.services.ConnectionService
import io.airbyte.data.services.DestinationService
import io.airbyte.data.services.StreamAttemptMetadataService
import io.airbyte.featureflag.Context
import io.airbyte.featureflag.EnableResumableFullRefresh
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.featureflag.Flag
import io.airbyte.featureflag.TestClient
import io.airbyte.mappers.transformations.Mapper
import io.airbyte.persistence.job.JobPersistence
import io.airbyte.validation.json.JsonValidationException
import jakarta.validation.Valid
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import org.junit.jupiter.params.provider.ValueSource
import org.mockito.Mockito
import org.mockito.kotlin.anyOrNull
import org.mockito.kotlin.argumentCaptor
import org.mockito.kotlin.eq
import java.io.IOException
import java.nio.file.Path
import java.util.Arrays
import java.util.Map
import java.util.Optional
import java.util.UUID
import java.util.stream.Collectors
import java.util.stream.Stream

internal class AttemptHandlerTest {
  private lateinit var jobConverter: JobConverter
  private lateinit var jobPersistence: JobPersistence
  private lateinit var statePersistence: StatePersistence
  private lateinit var path: Path
  private lateinit var helper: JobCreationAndStatusUpdateHelper
  private lateinit var ffClient: FeatureFlagClient
  private lateinit var generationBumper: GenerationBumper
  private lateinit var connectionService: ConnectionService
  private lateinit var destinationService: DestinationService
  private lateinit var actorDefinitionVersionHelper: ActorDefinitionVersionHelper
  private lateinit var streamAttemptMetadataService: StreamAttemptMetadataService

  private val apiPojoConverters = ApiPojoConverters(CatalogConverter(FieldGenerator(), mutableListOf<Mapper<out MapperConfig>>()))

  private lateinit var handler: AttemptHandler

  @BeforeEach
  fun setUp() {
    jobConverter = Mockito.mock(JobConverter::class.java)
    jobPersistence = Mockito.mock(JobPersistence::class.java)
    statePersistence = Mockito.mock(StatePersistence::class.java)
    path = Mockito.mock(Path::class.java)
    helper = Mockito.mock(JobCreationAndStatusUpdateHelper::class.java)
    ffClient = Mockito.mock(TestClient::class.java)
    generationBumper = Mockito.mock(GenerationBumper::class.java)
    connectionService = Mockito.mock(ConnectionService::class.java)
    destinationService = Mockito.mock(DestinationService::class.java)
    actorDefinitionVersionHelper = Mockito.mock(ActorDefinitionVersionHelper::class.java)
    streamAttemptMetadataService = Mockito.mock(StreamAttemptMetadataService::class.java)

    handler =
      AttemptHandler(
        jobPersistence,
        statePersistence,
        jobConverter,
        ffClient,
        helper,
        path,
        generationBumper,
        connectionService,
        destinationService,
        actorDefinitionVersionHelper,
        streamAttemptMetadataService,
        apiPojoConverters,
      )
  }

  @Test
  @Throws(Exception::class)
  fun testInternalHandlerSetsAttemptSyncConfig() {
    val attemptNumberCapture = argumentCaptor<Int>()
    val jobIdCapture = argumentCaptor<Long>()
    val attemptSyncConfigCapture = argumentCaptor<AttemptSyncConfig>()

    val sourceConfig = jsonNode<MutableMap<String?, String?>?>(Map.of<String?, String?>("source_key", "source_val"))
    val destinationConfig = jsonNode<MutableMap<String?, String?>?>(Map.of<String?, String?>("destination_key", "destination_val"))
    val state =
      ConnectionState()
        .connectionId(CONNECTION_ID)
        .stateType(ConnectionStateType.GLOBAL)
        .streamState(null)
        .globalState(GlobalState().sharedState(jsonNode<MutableMap<String?, String?>?>(Map.of<String?, String?>("state_key", "state_val"))))

    val attemptSyncConfig =
      io.airbyte.api.model.generated
        .AttemptSyncConfig()
        .destinationConfiguration(destinationConfig)
        .sourceConfiguration(sourceConfig)
        .state(state)

    val requestBody =
      SaveAttemptSyncConfigRequestBody().attemptNumber(ATTEMPT_NUMBER).jobId(JOB_ID).syncConfig(attemptSyncConfig)

    Assertions.assertTrue(handler.saveSyncConfig(requestBody).getSucceeded())

    Mockito
      .verify(jobPersistence)
      .writeAttemptSyncConfig(jobIdCapture.capture(), attemptNumberCapture.capture(), attemptSyncConfigCapture.capture())

    val expectedAttemptSyncConfig = apiPojoConverters.attemptSyncConfigToInternal(attemptSyncConfig)

    Assertions.assertEquals(ATTEMPT_NUMBER, attemptNumberCapture.firstValue)
    Assertions.assertEquals(JOB_ID, jobIdCapture.firstValue)
    Assertions.assertEquals(expectedAttemptSyncConfig, attemptSyncConfigCapture.firstValue)
  }

  @ParameterizedTest
  @ValueSource(booleans = [true, false])
  @Throws(IOException::class, ConfigNotFoundException::class, JsonValidationException::class, io.airbyte.data.ConfigNotFoundException::class)
  fun createAttemptNumberForSync(enableRfr: Boolean) {
    val attemptNumber = 0
    val connId = UUID.randomUUID()

    val mConfig = Mockito.mock(JobConfig::class.java)
    Mockito.`when`(mConfig.getConfigType()).thenReturn(ConfigType.SYNC)

    val job =
      Job(
        JOB_ID,
        ConfigType.SYNC,
        connId.toString(),
        mConfig,
        mutableListOf(),
        JobStatus.PENDING,
        1001L,
        1000L,
        1002L,
        true,
      )

    val mSyncConfig = Mockito.mock(JobSyncConfig::class.java)
    Mockito.`when`(mConfig.getSync()).thenReturn(mSyncConfig)
    Mockito.`when`(mSyncConfig.getWorkspaceId()).thenReturn(UUID.randomUUID())

    val mCatalog =
      ConfiguredAirbyteCatalog().withStreams(
        listOf(
          ConfiguredAirbyteStream(
            AirbyteStream("rfrStream", emptyObject(), listOf(SyncMode.FULL_REFRESH)).withIsResumable(true),
            SyncMode.FULL_REFRESH,
            DestinationSyncMode.APPEND,
          ),
        ),
      )
    Mockito.`when`(mSyncConfig.getConfiguredAirbyteCatalog()).thenReturn(mCatalog)

    Mockito.`when`(jobPersistence.getJob(JOB_ID)).thenReturn(job)
    Mockito.`when`(path.resolve(Mockito.anyString())).thenReturn(path)

    val expectedRoot = TemporalUtils.getJobRoot(path, JOB_ID.toString(), ATTEMPT_NUMBER.toLong())
    val expectedLogPath: Path = expectedRoot.resolve(DEFAULT_LOG_FILENAME)

    Mockito
      .`when`(jobPersistence.createAttempt(JOB_ID, expectedLogPath))
      .thenReturn(attemptNumber)
    Mockito
      .`when`(
        ffClient.boolVariation(
          anyOrNull<Flag<Boolean>>(),
          anyOrNull<Context>(),
        ),
      ).thenReturn(true)
    Mockito
      .`when`(
        ffClient.boolVariation(
          eq<EnableResumableFullRefresh>(EnableResumableFullRefresh),
          anyOrNull<Context>(),
        ),
      ).thenReturn(enableRfr)
    val stateWrapper = StateWrapper().withStateType(StateType.STREAM)
    if (enableRfr) {
      Mockito.`when`(statePersistence.getCurrentState(connId)).thenReturn(Optional.of<StateWrapper>(stateWrapper))
    }
    val destinationId = UUID.randomUUID()
    Mockito
      .`when`(connectionService.getStandardSync(connId))
      .thenReturn(StandardSync().withDestinationId(destinationId))
    Mockito
      .`when`(destinationService.getDestinationConnection(destinationId))
      .thenReturn(DestinationConnection().withWorkspaceId(WORKSPACE_ID))
    Mockito
      .`when`(
        actorDefinitionVersionHelper.getDestinationVersion(
          anyOrNull<StandardDestinationDefinition>(),
          anyOrNull<UUID>(),
          anyOrNull<UUID>(),
        ),
      ).thenReturn(ActorDefinitionVersion().withSupportsRefreshes(enableRfr))
    val output = handler.createNewAttemptNumber(JOB_ID)
    org.assertj.core.api.Assertions
      .assertThat(output.getAttemptNumber())
      .isEqualTo(attemptNumber)
    if (enableRfr) {
      Mockito.verify(generationBumper).updateGenerationForStreams(
        connId,
        JOB_ID,
        mutableListOf<StreamRefresh>(),
        setOf(
          StreamDescriptor().withName("rfrStream"),
        ),
      )
      Mockito
        .verify(statePersistence)
        .bulkDelete(connId, setOf(StreamDescriptor().withName("rfrStream")))
    }
  }

  @Test
  @Throws(IOException::class, ConfigNotFoundException::class, JsonValidationException::class, io.airbyte.data.ConfigNotFoundException::class)
  fun createAttemptNumberForClear() {
    val attemptNumber = 0
    val connId = UUID.randomUUID()
    val mResetConfig = Mockito.mock(JobResetConnectionConfig::class.java)
    val config =
      JobConfig()
        .withConfigType(ConfigType.CLEAR)
        .withResetConnection(mResetConfig)

    val job =
      Job(
        JOB_ID,
        ConfigType.CLEAR,
        connId.toString(),
        config,
        mutableListOf(),
        JobStatus.PENDING,
        1001L,
        1000L,
        1002L,
        true,
      )

    Mockito.`when`(mResetConfig.getWorkspaceId()).thenReturn(UUID.randomUUID())

    val mCatalog =
      ConfiguredAirbyteCatalog(
        listOf(
          ConfiguredAirbyteStream(
            AirbyteStream("rfrStream", emptyObject(), listOf(SyncMode.INCREMENTAL)).withIsResumable(true),
            SyncMode.INCREMENTAL,
            DestinationSyncMode.APPEND,
          ),
        ),
      )
    Mockito.`when`(mResetConfig.getConfiguredAirbyteCatalog()).thenReturn(mCatalog)
    Mockito.`when`(mResetConfig.getResetSourceConfiguration()).thenReturn(
      ResetSourceConfiguration().withStreamsToReset(
        listOf<StreamDescriptor?>(StreamDescriptor().withName("rfrStream")),
      ),
    )

    Mockito.`when`(jobPersistence.getJob(JOB_ID)).thenReturn(job)
    Mockito.`when`(path.resolve(Mockito.anyString())).thenReturn(path)

    val expectedRoot = TemporalUtils.getJobRoot(path, JOB_ID.toString(), ATTEMPT_NUMBER.toLong())
    val expectedLogPath: Path = expectedRoot.resolve(DEFAULT_LOG_FILENAME)

    Mockito
      .`when`(jobPersistence.createAttempt(JOB_ID, expectedLogPath))
      .thenReturn(attemptNumber)
    Mockito
      .`when`(
        ffClient.boolVariation(
          anyOrNull<Flag<Boolean>>(),
          anyOrNull<Context>(),
        ),
      ).thenReturn(true)
    val destinationId = UUID.randomUUID()
    Mockito
      .`when`(connectionService.getStandardSync(connId))
      .thenReturn(StandardSync().withDestinationId(destinationId))
    Mockito
      .`when`(destinationService.getDestinationConnection(destinationId))
      .thenReturn(DestinationConnection().withWorkspaceId(WORKSPACE_ID))
    Mockito
      .`when`(
        actorDefinitionVersionHelper.getDestinationVersion(
          anyOrNull<StandardDestinationDefinition>(),
          anyOrNull<UUID>(),
          anyOrNull<UUID>(),
        ),
      ).thenReturn(ActorDefinitionVersion().withSupportsRefreshes(true))
    val output = handler.createNewAttemptNumber(JOB_ID)
    org.assertj.core.api.Assertions
      .assertThat(output.getAttemptNumber())
      .isEqualTo(attemptNumber)
    Mockito.verify(generationBumper).updateGenerationForStreams(
      connId,
      JOB_ID,
      mutableListOf<StreamRefresh>(),
      setOf(
        StreamDescriptor().withName("rfrStream"),
      ),
    )
    Mockito
      .verify(statePersistence)
      .bulkDelete(connId, setOf(StreamDescriptor().withName("rfrStream")))
  }

  @ParameterizedTest
  @ValueSource(ints = [0, 2])
  @Throws(IOException::class, ConfigNotFoundException::class, JsonValidationException::class, io.airbyte.data.ConfigNotFoundException::class)
  fun createAttemptNumberForRefresh(attemptNumber: Int) {
    val connId = UUID.randomUUID()

    val mConfig = Mockito.mock(JobConfig::class.java)
    Mockito.`when`(mConfig.getConfigType()).thenReturn(ConfigType.REFRESH)

    val job =
      Job(
        JOB_ID,
        ConfigType.REFRESH,
        connId.toString(),
        mConfig,
        mutableListOf(),
        JobStatus.PENDING,
        1001L,
        1000L,
        1002L,
        true,
      )

    val mRefreshConfig = Mockito.mock(RefreshConfig::class.java)
    Mockito.`when`(mConfig.getRefresh()).thenReturn(mRefreshConfig)
    Mockito.`when`(mRefreshConfig.getWorkspaceId()).thenReturn(UUID.randomUUID())

    val mCatalog =
      ConfiguredAirbyteCatalog(
        listOf<ConfiguredAirbyteStream>(
          ConfiguredAirbyteStream(
            AirbyteStream("rfrStream", emptyObject(), listOf(SyncMode.FULL_REFRESH)).withIsResumable(true),
            SyncMode.FULL_REFRESH,
            DestinationSyncMode.APPEND,
          ),
          ConfiguredAirbyteStream(
            AirbyteStream("nonRfrStream", emptyObject(), listOf(SyncMode.FULL_REFRESH)),
            SyncMode.FULL_REFRESH,
            DestinationSyncMode.APPEND,
          ),
        ),
      )
    Mockito.`when`(mRefreshConfig.getConfiguredAirbyteCatalog()).thenReturn(mCatalog)

    Mockito.`when`(jobPersistence.getJob(JOB_ID)).thenReturn(job)
    Mockito.`when`(path.resolve(Mockito.anyString())).thenReturn(path)

    val expectedRoot = TemporalUtils.getJobRoot(path, JOB_ID.toString(), ATTEMPT_NUMBER.toLong())
    val expectedLogPath: Path = expectedRoot.resolve(DEFAULT_LOG_FILENAME)

    Mockito
      .`when`(jobPersistence.createAttempt(JOB_ID, expectedLogPath))
      .thenReturn(attemptNumber)
    Mockito
      .`when`(
        ffClient.boolVariation(
          anyOrNull<Flag<Boolean>>(),
          anyOrNull<Context>(),
        ),
      ).thenReturn(true)
    Mockito
      .`when`(
        ffClient.boolVariation(
          eq<EnableResumableFullRefresh>(EnableResumableFullRefresh),
          anyOrNull<Context>(),
        ),
      ).thenReturn(true)
    val destinationId = UUID.randomUUID()
    Mockito
      .`when`(connectionService.getStandardSync(connId))
      .thenReturn(StandardSync().withDestinationId(destinationId))
    Mockito
      .`when`(destinationService.getDestinationConnection(destinationId))
      .thenReturn(DestinationConnection().withWorkspaceId(WORKSPACE_ID))
    Mockito
      .`when`(
        actorDefinitionVersionHelper.getDestinationVersion(
          anyOrNull<StandardDestinationDefinition>(),
          anyOrNull<UUID>(),
          anyOrNull<UUID>(),
        ),
      ).thenReturn(ActorDefinitionVersion().withSupportsRefreshes(true))
    val output = handler.createNewAttemptNumber(JOB_ID)
    org.assertj.core.api.Assertions
      .assertThat(output.getAttemptNumber())
      .isEqualTo(attemptNumber)
    if (attemptNumber == 0) {
      Mockito.verify(generationBumper).updateGenerationForStreams(
        connId,
        JOB_ID,
        mutableListOf<StreamRefresh>(),
        setOf(
          StreamDescriptor().withName("rfrStream"),
          StreamDescriptor().withName("nonRfrStream"),
        ),
      )
      Mockito.verify(statePersistence).bulkDelete(
        connId,
        setOf(
          StreamDescriptor().withName("rfrStream"),
          StreamDescriptor().withName("nonRfrStream"),
        ),
      )
    } else {
      Mockito.verify(generationBumper).updateGenerationForStreams(
        connId,
        JOB_ID,
        mutableListOf<StreamRefresh>(),
        setOf(
          StreamDescriptor().withName("nonRfrStream"),
        ),
      )
      Mockito
        .verify(statePersistence)
        .bulkDelete(connId, setOf(StreamDescriptor().withName("nonRfrStream")))
    }
  }

  @Nested
  internal inner class ClearFullRefreshStreamStateFirstAttempt {
    @ParameterizedTest
    @ValueSource(booleans = [true, false])
    fun getFullRefreshStreamsShouldOnlyReturnFullRefreshStreams(enableResumableFullRefresh: Boolean) {
      val mJobConfig = Mockito.mock(JobConfig::class.java)

      val mSyncConfig = Mockito.mock(JobSyncConfig::class.java)
      Mockito.`when`(mJobConfig.getSync()).thenReturn(mSyncConfig)

      val mCatalog =
        ConfiguredAirbyteCatalog(
          listOf<ConfiguredAirbyteStream>(
            ConfiguredAirbyteStream(
              AirbyteStream("full", emptyObject(), listOf(SyncMode.FULL_REFRESH)).withIsResumable(true),
              SyncMode.FULL_REFRESH,
              DestinationSyncMode.APPEND,
            ),
            ConfiguredAirbyteStream(
              AirbyteStream("incre", emptyObject(), listOf(SyncMode.INCREMENTAL)),
              SyncMode.INCREMENTAL,
              DestinationSyncMode.APPEND,
            ),
            ConfiguredAirbyteStream(
              AirbyteStream("full", emptyObject(), listOf(SyncMode.FULL_REFRESH)).withNamespace("name"),
              SyncMode.FULL_REFRESH,
              DestinationSyncMode.APPEND,
            ),
            ConfiguredAirbyteStream(
              AirbyteStream("incre", emptyObject(), listOf(SyncMode.INCREMENTAL)).withNamespace("name"),
              SyncMode.INCREMENTAL,
              DestinationSyncMode.APPEND,
            ),
          ),
        )
      Mockito.`when`(mSyncConfig.getConfiguredAirbyteCatalog()).thenReturn(mCatalog)

      val streams = handler.getFullRefreshStreamsToClear(mCatalog, 1, enableResumableFullRefresh)
      val exp =
        if (enableResumableFullRefresh) {
          setOf(StreamDescriptor().withName("full").withNamespace("name"))
        } else {
          setOf(StreamDescriptor().withName("full"), StreamDescriptor().withName("full").withNamespace("name"))
        }
      Assertions.assertEquals(exp, streams)
    }

    @ParameterizedTest
    @MethodSource("io.airbyte.commons.server.handlers.AttemptHandlerTest#provideCreateAttemptConfig")
    @Throws(IOException::class, ConfigNotFoundException::class, JsonValidationException::class, io.airbyte.data.ConfigNotFoundException::class)
    fun createAttemptShouldAlwaysDeleteFullRefreshStreamState(
      attemptNumber: Int,
      enableResumableFullRefresh: Boolean,
    ) {
      val connId = UUID.randomUUID()

      val mConfig = Mockito.mock(JobConfig::class.java)
      Mockito.`when`(mConfig.getConfigType()).thenReturn(ConfigType.SYNC)

      val job =
        Job(
          JOB_ID,
          ConfigType.SYNC,
          connId.toString(),
          mConfig,
          mutableListOf(),
          JobStatus.PENDING,
          1001L,
          1000L,
          1002L,
          true,
        )

      val mSyncConfig = Mockito.mock(JobSyncConfig::class.java)
      Mockito.`when`(mConfig.getSync()).thenReturn(mSyncConfig)
      Mockito.`when`(mSyncConfig.getWorkspaceId()).thenReturn(UUID.randomUUID())

      Mockito.`when`(jobPersistence.getJob(JOB_ID)).thenReturn(job)

      Mockito.`when`(path.resolve(Mockito.anyString())).thenReturn(path)
      Mockito
        .`when`(
          ffClient.boolVariation(
            anyOrNull<Flag<Boolean>>(),
            anyOrNull<Context>(),
          ),
        ).thenReturn(true)
      Mockito
        .`when`(
          ffClient.boolVariation(
            eq<EnableResumableFullRefresh>(EnableResumableFullRefresh),
            anyOrNull<Context>(),
          ),
        ).thenReturn(enableResumableFullRefresh)
      val expectedRoot = TemporalUtils.getJobRoot(path, JOB_ID.toString(), ATTEMPT_NUMBER.toLong())
      val expectedLogPath: Path = expectedRoot.resolve(DEFAULT_LOG_FILENAME)

      val mCatalog =
        ConfiguredAirbyteCatalog(
          listOf<ConfiguredAirbyteStream>(
            ConfiguredAirbyteStream(
              AirbyteStream("full", emptyObject(), listOf(SyncMode.FULL_REFRESH)).withIsResumable(true),
              SyncMode.FULL_REFRESH,
              DestinationSyncMode.APPEND,
            ),
            ConfiguredAirbyteStream(
              AirbyteStream("incre", emptyObject(), listOf(SyncMode.INCREMENTAL)),
              SyncMode.INCREMENTAL,
              DestinationSyncMode.APPEND,
            ),
            ConfiguredAirbyteStream(
              AirbyteStream("full", emptyObject(), listOf(SyncMode.FULL_REFRESH)).withNamespace("name"),
              SyncMode.FULL_REFRESH,
              DestinationSyncMode.APPEND,
            ),
            ConfiguredAirbyteStream(
              AirbyteStream("incre", emptyObject(), listOf(SyncMode.INCREMENTAL)).withNamespace("name"),
              SyncMode.INCREMENTAL,
              DestinationSyncMode.APPEND,
            ),
          ),
        )
      Mockito.`when`(mSyncConfig.getConfiguredAirbyteCatalog()).thenReturn(mCatalog)

      Mockito.`when`(jobPersistence.createAttempt(JOB_ID, expectedLogPath)).thenReturn(attemptNumber)

      val destinationId = UUID.randomUUID()
      Mockito
        .`when`(connectionService.getStandardSync(connId))
        .thenReturn(StandardSync().withDestinationId(destinationId))
      Mockito
        .`when`(destinationService.getDestinationConnection(destinationId))
        .thenReturn(DestinationConnection().withWorkspaceId(WORKSPACE_ID))
      Mockito
        .`when`(
          actorDefinitionVersionHelper.getDestinationVersion(
            anyOrNull<StandardDestinationDefinition>(),
            anyOrNull<UUID>(),
            anyOrNull<UUID>(),
          ),
        ).thenReturn(ActorDefinitionVersion().withSupportsRefreshes(true))

      val output = handler.createNewAttemptNumber(JOB_ID)
      org.assertj.core.api.Assertions
        .assertThat(output.getAttemptNumber())
        .isEqualTo(attemptNumber)

      val captor1 = argumentCaptor<UUID>()
      val captor2 = argumentCaptor<MutableSet<StreamDescriptor>>()
      Mockito.verify(statePersistence).bulkDelete(captor1.capture(), captor2.capture())
      Assertions.assertEquals(connId, captor1.firstValue)
      if (enableResumableFullRefresh) {
        val nonResumableFullRefresh =
          if (attemptNumber == 0) {
            setOf(
              StreamDescriptor().withName("full"),
              StreamDescriptor().withName("full").withNamespace("name"),
            )
          } else {
            setOf(StreamDescriptor().withName("full").withNamespace("name"))
          }
        Assertions.assertEquals(nonResumableFullRefresh, captor2.firstValue)
        Mockito
          .verify(generationBumper)
          .updateGenerationForStreams(connId, JOB_ID, mutableListOf<StreamRefresh>(), nonResumableFullRefresh)
      } else {
        Assertions.assertEquals(
          setOf(StreamDescriptor().withName("full"), StreamDescriptor().withName("full").withNamespace("name")),
          captor2.firstValue,
        )
      }
    }
  }

  @Test
  @Throws(IOException::class)
  fun createAttemptNumberWithUnknownJobId() {
    val job = Mockito.mock(Job::class.java)
    Mockito
      .`when`(job.getAttemptsCount())
      .thenReturn(ATTEMPT_NUMBER)

    Mockito
      .`when`(jobPersistence.getJob(JOB_ID))
      .thenThrow(RuntimeException("unknown jobId " + JOB_ID))

    org.assertj.core.api.Assertions
      .assertThatThrownBy { handler.createNewAttemptNumber(JOB_ID) }
      .isInstanceOf(UnprocessableContentException::class.java)
  }

  @Test
  @Throws(Exception::class)
  fun getAttemptThrowsNotFound() {
    Mockito.`when`(jobPersistence.getAttemptForJob(anyOrNull(), anyOrNull())).thenReturn(
      Optional.empty<Attempt>(),
    )

    Assertions.assertThrows(
      IdNotFoundKnownException::class.java,
    ) { handler.getAttemptForJob(1L, 2) }
  }

  @Test
  @Throws(Exception::class)
  fun getAttemptCombinedStatsThrowsNotFound() {
    Mockito
      .`when`(jobPersistence.getAttemptCombinedStats(anyOrNull(), anyOrNull()))
      .thenReturn(null)

    Assertions.assertThrows(
      IdNotFoundKnownException::class.java,
    ) { handler.getAttemptCombinedStats(1L, 2) }
  }

  @Test
  @Throws(Exception::class)
  fun getAttemptCombinedStatsReturnsStats() {
    val stats = SyncStats()
    stats.setRecordsEmitted(123L)
    stats.setBytesEmitted(123L)
    stats.setBytesCommitted(123L)
    stats.setRecordsCommitted(123L)
    stats.setEstimatedRecords(123L)
    stats.setEstimatedBytes(123L)

    Mockito
      .`when`(jobPersistence.getAttemptCombinedStats(anyOrNull(), anyOrNull()))
      .thenReturn(stats)

    val result = handler.getAttemptCombinedStats(1L, 2)
    Assertions.assertEquals(stats.getRecordsEmitted(), result.getRecordsEmitted())
    Assertions.assertEquals(stats.getBytesEmitted(), result.getBytesEmitted())
    Assertions.assertEquals(stats.getBytesCommitted(), result.getBytesCommitted())
    Assertions.assertEquals(stats.getRecordsCommitted(), result.getRecordsCommitted())
    Assertions.assertEquals(stats.getEstimatedRecords(), result.getEstimatedRecords())
    Assertions.assertEquals(stats.getEstimatedBytes(), result.getEstimatedBytes())
    Assertions.assertNull(result.getStateMessagesEmitted()) // punting on this for now
  }

  @Test
  @Throws(IOException::class)
  fun failAttemptSyncSummaryOutputPresent() {
    handler.failAttempt(ATTEMPT_NUMBER, JOB_ID, failureSummary, standardSyncOutput)

    Mockito.verify(jobPersistence).failAttempt(JOB_ID, ATTEMPT_NUMBER)
    Mockito.verify(jobPersistence).writeOutput(JOB_ID, ATTEMPT_NUMBER, jobOutput)
    Mockito.verify(jobPersistence).writeAttemptFailureSummary(JOB_ID, ATTEMPT_NUMBER, failureSummary)
  }

  @Test
  @Throws(IOException::class)
  fun failAttemptSyncSummaryOutputNotPresent() {
    handler.failAttempt(ATTEMPT_NUMBER, JOB_ID, failureSummary, null)

    Mockito.verify(jobPersistence).failAttempt(JOB_ID, ATTEMPT_NUMBER)
    Mockito.verify(jobPersistence, Mockito.never()).writeOutput(JOB_ID, ATTEMPT_NUMBER, jobOutput)
    Mockito.verify(jobPersistence).writeAttemptFailureSummary(JOB_ID, ATTEMPT_NUMBER, failureSummary)
  }

  @Test
  @Throws(IOException::class)
  fun failAttemptSyncSummaryNotPresent() {
    handler.failAttempt(ATTEMPT_NUMBER, JOB_ID, null, standardSyncOutput)

    Mockito.verify(jobPersistence).failAttempt(JOB_ID, ATTEMPT_NUMBER)
    Mockito.verify(jobPersistence).writeOutput(JOB_ID, ATTEMPT_NUMBER, jobOutput)
    Mockito.verify(jobPersistence).writeAttemptFailureSummary(JOB_ID, ATTEMPT_NUMBER, null)
  }

  @ParameterizedTest
  @MethodSource("randomObjects")
  fun failAttemptValidatesFailureSummary(thing: Any?) {
    Assertions.assertThrows(BadRequestException::class.java) {
      handler.failAttempt(
        ATTEMPT_NUMBER,
        JOB_ID,
        thing,
        standardSyncOutput,
      )
    }
  }

  @ParameterizedTest
  @MethodSource("randomObjects")
  fun failAttemptValidatesSyncOutput(thing: Any?) {
    Assertions.assertThrows(BadRequestException::class.java) {
      handler.failAttempt(
        ATTEMPT_NUMBER,
        JOB_ID,
        failureSummary,
        thing,
      )
    }
  }

  @Test
  fun saveStreamMetadata() {
    val jobId = 123L
    val attemptNumber = 1

    val result =
      handler.saveStreamMetadata(
        SaveStreamAttemptMetadataRequestBody()
          .jobId(jobId)
          .attemptNumber(attemptNumber)
          .streamMetadata(
            listOf<@Valid StreamAttemptMetadata?>(
              StreamAttemptMetadata().streamName("s1").wasBackfilled(false).wasResumed(true),
              StreamAttemptMetadata()
                .streamName("s2")
                .streamNamespace("ns")
                .wasBackfilled(true)
                .wasResumed(false),
            ),
          ),
      )
    Mockito.verify(streamAttemptMetadataService).upsertStreamAttemptMetadata(
      jobId,
      attemptNumber.toLong(),
      listOf(
        io.airbyte.data.services
          .StreamAttemptMetadata("s1", null, false, true),
        io.airbyte.data.services
          .StreamAttemptMetadata("s2", "ns", true, false),
      ),
    )
    Assertions.assertEquals(InternalOperationResult().succeeded(true), result)
  }

  @Test
  fun saveStreamMetadataFailure() {
    val jobId = 123L
    val attemptNumber = 1

    Mockito.doThrow(RuntimeException("oops")).`when`(streamAttemptMetadataService).upsertStreamAttemptMetadata(
      anyOrNull(),
      anyOrNull(),
      anyOrNull(),
    )

    val result =
      handler.saveStreamMetadata(
        SaveStreamAttemptMetadataRequestBody()
          .jobId(jobId)
          .attemptNumber(attemptNumber)
          .streamMetadata(
            listOf<@Valid StreamAttemptMetadata?>(
              StreamAttemptMetadata().streamName("s").wasBackfilled(false).wasResumed(false),
            ),
          ),
      )
    Assertions.assertEquals(InternalOperationResult().succeeded(false), result)
  }

  @ParameterizedTest
  @MethodSource("testStateClearingLogicProvider")
  @Throws(Exception::class)
  fun testStateClearingLogic(
    attemptNumber: Int,
    supportsRefresh: Boolean,
    expectedStreamsToClear: Set<StreamDescriptor>,
  ) {
    Mockito
      .`when`(
        ffClient.boolVariation(
          eq(EnableResumableFullRefresh),
          anyOrNull(),
        ),
      ).thenReturn(true)
    val configuredCatalog =
      ConfiguredAirbyteCatalog(
        listOf(
          buildStreamForClearStateTest(STREAM_INCREMENTAL_NOT_RESUMABLE, SyncMode.INCREMENTAL, false),
          buildStreamForClearStateTest(STREAM_INCREMENTAL, SyncMode.INCREMENTAL, true),
          buildStreamForClearStateTest(STREAM_FULL_REFRESH_NOT_RESUMABLE, SyncMode.FULL_REFRESH, false),
          buildStreamForClearStateTest(STREAM_FULL_REFRESH_RESUMABLE, SyncMode.FULL_REFRESH, true),
        ),
      )

    val connectionId = UUID.randomUUID()

    val job =
      Job(
        JOB_ID,
        ConfigType.SYNC,
        connectionId.toString(),
        JobConfig().withConfigType(ConfigType.SYNC).withSync(JobSyncConfig().withConfiguredAirbyteCatalog(configuredCatalog)),
        mutableListOf(),
        JobStatus.PENDING,
        1001L,
        1000L,
        1002L,
        true,
      )

    if (attemptNumber == 0) {
      handler.updateGenerationAndStateForFirstAttempt(job, connectionId, supportsRefresh)
    } else {
      handler.updateGenerationAndStateForSubsequentAttempts(job, supportsRefresh)
    }
    Mockito.verify(statePersistence).bulkDelete(connectionId, expectedStreamsToClear)
  }

  companion object {
    private val CONNECTION_ID: UUID = UUID.randomUUID()
    private val WORKSPACE_ID: UUID = UUID.randomUUID()
    private const val JOB_ID = 10002L
    private const val ATTEMPT_NUMBER = 1
    private const val PROCESSING_TASK_QUEUE = "SYNC"
    private const val DEFAULT_LOG_FILENAME = "logs.log"

    private const val STREAM_INCREMENTAL = "incremental"
    private const val STREAM_INCREMENTAL_NOT_RESUMABLE = "incremental not resumable"
    private const val STREAM_FULL_REFRESH_RESUMABLE = "full refresh resumable"
    private const val STREAM_FULL_REFRESH_NOT_RESUMABLE = "full refresh not resumable"

    private val standardSyncOutput: StandardSyncOutput? =
      StandardSyncOutput()
        .withStandardSyncSummary(
          StandardSyncSummary()
            .withStatus(StandardSyncSummary.ReplicationStatus.COMPLETED),
        )

    private val jobOutput: JobOutput = JobOutput().withSync(standardSyncOutput)

    private val failureSummary: AttemptFailureSummary? =
      AttemptFailureSummary()
        .withFailures(
          mutableListOf<FailureReason?>(
            FailureReason()
              .withFailureOrigin(FailureReason.FailureOrigin.SOURCE),
          ),
        )

    @JvmStatic
    private fun testStateClearingLogicProvider(): Stream<Arguments> =
      Stream.of(
        // streams are STREAM_INCREMENTAL, STREAM_INCREMENTAL_NOT_RESUMABLE, STREAM_FULL_REFRESH_RESUMABLE,
        // STREAM_FULL_REFRESH_NOT_RESUMABLE
        // AttemptNumber, SupportsRefresh, streams to clear
        Arguments.of(0, false, streamDescriptorsFromNames(STREAM_FULL_REFRESH_NOT_RESUMABLE, STREAM_FULL_REFRESH_RESUMABLE)),
        Arguments.of(0, true, streamDescriptorsFromNames(STREAM_FULL_REFRESH_NOT_RESUMABLE, STREAM_FULL_REFRESH_RESUMABLE)),
        Arguments.of(1, false, streamDescriptorsFromNames(STREAM_FULL_REFRESH_NOT_RESUMABLE, STREAM_FULL_REFRESH_RESUMABLE)),
        Arguments.of(1, true, streamDescriptorsFromNames(STREAM_FULL_REFRESH_NOT_RESUMABLE)),
      )

    private fun buildStreamForClearStateTest(
      streamName: String,
      syncMode: SyncMode,
      isResumable: Boolean,
    ): ConfiguredAirbyteStream =
      ConfiguredAirbyteStream(
        AirbyteStream(streamName, emptyObject(), listOf(syncMode)).withIsResumable(
          isResumable,
        ),
      ).withSyncMode(syncMode)

    private fun streamDescriptorsFromNames(vararg streamNames: String?): MutableSet<StreamDescriptor?> =
      Arrays
        .stream<String?>(streamNames)
        .map { n: String? -> StreamDescriptor().withName(n) }
        .collect(Collectors.toSet())

    @JvmStatic
    private fun provideCreateAttemptConfig(): Stream<Arguments?> =
      Stream.of<Arguments?>(
        Arguments.of(0, true),
        Arguments.of(1, true),
        Arguments.of(2, true),
        Arguments.of(3, true),
        Arguments.of(0, false),
        Arguments.of(1, false),
        Arguments.of(2, false),
        Arguments.of(3, false),
      )

    @JvmStatic
    private fun randomObjects(): Stream<Arguments?> =
      Stream.of<Arguments?>(
        Arguments.of(123L),
        Arguments.of(true),
        Arguments.of(mutableListOf<String?>("123", "123")),
        Arguments.of("a string"),
        Arguments.of(543.0),
      )
  }
}
