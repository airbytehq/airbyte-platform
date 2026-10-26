/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers

import io.airbyte.api.server.generated.models.AttemptStats
import io.airbyte.api.server.generated.models.AttemptStreamStats
import io.airbyte.api.server.generated.models.ConnectionState
import io.airbyte.api.server.generated.models.ConnectionStateType
import io.airbyte.api.server.generated.models.GlobalState
import io.airbyte.api.server.generated.models.InternalOperationResult
import io.airbyte.api.server.generated.models.SaveAttemptSyncConfigRequestBody
import io.airbyte.api.server.generated.models.SaveStatsRequestBody
import io.airbyte.api.server.generated.models.SaveStreamAttemptMetadataRequestBody
import io.airbyte.api.server.generated.models.StreamAttemptMetadata
import io.airbyte.commons.json.Jsons.emptyObject
import io.airbyte.commons.json.Jsons.jsonNode
import io.airbyte.commons.server.converters.ApiPojoConverters
import io.airbyte.commons.server.converters.JobConverter
import io.airbyte.commons.server.errors.BadRequestException
import io.airbyte.commons.server.errors.ConflictException
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
import io.airbyte.config.StreamSyncStats
import io.airbyte.config.SyncMode
import io.airbyte.config.SyncStats
import io.airbyte.config.helpers.FieldGenerator
import io.airbyte.config.persistence.ActorDefinitionVersionHelper
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
import io.airbyte.persistence.job.JobInTerminalStateException
import io.airbyte.persistence.job.JobPersistence
import io.airbyte.persistence.job.JobRunningAttemptExistsException
import io.mockk.Called
import io.mockk.Runs
import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import io.mockk.slot
import io.mockk.verify
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import org.junit.jupiter.params.provider.ValueSource
import java.nio.file.Path
import java.util.Optional
import java.util.UUID

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
    jobConverter = mockk<JobConverter>()
    jobPersistence = mockk<JobPersistence>()
    statePersistence = mockk<StatePersistence>()
    path = mockk<Path>()
    helper = mockk<JobCreationAndStatusUpdateHelper>()
    ffClient = mockk<TestClient>()
    generationBumper = mockk<GenerationBumper>()
    connectionService = mockk<ConnectionService>()
    destinationService = mockk<DestinationService>()
    actorDefinitionVersionHelper = mockk<ActorDefinitionVersionHelper>()
    streamAttemptMetadataService = mockk<StreamAttemptMetadataService>()

    every { helper.traceFailures(any()) } just Runs
    every { helper.emitJobToReleaseStagesMetric(any(), any()) } just Runs
    every { helper.emitAttemptCompletedEventIfAttemptPresent(any()) } just Runs
    every { helper.trackFailures(any()) } just Runs
    every { helper.emitAttemptCreatedEvent(any(), any()) } just Runs
    every { generationBumper.updateGenerationForStreams(any(), any(), any(), any()) } just Runs
    every { statePersistence.bulkDelete(any(), any()) } just Runs
    every { streamAttemptMetadataService.upsertStreamAttemptMetadata(any(), any(), any()) } just Runs
    every { jobPersistence.failAttempt(any(), any()) } just Runs
    every { jobPersistence.writeOutput(any(), any(), any()) } just Runs
    every { jobPersistence.writeAttemptFailureSummary(any(), any(), any()) } just Runs
    every { jobPersistence.writeAttemptSyncConfig(any(), any(), any()) } just Runs
    every {
      jobPersistence.writeStats(any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any())
    } just Runs
    every { destinationService.getDestinationDefinitionFromDestination(any()) } returns StandardDestinationDefinition()

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
  fun createNewAttemptNumberThrowsConflictWhenAttemptRunning() {
    val connId = UUID.randomUUID()
    val job =
      Job(
        JOB_ID,
        ConfigType.SYNC,
        connId.toString(),
        mockk<JobConfig>(),
        mutableListOf(),
        JobStatus.RUNNING,
        1001L,
        1000L,
        1002L,
        true,
      )
    every { jobPersistence.getJob(JOB_ID) } returns job
    every { path.resolve(any<String>()) } returns path
    every { jobPersistence.createAttempt(JOB_ID, any()) } throws JobRunningAttemptExistsException(JOB_ID, connId.toString(), 2)

    // Conflict, not idempotent success: the job already has an in-flight attempt.
    Assertions.assertThrows(ConflictException::class.java) { handler.createNewAttemptNumber(JOB_ID) }
    verify { generationBumper wasNot Called }
    verify { connectionService wasNot Called }
  }

  @Test
  fun createNewAttemptNumberThrowsConflictWhenJobTerminal() {
    val connId = UUID.randomUUID()
    val job =
      Job(
        JOB_ID,
        ConfigType.SYNC,
        connId.toString(),
        mockk<JobConfig>(),
        mutableListOf(),
        JobStatus.SUCCEEDED,
        1001L,
        1000L,
        1002L,
        true,
      )
    every { jobPersistence.getJob(JOB_ID) } returns job
    every { path.resolve(any<String>()) } returns path
    every { jobPersistence.createAttempt(JOB_ID, any()) } throws JobInTerminalStateException(JOB_ID, connId.toString(), JobStatus.SUCCEEDED)

    Assertions.assertThrows(ConflictException::class.java) { handler.createNewAttemptNumber(JOB_ID) }
    verify { generationBumper wasNot Called }
  }

  @Test
  fun testInternalHandlerSetsAttemptSyncConfig() {
    val attemptNumberCapture = slot<Int>()
    val jobIdCapture = slot<Long>()
    val attemptSyncConfigCapture = slot<AttemptSyncConfig>()

    val sourceConfig = jsonNode(mapOf("source_key" to "source_val"))
    val destinationConfig = jsonNode(mapOf("destination_key" to "destination_val"))
    val state =
      ConnectionState(
        connectionId = CONNECTION_ID,
        stateType = ConnectionStateType.GLOBAL,
        streamState = null,
        globalState = GlobalState(streamStates = emptyList(), sharedState = jsonNode(mapOf("state_key" to "state_val"))),
      )

    val attemptSyncConfig =
      io.airbyte.api.server.generated.models.AttemptSyncConfig(
        destinationConfiguration = destinationConfig,
        sourceConfiguration = sourceConfig,
        state = state,
      )

    val requestBody =
      SaveAttemptSyncConfigRequestBody(
        attemptNumber = ATTEMPT_NUMBER,
        jobId = JOB_ID,
        syncConfig = attemptSyncConfig,
      )

    Assertions.assertTrue(handler.saveSyncConfig(requestBody).succeeded)

    verify { jobPersistence.writeAttemptSyncConfig(capture(jobIdCapture), capture(attemptNumberCapture), capture(attemptSyncConfigCapture)) }

    val expectedAttemptSyncConfig = apiPojoConverters.attemptSyncConfigToInternal(attemptSyncConfig)

    Assertions.assertEquals(ATTEMPT_NUMBER, attemptNumberCapture.captured)
    Assertions.assertEquals(JOB_ID, jobIdCapture.captured)
    Assertions.assertEquals(expectedAttemptSyncConfig, attemptSyncConfigCapture.captured)
  }

  @ParameterizedTest
  @ValueSource(booleans = [true, false])
  fun createAttemptNumberForSync(enableRfr: Boolean) {
    val attemptNumber = 0
    val connId = UUID.randomUUID()

    val mConfig = mockk<JobConfig>()
    every { mConfig.getConfigType() } returns ConfigType.SYNC

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

    val mSyncConfig = mockk<JobSyncConfig>()
    every { mConfig.getSync() } returns mSyncConfig
    every { mSyncConfig.getWorkspaceId() } returns UUID.randomUUID()

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
    every { mSyncConfig.getConfiguredAirbyteCatalog() } returns mCatalog

    every { jobPersistence.getJob(JOB_ID) } returns job
    every { path.resolve(any<String>()) } returns path

    val expectedRoot = TemporalUtils.getJobRoot(path, JOB_ID.toString(), ATTEMPT_NUMBER.toLong())
    val expectedLogPath: Path = expectedRoot.resolve(DEFAULT_LOG_FILENAME)

    every { jobPersistence.createAttempt(JOB_ID, expectedLogPath) } returns attemptNumber
    every {
      ffClient.boolVariation(
        any<Flag<Boolean>>(),
        any<Context>(),
      )
    } returns true
    every {
      ffClient.boolVariation(
        eq<EnableResumableFullRefresh>(EnableResumableFullRefresh),
        any<Context>(),
      )
    } returns enableRfr
    val stateWrapper = StateWrapper().withStateType(StateType.STREAM)
    if (enableRfr) {
      every { statePersistence.getCurrentState(connId) } returns Optional.of<StateWrapper>(stateWrapper)
    }
    val destinationId = UUID.randomUUID()
    every { connectionService.getStandardSync(connId) } returns StandardSync().withDestinationId(destinationId)
    every { destinationService.getDestinationConnection(destinationId) } returns DestinationConnection().withWorkspaceId(WORKSPACE_ID)
    every {
      actorDefinitionVersionHelper.getDestinationVersion(
        any<StandardDestinationDefinition>(),
        any<UUID>(),
        any<UUID>(),
      )
    } returns ActorDefinitionVersion().withSupportsRefreshes(enableRfr)
    val output = handler.createNewAttemptNumber(JOB_ID)
    org.assertj.core.api.Assertions
      .assertThat(output.attemptNumber)
      .isEqualTo(attemptNumber)
    if (enableRfr) {
      verify {
        generationBumper.updateGenerationForStreams(
          connId,
          JOB_ID,
          mutableListOf<StreamRefresh>(),
          setOf(
            StreamDescriptor().withName("rfrStream"),
          ),
        )
      }
      verify { statePersistence.bulkDelete(connId, setOf(StreamDescriptor().withName("rfrStream"))) }
    }
  }

  @Test
  fun createAttemptNumberForClear() {
    val attemptNumber = 0
    val connId = UUID.randomUUID()
    val mResetConfig = mockk<JobResetConnectionConfig>()
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

    every { mResetConfig.getWorkspaceId() } returns UUID.randomUUID()

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
    every { mResetConfig.getConfiguredAirbyteCatalog() } returns mCatalog
    every { mResetConfig.getResetSourceConfiguration() } returns
      ResetSourceConfiguration().withStreamsToReset(
        listOf<StreamDescriptor?>(StreamDescriptor().withName("rfrStream")),
      )

    every { jobPersistence.getJob(JOB_ID) } returns job
    every { path.resolve(any<String>()) } returns path

    val expectedRoot = TemporalUtils.getJobRoot(path, JOB_ID.toString(), ATTEMPT_NUMBER.toLong())
    val expectedLogPath: Path = expectedRoot.resolve(DEFAULT_LOG_FILENAME)

    every { jobPersistence.createAttempt(JOB_ID, expectedLogPath) } returns attemptNumber
    every {
      ffClient.boolVariation(
        any<Flag<Boolean>>(),
        any<Context>(),
      )
    } returns true
    val destinationId = UUID.randomUUID()
    every { connectionService.getStandardSync(connId) } returns StandardSync().withDestinationId(destinationId)
    every { destinationService.getDestinationConnection(destinationId) } returns DestinationConnection().withWorkspaceId(WORKSPACE_ID)
    every {
      actorDefinitionVersionHelper.getDestinationVersion(
        any<StandardDestinationDefinition>(),
        any<UUID>(),
        any<UUID>(),
      )
    } returns ActorDefinitionVersion().withSupportsRefreshes(true)
    val output = handler.createNewAttemptNumber(JOB_ID)
    org.assertj.core.api.Assertions
      .assertThat(output.attemptNumber)
      .isEqualTo(attemptNumber)
    verify {
      generationBumper.updateGenerationForStreams(
        connId,
        JOB_ID,
        mutableListOf<StreamRefresh>(),
        setOf(
          StreamDescriptor().withName("rfrStream"),
        ),
      )
    }
    verify { statePersistence.bulkDelete(connId, setOf(StreamDescriptor().withName("rfrStream"))) }
  }

  @ParameterizedTest
  @ValueSource(ints = [0, 2])
  fun createAttemptNumberForRefresh(attemptNumber: Int) {
    val connId = UUID.randomUUID()

    val mConfig = mockk<JobConfig>()
    every { mConfig.getConfigType() } returns ConfigType.REFRESH

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

    val mRefreshConfig = mockk<RefreshConfig>()
    every { mConfig.getRefresh() } returns mRefreshConfig
    every { mRefreshConfig.getWorkspaceId() } returns UUID.randomUUID()
    every { mRefreshConfig.getStreamsToRefresh() } returns emptyList()

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
    every { mRefreshConfig.getConfiguredAirbyteCatalog() } returns mCatalog

    every { jobPersistence.getJob(JOB_ID) } returns job
    every { path.resolve(any<String>()) } returns path

    val expectedRoot = TemporalUtils.getJobRoot(path, JOB_ID.toString(), ATTEMPT_NUMBER.toLong())
    val expectedLogPath: Path = expectedRoot.resolve(DEFAULT_LOG_FILENAME)

    every { jobPersistence.createAttempt(JOB_ID, expectedLogPath) } returns attemptNumber
    every {
      ffClient.boolVariation(
        any<Flag<Boolean>>(),
        any<Context>(),
      )
    } returns true
    every {
      ffClient.boolVariation(
        eq<EnableResumableFullRefresh>(EnableResumableFullRefresh),
        any<Context>(),
      )
    } returns true
    val destinationId = UUID.randomUUID()
    every { connectionService.getStandardSync(connId) } returns StandardSync().withDestinationId(destinationId)
    every { destinationService.getDestinationConnection(destinationId) } returns DestinationConnection().withWorkspaceId(WORKSPACE_ID)
    every {
      actorDefinitionVersionHelper.getDestinationVersion(
        any<StandardDestinationDefinition>(),
        any<UUID>(),
        any<UUID>(),
      )
    } returns ActorDefinitionVersion().withSupportsRefreshes(true)
    val output = handler.createNewAttemptNumber(JOB_ID)
    org.assertj.core.api.Assertions
      .assertThat(output.attemptNumber)
      .isEqualTo(attemptNumber)
    if (attemptNumber == 0) {
      verify {
        generationBumper.updateGenerationForStreams(
          connId,
          JOB_ID,
          mutableListOf<StreamRefresh>(),
          setOf(
            StreamDescriptor().withName("rfrStream"),
            StreamDescriptor().withName("nonRfrStream"),
          ),
        )
      }
      verify {
        statePersistence.bulkDelete(
          connId,
          setOf(
            StreamDescriptor().withName("rfrStream"),
            StreamDescriptor().withName("nonRfrStream"),
          ),
        )
      }
    } else {
      verify {
        generationBumper.updateGenerationForStreams(
          connId,
          JOB_ID,
          mutableListOf<StreamRefresh>(),
          setOf(
            StreamDescriptor().withName("nonRfrStream"),
          ),
        )
      }
      verify { statePersistence.bulkDelete(connId, setOf(StreamDescriptor().withName("nonRfrStream"))) }
    }
  }

  @Nested
  internal inner class ClearFullRefreshStreamStateFirstAttempt {
    @ParameterizedTest
    @ValueSource(booleans = [true, false])
    fun getFullRefreshStreamsShouldOnlyReturnFullRefreshStreams(enableResumableFullRefresh: Boolean) {
      val mJobConfig = mockk<JobConfig>()

      val mSyncConfig = mockk<JobSyncConfig>()
      every { mJobConfig.getSync() } returns mSyncConfig

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
      every { mSyncConfig.getConfiguredAirbyteCatalog() } returns mCatalog

      val streams = handler.getFullRefreshStreamsToClear(mCatalog, enableResumableFullRefresh)
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
    fun createAttemptShouldAlwaysDeleteFullRefreshStreamState(
      attemptNumber: Int,
      enableResumableFullRefresh: Boolean,
    ) {
      val connId = UUID.randomUUID()

      val mConfig = mockk<JobConfig>()
      every { mConfig.getConfigType() } returns ConfigType.SYNC

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

      val mSyncConfig = mockk<JobSyncConfig>()
      every { mConfig.getSync() } returns mSyncConfig
      every { mSyncConfig.getWorkspaceId() } returns UUID.randomUUID()

      every { jobPersistence.getJob(JOB_ID) } returns job

      every { path.resolve(any<String>()) } returns path
      every {
        ffClient.boolVariation(
          any<Flag<Boolean>>(),
          any<Context>(),
        )
      } returns true
      every {
        ffClient.boolVariation(
          eq<EnableResumableFullRefresh>(EnableResumableFullRefresh),
          any<Context>(),
        )
      } returns enableResumableFullRefresh
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
      every { mSyncConfig.getConfiguredAirbyteCatalog() } returns mCatalog

      every { jobPersistence.createAttempt(JOB_ID, expectedLogPath) } returns attemptNumber

      val destinationId = UUID.randomUUID()
      every { connectionService.getStandardSync(connId) } returns StandardSync().withDestinationId(destinationId)
      every { destinationService.getDestinationConnection(destinationId) } returns DestinationConnection().withWorkspaceId(WORKSPACE_ID)
      every {
        actorDefinitionVersionHelper.getDestinationVersion(
          any<StandardDestinationDefinition>(),
          any<UUID>(),
          any<UUID>(),
        )
      } returns ActorDefinitionVersion().withSupportsRefreshes(true)

      val output = handler.createNewAttemptNumber(JOB_ID)
      org.assertj.core.api.Assertions
        .assertThat(output.attemptNumber)
        .isEqualTo(attemptNumber)

      val captor1 = slot<UUID>()
      val captor2 = slot<MutableSet<StreamDescriptor>>()
      verify { statePersistence.bulkDelete(capture(captor1), capture(captor2)) }
      Assertions.assertEquals(connId, captor1.captured)
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
        Assertions.assertEquals(nonResumableFullRefresh, captor2.captured)
        verify { generationBumper.updateGenerationForStreams(connId, JOB_ID, mutableListOf<StreamRefresh>(), nonResumableFullRefresh) }
      } else {
        Assertions.assertEquals(
          setOf(StreamDescriptor().withName("full"), StreamDescriptor().withName("full").withNamespace("name")),
          captor2.captured,
        )
      }
    }
  }

  @Test
  fun createAttemptNumberWithUnknownJobId() {
    val job = mockk<Job>()
    every { job.getAttemptsCount() } returns ATTEMPT_NUMBER

    every { jobPersistence.getJob(JOB_ID) } throws RuntimeException("unknown jobId $JOB_ID")

    org.assertj.core.api.Assertions
      .assertThatThrownBy { handler.createNewAttemptNumber(JOB_ID) }
      .isInstanceOf(UnprocessableContentException::class.java)
  }

  @Test
  fun getAttemptThrowsNotFound() {
    every { jobPersistence.getAttemptForJob(any(), any()) } returns Optional.empty<Attempt>()

    Assertions.assertThrows(
      IdNotFoundKnownException::class.java,
    ) { handler.getAttemptForJob(1L, 2) }
  }

  @Test
  fun getAttemptCombinedStatsThrowsNotFound() {
    every { jobPersistence.getAttemptCombinedStats(any(), any()) } returns null

    Assertions.assertThrows(
      IdNotFoundKnownException::class.java,
    ) { handler.getAttemptCombinedStats(1L, 2) }
  }

  @Test
  fun getAttemptCombinedStatsReturnsStats() {
    val stats = SyncStats()
    stats.setRecordsEmitted(123L)
    stats.setBytesEmitted(123L)
    stats.setBytesCommitted(123L)
    stats.setRecordsCommitted(123L)
    stats.setEstimatedRecords(123L)
    stats.setEstimatedBytes(123L)

    every { jobPersistence.getAttemptCombinedStats(any(), any()) } returns stats

    val result = handler.getAttemptCombinedStats(1L, 2)
    Assertions.assertEquals(stats.recordsEmitted, result.recordsEmitted)
    Assertions.assertEquals(stats.bytesEmitted, result.bytesEmitted)
    Assertions.assertEquals(stats.bytesCommitted, result.bytesCommitted)
    Assertions.assertEquals(stats.recordsCommitted, result.recordsCommitted)
    Assertions.assertEquals(stats.estimatedRecords, result.estimatedRecords)
    Assertions.assertEquals(stats.estimatedBytes, result.estimatedBytes)
    Assertions.assertNull(result.stateMessagesEmitted) // punting on this for now
  }

  @Test
  fun failAttemptSyncSummaryOutputPresent() {
    every { jobPersistence.getJob(JOB_ID) } returns mockk()
    handler.failAttempt(ATTEMPT_NUMBER, JOB_ID, failureSummary, standardSyncOutput)

    verify { jobPersistence.failAttempt(JOB_ID, ATTEMPT_NUMBER) }
    verify { jobPersistence.writeOutput(JOB_ID, ATTEMPT_NUMBER, jobOutput) }
    verify { jobPersistence.writeAttemptFailureSummary(JOB_ID, ATTEMPT_NUMBER, failureSummary) }
  }

  @Test
  fun failAttemptSyncSummaryOutputNotPresent() {
    every { jobPersistence.getJob(JOB_ID) } returns mockk()
    handler.failAttempt(ATTEMPT_NUMBER, JOB_ID, failureSummary, null)

    verify { jobPersistence.failAttempt(JOB_ID, ATTEMPT_NUMBER) }
    verify(exactly = 0) { jobPersistence.writeOutput(JOB_ID, ATTEMPT_NUMBER, jobOutput) }
    verify { jobPersistence.writeAttemptFailureSummary(JOB_ID, ATTEMPT_NUMBER, failureSummary) }
  }

  @Test
  fun failAttemptSyncSummaryNotPresent() {
    every { jobPersistence.getJob(JOB_ID) } returns mockk()
    handler.failAttempt(ATTEMPT_NUMBER, JOB_ID, null, standardSyncOutput)

    verify { jobPersistence.failAttempt(JOB_ID, ATTEMPT_NUMBER) }
    verify { jobPersistence.writeOutput(JOB_ID, ATTEMPT_NUMBER, jobOutput) }
    verify { jobPersistence.writeAttemptFailureSummary(JOB_ID, ATTEMPT_NUMBER, null) }
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
        SaveStreamAttemptMetadataRequestBody(
          jobId = jobId,
          attemptNumber = attemptNumber,
          streamMetadata =
            listOf(
              StreamAttemptMetadata(streamName = "s1", wasBackfilled = false, wasResumed = true),
              StreamAttemptMetadata(
                streamName = "s2",
                streamNamespace = "ns",
                wasBackfilled = true,
                wasResumed = false,
              ),
            ),
        ),
      )
    verify {
      streamAttemptMetadataService.upsertStreamAttemptMetadata(
        jobId,
        attemptNumber.toLong(),
        listOf(
          io.airbyte.data.services
            .StreamAttemptMetadata("s1", null, false, true),
          io.airbyte.data.services
            .StreamAttemptMetadata("s2", "ns", true, false),
        ),
      )
    }
    Assertions.assertEquals(InternalOperationResult(succeeded = true), result)
  }

  @Test
  fun saveStreamMetadataFailure() {
    val jobId = 123L
    val attemptNumber = 1

    every { streamAttemptMetadataService.upsertStreamAttemptMetadata(any(), any(), any()) } throws RuntimeException("oops")

    val result =
      handler.saveStreamMetadata(
        SaveStreamAttemptMetadataRequestBody(
          jobId = jobId,
          attemptNumber = attemptNumber,
          streamMetadata =
            listOf(
              StreamAttemptMetadata(streamName = "s", wasBackfilled = false, wasResumed = false),
            ),
        ),
      )
    Assertions.assertEquals(InternalOperationResult(succeeded = false), result)
  }

  @ParameterizedTest
  @MethodSource("testStateClearingLogicProvider")
  fun testStateClearingLogic(
    attemptNumber: Int,
    supportsRefresh: Boolean,
    expectedStreamsToClear: Set<StreamDescriptor>,
  ) {
    every {
      ffClient.boolVariation(
        EnableResumableFullRefresh,
        any(),
      )
    } returns true
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
    verify { statePersistence.bulkDelete(connectionId, expectedStreamsToClear) }
  }

  @Test
  fun testSaveStats() {
    val attemptStats =
      AttemptStats(
        bytesEmitted = 123L,
        bytesCommitted = 223L,
        estimatedBytes = 323L,
        estimatedRecords = 423L,
        recordsEmitted = 523L,
        recordsCommitted = 623L,
        recordsRejected = 723L,
        stateMessagesEmitted = 823L,
        additionalStats = mapOf("additional" to 923L.toBigDecimal()),
      )
    val attemptStreamStats =
      AttemptStreamStats(
        streamName = "streamName",
        streamNamespace = "streamNamespace",
        stats = attemptStats,
      )
    val saveStatsRequest =
      SaveStatsRequestBody(
        connectionId = CONNECTION_ID,
        jobId = JOB_ID,
        attemptNumber = ATTEMPT_NUMBER,
        stats = attemptStats,
        streamStats = listOf(attemptStreamStats),
      )

    val expectedStreamSyncStats = StreamSyncStats()
    expectedStreamSyncStats.streamName = attemptStreamStats.streamName
    expectedStreamSyncStats.streamNamespace = attemptStreamStats.streamNamespace
    expectedStreamSyncStats.stats =
      SyncStats()
        .withBytesEmitted(attemptStreamStats.stats.bytesEmitted)
        .withRecordsEmitted(attemptStreamStats.stats.recordsEmitted)
        .withBytesCommitted(attemptStreamStats.stats.bytesCommitted)
        .withRecordsCommitted(attemptStreamStats.stats.recordsCommitted)
        .withRecordsRejected(attemptStreamStats.stats.recordsRejected)
        .withEstimatedBytes(attemptStreamStats.stats.estimatedBytes)
        .withEstimatedRecords(attemptStreamStats.stats.estimatedRecords)
        .withAdditionalStats(attemptStreamStats.stats.additionalStats)

    handler.saveStats(saveStatsRequest)

    verify {
      jobPersistence.writeStats(
        jobId = JOB_ID,
        attemptNumber = ATTEMPT_NUMBER,
        estimatedRecords = attemptStats.estimatedRecords,
        estimatedBytes = attemptStats.estimatedBytes,
        bytesEmitted = attemptStats.bytesEmitted,
        recordsEmitted = attemptStats.recordsEmitted,
        recordsCommitted = attemptStats.recordsCommitted,
        recordsRejected = attemptStats.recordsRejected,
        bytesCommitted = attemptStats.bytesCommitted,
        connectionId = CONNECTION_ID,
        streamStats = listOf(expectedStreamSyncStats),
      )
    }
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
    private fun testStateClearingLogicProvider() =
      listOf(
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
      streamNames
        .toList()
        .map { n: String? -> StreamDescriptor().withName(n) }
        .toMutableSet()

    @JvmStatic
    private fun provideCreateAttemptConfig() =
      listOf(
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
    private fun randomObjects() =
      listOf(
        Arguments.of(123L),
        Arguments.of(true),
        Arguments.of(mutableListOf<String?>("123", "123")),
        Arguments.of("a string"),
        Arguments.of(543.0),
      )
  }
}
