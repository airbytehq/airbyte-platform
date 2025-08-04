/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.persistence.job

import com.fasterxml.jackson.databind.JsonNode
import com.google.common.collect.ImmutableMap
import io.airbyte.commons.json.Jsons.clone
import io.airbyte.commons.json.Jsons.deserialize
import io.airbyte.commons.json.Jsons.emptyObject
import io.airbyte.commons.json.Jsons.jsonNode
import io.airbyte.commons.json.Jsons.serialize
import io.airbyte.commons.version.Version
import io.airbyte.config.ActorDefinitionVersion
import io.airbyte.config.AirbyteStream
import io.airbyte.config.ConfiguredAirbyteCatalog
import io.airbyte.config.ConfiguredAirbyteStream
import io.airbyte.config.DestinationConnection
import io.airbyte.config.DestinationSyncMode
import io.airbyte.config.JobConfig
import io.airbyte.config.JobConfig.ConfigType
import io.airbyte.config.JobResetConnectionConfig
import io.airbyte.config.JobSyncConfig
import io.airbyte.config.JobTypeResourceLimit
import io.airbyte.config.RefreshConfig
import io.airbyte.config.RefreshStream
import io.airbyte.config.ResetSourceConfiguration
import io.airbyte.config.ResourceRequirements
import io.airbyte.config.ResourceRequirementsType
import io.airbyte.config.ScopedResourceRequirements
import io.airbyte.config.SourceConnection
import io.airbyte.config.StandardDestinationDefinition
import io.airbyte.config.StandardSourceDefinition
import io.airbyte.config.StandardSync
import io.airbyte.config.StandardSyncOperation
import io.airbyte.config.StateType
import io.airbyte.config.StateWrapper
import io.airbyte.config.StreamDescriptor
import io.airbyte.config.SyncMode
import io.airbyte.config.SyncResourceRequirements
import io.airbyte.config.SyncResourceRequirementsKey
import io.airbyte.config.helpers.CatalogHelpers.Companion.createAirbyteStream
import io.airbyte.config.persistence.StatePersistence
import io.airbyte.config.persistence.StreamRefreshesRepository
import io.airbyte.config.persistence.domain.StreamRefresh
import io.airbyte.config.provider.ResourceRequirementsProvider
import io.airbyte.db.instance.configs.jooq.generated.enums.RefreshType
import io.airbyte.featureflag.DestResourceOverrides
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.featureflag.OrchestratorResourceOverrides
import io.airbyte.featureflag.SourceResourceOverrides
import io.airbyte.featureflag.TestClient
import io.airbyte.protocol.models.JsonSchemaType
import io.airbyte.protocol.models.v0.AirbyteStateMessage
import io.airbyte.protocol.models.v0.Field
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.function.Executable
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.EnumSource
import org.junit.jupiter.params.provider.MethodSource
import org.junit.jupiter.params.provider.ValueSource
import org.junit.platform.commons.util.StringUtils
import org.mockito.kotlin.any
import org.mockito.kotlin.anyOrNull
import org.mockito.kotlin.argumentCaptor
import org.mockito.kotlin.eq
import org.mockito.kotlin.mock
import org.mockito.kotlin.times
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import java.io.IOException
import java.util.List
import java.util.Map
import java.util.Optional
import java.util.UUID
import java.util.stream.Stream

internal class DefaultJobCreatorTest {
  private lateinit var jobPersistence: JobPersistence
  private lateinit var statePersistence: StatePersistence
  private lateinit var jobCreator: DefaultJobCreator
  private lateinit var resourceRequirementsProvider: ResourceRequirementsProvider
  private lateinit var workerResourceRequirements: ResourceRequirements
  private lateinit var sourceResourceRequirements: ResourceRequirements
  private lateinit var destResourceRequirements: ResourceRequirements
  private lateinit var mFeatureFlagClient: FeatureFlagClient

  private lateinit var streamRefreshesRepository: StreamRefreshesRepository

  @BeforeEach
  fun setup() {
    jobPersistence = mock<JobPersistence>()
    statePersistence = mock<StatePersistence>()
    workerResourceRequirements =
      ResourceRequirements()
        .withCpuLimit("0.2")
        .withCpuRequest("0.2")
        .withMemoryLimit("200Mi")
        .withMemoryRequest("200Mi")
    sourceResourceRequirements =
      ResourceRequirements()
        .withCpuLimit("0.1")
        .withCpuRequest("0.1")
        .withMemoryLimit("400Mi")
        .withMemoryRequest("300Mi")
    destResourceRequirements =
      ResourceRequirements()
        .withCpuLimit("0.3")
        .withCpuRequest("0.3")
        .withMemoryLimit("1200Mi")
        .withMemoryRequest("1000Mi")
    resourceRequirementsProvider = mock<ResourceRequirementsProvider>()
    whenever(
      resourceRequirementsProvider.getResourceRequirements(
        eq(ResourceRequirementsType.ORCHESTRATOR),
        anyOrNull<String>(),
        any<String>(),
      ),
    ).thenReturn(workerResourceRequirements)
    whenever(
      resourceRequirementsProvider.getResourceRequirements(
        eq(ResourceRequirementsType.SOURCE),
        anyOrNull<String>(),
        any<String>(),
      ),
    ).thenReturn(workerResourceRequirements)
    whenever(
      resourceRequirementsProvider.getResourceRequirements(
        eq(ResourceRequirementsType.DESTINATION),
        anyOrNull<String>(),
        any<String>(),
      ),
    ).thenReturn(workerResourceRequirements)
    streamRefreshesRepository = mock<StreamRefreshesRepository>()
    mFeatureFlagClient = TestClient()
    jobCreator =
      DefaultJobCreator(jobPersistence, resourceRequirementsProvider, mFeatureFlagClient, streamRefreshesRepository, null)
  }

  @ParameterizedTest
  @EnumSource(RefreshStream.RefreshType::class)
  @Throws(IOException::class)
  fun testCreateRefreshJob(refreshType: RefreshStream.RefreshType?) {
    val streamToRefresh = "name"
    val streamNamespace = "namespace"

    whenever(
      jobPersistence.enqueueJob(
        any<String>(),
        any<JobConfig>(),
        any<Boolean>(),
      ),
    ).thenReturn(Optional.of(1L))

    val stateWrapper =
      StateWrapper()
        .withStateType(StateType.STREAM)
        .withStateMessages(mutableListOf<AirbyteStateMessage?>())

    whenever(statePersistence.getCurrentState(STANDARD_SYNC.connectionId))
      .thenReturn(Optional.of(stateWrapper))

    jobCreator =
      DefaultJobCreator(jobPersistence, resourceRequirementsProvider, mFeatureFlagClient, streamRefreshesRepository, null)

    val expectedSourceType: Optional<String> = Optional.of("database")

    mockResourcesRequirement(expectedSourceType)

    val expectedSyncResourceRequirements = this.expectedResourcesRequirement

    val refreshConfig: RefreshConfig =
      getRefreshConfig(
        expectedSyncResourceRequirements,
        listOf(
          RefreshStream()
            .withRefreshType(refreshType)
            .withStreamDescriptor(StreamDescriptor().withName(streamToRefresh).withNamespace(streamNamespace)),
        ) as java.util.List<RefreshStream>,
      )

    val jobConfig =
      JobConfig()
        .withConfigType(ConfigType.REFRESH)
        .withRefresh(refreshConfig)

    val expectedScope = STANDARD_SYNC.connectionId.toString()
    whenever(jobPersistence.enqueueJob(expectedScope, jobConfig, true)).thenReturn(Optional.of(JOB_ID))

    val expectedRefreshType = if (refreshType == RefreshStream.RefreshType.TRUNCATE) RefreshType.TRUNCATE else RefreshType.MERGE
    val refreshes =
      listOf(
        StreamRefresh(
          UUID.randomUUID(),
          STANDARD_SYNC.connectionId,
          streamToRefresh,
          streamNamespace,
          null,
          expectedRefreshType,
        ),
      )

    jobCreator.createRefreshConnection(
      SOURCE_CONNECTION,
      DESTINATION_CONNECTION,
      STANDARD_SYNC,
      SOURCE_IMAGE_NAME,
      SOURCE_PROTOCOL_VERSION,
      DESTINATION_IMAGE_NAME,
      DESTINATION_PROTOCOL_VERSION,
      listOf(STANDARD_SYNC_OPERATION),
      PERSISTED_WEBHOOK_CONFIGS,
      STANDARD_SOURCE_DEFINITION_WITH_SOURCE_TYPE,
      STANDARD_DESTINATION_DEFINITION,
      SOURCE_DEFINITION_VERSION,
      DESTINATION_DEFINITION_VERSION.withSupportsRefreshes(true),
      WORKSPACE_ID,
      refreshes,
    )

    verify(jobPersistence).enqueueJob(expectedScope, jobConfig, false)
  }

  @Test
  fun testFailToCreateRefreshIfNotAllowed() {
    val mFeatureFlagClient: FeatureFlagClient = TestClient()
    jobCreator =
      DefaultJobCreator(jobPersistence, resourceRequirementsProvider, mFeatureFlagClient, streamRefreshesRepository, null)

    Assertions.assertThrows<IllegalStateException?>(
      IllegalStateException::class.java,
      Executable {
        jobCreator.createRefreshConnection(
          SOURCE_CONNECTION,
          DESTINATION_CONNECTION,
          STANDARD_SYNC,
          SOURCE_IMAGE_NAME,
          SOURCE_PROTOCOL_VERSION,
          DESTINATION_IMAGE_NAME,
          DESTINATION_PROTOCOL_VERSION,
          listOf(STANDARD_SYNC_OPERATION),
          PERSISTED_WEBHOOK_CONFIGS,
          STANDARD_SOURCE_DEFINITION_WITH_SOURCE_TYPE,
          STANDARD_DESTINATION_DEFINITION,
          SOURCE_DEFINITION_VERSION,
          DESTINATION_DEFINITION_VERSION.withSupportsRefreshes(false),
          WORKSPACE_ID,
          mutableListOf<StreamRefresh>(),
        )
      },
    )
  }

  @ParameterizedTest
  @ValueSource(booleans = [true, false])
  @Throws(IOException::class)
  fun testCreateSyncJob(isScheduled: Boolean) {
    val expectedSourceType: Optional<String> = Optional.of("database")

    mockResourcesRequirement(expectedSourceType)

    val expectedSyncResourceRequirements = this.expectedResourcesRequirement

    val jobSyncConfig =
      JobSyncConfig()
        .withNamespaceDefinition(STANDARD_SYNC.namespaceDefinition)
        .withNamespaceFormat(STANDARD_SYNC.namespaceFormat)
        .withPrefix(STANDARD_SYNC.prefix)
        .withSourceDockerImage(SOURCE_IMAGE_NAME)
        .withSourceDockerImageIsDefault(SOURCE_IMAGE_IS_DEFAULT)
        .withSourceProtocolVersion(SOURCE_PROTOCOL_VERSION)
        .withDestinationDockerImage(DESTINATION_IMAGE_NAME)
        .withDestinationDockerImageIsDefault(DESTINATION_IMAGE_IS_DEFAULT)
        .withDestinationProtocolVersion(DESTINATION_PROTOCOL_VERSION)
        .withConfiguredAirbyteCatalog(STANDARD_SYNC.catalog)
        .withOperationSequence(listOf(STANDARD_SYNC_OPERATION))
        .withSyncResourceRequirements(expectedSyncResourceRequirements)
        .withWebhookOperationConfigs(PERSISTED_WEBHOOK_CONFIGS)
        .withIsSourceCustomConnector(false)
        .withIsDestinationCustomConnector(false)
        .withWorkspaceId(WORKSPACE_ID)
        .withSourceDefinitionVersionId(SOURCE_DEFINITION_VERSION.versionId)
        .withDestinationDefinitionVersionId(DESTINATION_DEFINITION_VERSION.versionId)

    val jobConfig =
      JobConfig()
        .withConfigType(ConfigType.SYNC)
        .withSync(jobSyncConfig)

    val expectedScope = STANDARD_SYNC.connectionId.toString()
    whenever(jobPersistence.enqueueJob(expectedScope, jobConfig, isScheduled)).thenReturn(Optional.of(JOB_ID))

    jobCreator.createSyncJob(
      SOURCE_CONNECTION,
      DESTINATION_CONNECTION,
      STANDARD_SYNC,
      SOURCE_IMAGE_NAME,
      SOURCE_IMAGE_IS_DEFAULT,
      SOURCE_PROTOCOL_VERSION,
      DESTINATION_IMAGE_NAME,
      DESTINATION_IMAGE_IS_DEFAULT,
      DESTINATION_PROTOCOL_VERSION,
      listOf(STANDARD_SYNC_OPERATION),
      PERSISTED_WEBHOOK_CONFIGS,
      STANDARD_SOURCE_DEFINITION_WITH_SOURCE_TYPE,
      STANDARD_DESTINATION_DEFINITION,
      SOURCE_DEFINITION_VERSION,
      DESTINATION_DEFINITION_VERSION,
      WORKSPACE_ID,
      isScheduled,
    )

    verify(jobPersistence).enqueueJob(expectedScope, jobConfig, isScheduled)
  }

  private fun mockResourcesRequirement(expectedSourceType: Optional<String>) {
    whenever(
      resourceRequirementsProvider!!.getResourceRequirements(
        ResourceRequirementsType.ORCHESTRATOR,
        expectedSourceType.orElse(null),
        DEFAULT_VARIANT,
      ),
    ).thenReturn(workerResourceRequirements)
    whenever(
      resourceRequirementsProvider!!.getResourceRequirements(
        ResourceRequirementsType.SOURCE,
        expectedSourceType.orElse(null),
        DEFAULT_VARIANT,
      ),
    ).thenReturn(sourceResourceRequirements)
    whenever(
      resourceRequirementsProvider!!.getResourceRequirements(
        ResourceRequirementsType.DESTINATION,
        expectedSourceType.orElse(null),
        DEFAULT_VARIANT,
      ),
    ).thenReturn(destResourceRequirements)
  }

  private val expectedResourcesRequirement: SyncResourceRequirements?
    get() =
      SyncResourceRequirements()
        .withConfigKey(SyncResourceRequirementsKey().withVariant(DEFAULT_VARIANT).withSubType("database"))
        .withDestination(destResourceRequirements)
        .withOrchestrator(workerResourceRequirements)
        .withSource(sourceResourceRequirements)

  @Test
  @Throws(IOException::class)
  fun testCreateSyncJobEnsureNoQueuing() {
    val jobSyncConfig =
      JobSyncConfig()
        .withNamespaceDefinition(STANDARD_SYNC.namespaceDefinition)
        .withNamespaceFormat(STANDARD_SYNC.namespaceFormat)
        .withPrefix(STANDARD_SYNC.prefix)
        .withSourceDockerImage(SOURCE_IMAGE_NAME)
        .withSourceDockerImageIsDefault(SOURCE_IMAGE_IS_DEFAULT)
        .withDestinationProtocolVersion(SOURCE_PROTOCOL_VERSION)
        .withDestinationDockerImage(DESTINATION_IMAGE_NAME)
        .withDestinationDockerImageIsDefault(DESTINATION_IMAGE_IS_DEFAULT)
        .withDestinationProtocolVersion(DESTINATION_PROTOCOL_VERSION)
        .withConfiguredAirbyteCatalog(STANDARD_SYNC.catalog)
        .withOperationSequence(listOf(STANDARD_SYNC_OPERATION))
        .withSourceDefinitionVersionId(SOURCE_DEFINITION_VERSION.versionId)
        .withDestinationDefinitionVersionId(DESTINATION_DEFINITION_VERSION.versionId)

    val jobConfig =
      JobConfig()
        .withConfigType(ConfigType.SYNC)
        .withSync(jobSyncConfig)

    val expectedScope = STANDARD_SYNC.connectionId.toString()
    whenever(jobPersistence.enqueueJob(expectedScope, jobConfig, true)).thenReturn(Optional.empty())

    Assertions.assertTrue(
      jobCreator
        .createSyncJob(
          SOURCE_CONNECTION,
          DESTINATION_CONNECTION,
          STANDARD_SYNC,
          SOURCE_IMAGE_NAME,
          SOURCE_IMAGE_IS_DEFAULT,
          SOURCE_PROTOCOL_VERSION,
          DESTINATION_IMAGE_NAME,
          DESTINATION_IMAGE_IS_DEFAULT,
          DESTINATION_PROTOCOL_VERSION,
          listOf(STANDARD_SYNC_OPERATION),
          null,
          STANDARD_SOURCE_DEFINITION,
          STANDARD_DESTINATION_DEFINITION,
          SOURCE_DEFINITION_VERSION,
          DESTINATION_DEFINITION_VERSION,
          UUID.randomUUID(),
          true,
        ).isEmpty(),
    )
  }

  @Test
  @Throws(IOException::class)
  fun testCreateSyncJobDefaultWorkerResourceReqs() {
    jobCreator.createSyncJob(
      SOURCE_CONNECTION,
      DESTINATION_CONNECTION,
      STANDARD_SYNC,
      SOURCE_IMAGE_NAME,
      SOURCE_IMAGE_IS_DEFAULT,
      SOURCE_PROTOCOL_VERSION,
      DESTINATION_IMAGE_NAME,
      DESTINATION_IMAGE_IS_DEFAULT,
      DESTINATION_PROTOCOL_VERSION,
      listOf(STANDARD_SYNC_OPERATION),
      null,
      STANDARD_SOURCE_DEFINITION,
      STANDARD_DESTINATION_DEFINITION,
      SOURCE_DEFINITION_VERSION,
      DESTINATION_DEFINITION_VERSION,
      WORKSPACE_ID,
      true,
    )

    val expectedSyncResourceRequirements =
      SyncResourceRequirements()
        .withConfigKey(SyncResourceRequirementsKey().withVariant(DEFAULT_VARIANT))
        .withDestination(workerResourceRequirements)
        .withOrchestrator(workerResourceRequirements)
        .withSource(workerResourceRequirements)

    val expectedJobSyncConfig =
      JobSyncConfig()
        .withNamespaceDefinition(STANDARD_SYNC.namespaceDefinition)
        .withNamespaceFormat(STANDARD_SYNC.namespaceFormat)
        .withPrefix(STANDARD_SYNC.prefix)
        .withSourceDockerImage(SOURCE_IMAGE_NAME)
        .withSourceDockerImageIsDefault(SOURCE_IMAGE_IS_DEFAULT)
        .withSourceProtocolVersion(SOURCE_PROTOCOL_VERSION)
        .withDestinationDockerImage(DESTINATION_IMAGE_NAME)
        .withDestinationDockerImageIsDefault(DESTINATION_IMAGE_IS_DEFAULT)
        .withDestinationProtocolVersion(DESTINATION_PROTOCOL_VERSION)
        .withConfiguredAirbyteCatalog(STANDARD_SYNC.catalog)
        .withOperationSequence(listOf(STANDARD_SYNC_OPERATION))
        .withSyncResourceRequirements(expectedSyncResourceRequirements)
        .withIsSourceCustomConnector(false)
        .withIsDestinationCustomConnector(false)
        .withWorkspaceId(WORKSPACE_ID)
        .withSourceDefinitionVersionId(SOURCE_DEFINITION_VERSION.versionId)
        .withDestinationDefinitionVersionId(DESTINATION_DEFINITION_VERSION.versionId)

    val expectedJobConfig =
      JobConfig()
        .withConfigType(ConfigType.SYNC)
        .withSync(expectedJobSyncConfig)

    val expectedScope = STANDARD_SYNC.connectionId.toString()

    verify(jobPersistence, times(1)).enqueueJob(expectedScope, expectedJobConfig, true)
  }

  @Test
  @Throws(IOException::class)
  fun testCreateSyncJobConnectionResourceReqs() {
    val standardSyncResourceRequirements =
      ResourceRequirements()
        .withCpuLimit("0.5")
        .withCpuRequest("0.5")
        .withMemoryLimit("500Mi")
        .withMemoryRequest("500Mi")
    val standardSync = clone<StandardSync>(STANDARD_SYNC).withResourceRequirements(standardSyncResourceRequirements)

    jobCreator.createSyncJob(
      SOURCE_CONNECTION,
      DESTINATION_CONNECTION,
      standardSync,
      SOURCE_IMAGE_NAME,
      SOURCE_IMAGE_IS_DEFAULT,
      SOURCE_PROTOCOL_VERSION,
      DESTINATION_IMAGE_NAME,
      DESTINATION_IMAGE_IS_DEFAULT,
      DESTINATION_PROTOCOL_VERSION,
      listOf(STANDARD_SYNC_OPERATION),
      null,
      STANDARD_SOURCE_DEFINITION,
      STANDARD_DESTINATION_DEFINITION,
      SOURCE_DEFINITION_VERSION,
      DESTINATION_DEFINITION_VERSION,
      WORKSPACE_ID,
      true,
    )

    val expectedSyncResourceRequirements =
      SyncResourceRequirements()
        .withConfigKey(SyncResourceRequirementsKey().withVariant(DEFAULT_VARIANT))
        .withDestination(standardSyncResourceRequirements)
        .withOrchestrator(standardSyncResourceRequirements)
        .withSource(standardSyncResourceRequirements)

    val expectedJobSyncConfig =
      JobSyncConfig()
        .withNamespaceDefinition(STANDARD_SYNC.namespaceDefinition)
        .withNamespaceFormat(STANDARD_SYNC.namespaceFormat)
        .withPrefix(STANDARD_SYNC.prefix)
        .withSourceDockerImage(SOURCE_IMAGE_NAME)
        .withSourceDockerImageIsDefault(SOURCE_IMAGE_IS_DEFAULT)
        .withSourceProtocolVersion(SOURCE_PROTOCOL_VERSION)
        .withDestinationDockerImage(DESTINATION_IMAGE_NAME)
        .withDestinationDockerImageIsDefault(DESTINATION_IMAGE_IS_DEFAULT)
        .withDestinationProtocolVersion(DESTINATION_PROTOCOL_VERSION)
        .withConfiguredAirbyteCatalog(STANDARD_SYNC.catalog)
        .withOperationSequence(listOf(STANDARD_SYNC_OPERATION))
        .withSyncResourceRequirements(expectedSyncResourceRequirements)
        .withIsSourceCustomConnector(false)
        .withIsDestinationCustomConnector(false)
        .withWorkspaceId(WORKSPACE_ID)
        .withSourceDefinitionVersionId(SOURCE_DEFINITION_VERSION.versionId)
        .withDestinationDefinitionVersionId(DESTINATION_DEFINITION_VERSION.versionId)

    val expectedJobConfig =
      JobConfig()
        .withConfigType(ConfigType.SYNC)
        .withSync(expectedJobSyncConfig)

    val expectedScope = STANDARD_SYNC.connectionId.toString()

    verify(jobPersistence, times(1)).enqueueJob(expectedScope, expectedJobConfig, true)
  }

  @Test
  @Throws(IOException::class)
  fun testCreateSyncJobSourceAndDestinationResourceReqs() {
    val sourceResourceRequirements =
      ResourceRequirements()
        .withCpuLimit("0.7")
        .withCpuRequest("0.7")
        .withMemoryLimit("700Mi")
        .withMemoryRequest("700Mi")
    val destResourceRequirements =
      ResourceRequirements()
        .withCpuLimit("0.8")
        .withCpuRequest("0.8")
        .withMemoryLimit("800Mi")
        .withMemoryRequest("800Mi")

    jobCreator.createSyncJob(
      SOURCE_CONNECTION,
      DESTINATION_CONNECTION,
      STANDARD_SYNC,
      SOURCE_IMAGE_NAME,
      SOURCE_IMAGE_IS_DEFAULT,
      SOURCE_PROTOCOL_VERSION,
      DESTINATION_IMAGE_NAME,
      DESTINATION_IMAGE_IS_DEFAULT,
      DESTINATION_PROTOCOL_VERSION,
      listOf(STANDARD_SYNC_OPERATION),
      null,
      StandardSourceDefinition()
        .withSourceDefinitionId(UUID.randomUUID())
        .withResourceRequirements(ScopedResourceRequirements().withDefault(sourceResourceRequirements)),
      StandardDestinationDefinition()
        .withDestinationDefinitionId(UUID.randomUUID())
        .withResourceRequirements(
          ScopedResourceRequirements().withJobSpecific(
            List.of<JobTypeResourceLimit?>(
              JobTypeResourceLimit().withJobType(JobTypeResourceLimit.JobType.SYNC).withResourceRequirements(destResourceRequirements),
            ),
          ),
        ),
      SOURCE_DEFINITION_VERSION,
      DESTINATION_DEFINITION_VERSION,
      WORKSPACE_ID,
      true,
    )

    val expectedSyncResourceRequirements =
      SyncResourceRequirements()
        .withConfigKey(SyncResourceRequirementsKey().withVariant(DEFAULT_VARIANT))
        .withDestination(destResourceRequirements)
        .withOrchestrator(workerResourceRequirements)
        .withSource(sourceResourceRequirements)

    val expectedJobSyncConfig =
      JobSyncConfig()
        .withNamespaceDefinition(STANDARD_SYNC.namespaceDefinition)
        .withNamespaceFormat(STANDARD_SYNC.namespaceFormat)
        .withPrefix(STANDARD_SYNC.prefix)
        .withSourceDockerImage(SOURCE_IMAGE_NAME)
        .withSourceDockerImageIsDefault(SOURCE_IMAGE_IS_DEFAULT)
        .withSourceProtocolVersion(SOURCE_PROTOCOL_VERSION)
        .withDestinationDockerImage(DESTINATION_IMAGE_NAME)
        .withDestinationDockerImageIsDefault(DESTINATION_IMAGE_IS_DEFAULT)
        .withDestinationProtocolVersion(DESTINATION_PROTOCOL_VERSION)
        .withConfiguredAirbyteCatalog(STANDARD_SYNC.catalog)
        .withOperationSequence(listOf(STANDARD_SYNC_OPERATION))
        .withSyncResourceRequirements(expectedSyncResourceRequirements)
        .withIsSourceCustomConnector(false)
        .withIsDestinationCustomConnector(false)
        .withWorkspaceId(WORKSPACE_ID)
        .withSourceDefinitionVersionId(SOURCE_DEFINITION_VERSION.versionId)
        .withDestinationDefinitionVersionId(DESTINATION_DEFINITION_VERSION.versionId)

    val expectedJobConfig =
      JobConfig()
        .withConfigType(ConfigType.SYNC)
        .withSync(expectedJobSyncConfig)

    val expectedScope = STANDARD_SYNC.connectionId.toString()

    verify(jobPersistence, times(1)).enqueueJob(expectedScope, expectedJobConfig, true)
  }

  @ParameterizedTest
  @MethodSource("resourceOverrideMatrix")
  @Throws(IOException::class)
  fun testDestinationResourceReqsOverrides(
    cpuReqOverride: String?,
    cpuLimitOverride: String?,
    memReqOverride: String?,
    memLimitOverride: String?,
  ) {
    val overrides = HashMap<Any?, Any?>()
    if (cpuReqOverride != null) {
      overrides.put("cpu_request", cpuReqOverride)
    }
    if (cpuLimitOverride != null) {
      overrides.put("cpu_limit", cpuLimitOverride)
    }
    if (memReqOverride != null) {
      overrides.put("memory_request", memReqOverride)
    }
    if (memLimitOverride != null) {
      overrides.put("memory_limit", memLimitOverride)
    }

    val originalReqs =
      ResourceRequirements()
        .withCpuLimit("0.8")
        .withCpuRequest("0.8")
        .withMemoryLimit("800Mi")
        .withMemoryRequest("800Mi")

    val jobCreator =
      DefaultJobCreator(
        jobPersistence!!,
        resourceRequirementsProvider!!,
        TestClient(Map.of<String, String?>(DestResourceOverrides.key, serialize<HashMap<Any?, Any?>?>(overrides))),
        streamRefreshesRepository!!,
        null,
      )

    jobCreator.createSyncJob(
      SOURCE_CONNECTION,
      DESTINATION_CONNECTION,
      STANDARD_SYNC,
      SOURCE_IMAGE_NAME,
      SOURCE_IMAGE_IS_DEFAULT,
      SOURCE_PROTOCOL_VERSION,
      DESTINATION_IMAGE_NAME,
      DESTINATION_IMAGE_IS_DEFAULT,
      DESTINATION_PROTOCOL_VERSION,
      listOf(STANDARD_SYNC_OPERATION),
      null,
      StandardSourceDefinition()
        .withSourceDefinitionId(UUID.randomUUID())
        .withResourceRequirements(ScopedResourceRequirements().withDefault(sourceResourceRequirements)),
      StandardDestinationDefinition()
        .withDestinationDefinitionId(UUID.randomUUID())
        .withResourceRequirements(
          ScopedResourceRequirements().withJobSpecific(
            List.of<JobTypeResourceLimit?>(
              JobTypeResourceLimit().withJobType(JobTypeResourceLimit.JobType.SYNC).withResourceRequirements(originalReqs),
            ),
          ),
        ),
      SOURCE_DEFINITION_VERSION,
      DESTINATION_DEFINITION_VERSION,
      WORKSPACE_ID,
      true,
    )

    val configCaptor = argumentCaptor<JobConfig>()
    verify(jobPersistence, times(1)).enqueueJob(any<String>(), configCaptor.capture(), eq(true))

    val destConfigValues =
      configCaptor.firstValue
        .getSync()
        .getSyncResourceRequirements()
        .getDestination()

    val expectedCpuReq = if (StringUtils.isNotBlank(cpuReqOverride)) cpuReqOverride else originalReqs.getCpuRequest()
    Assertions.assertEquals(expectedCpuReq, destConfigValues.getCpuRequest())

    val expectedCpuLimit = if (StringUtils.isNotBlank(cpuLimitOverride)) cpuLimitOverride else originalReqs.getCpuLimit()
    Assertions.assertEquals(expectedCpuLimit, destConfigValues.getCpuLimit())

    val expectedMemReq = if (StringUtils.isNotBlank(memReqOverride)) memReqOverride else originalReqs.getMemoryRequest()
    Assertions.assertEquals(expectedMemReq, destConfigValues.getMemoryRequest())

    val expectedMemLimit = if (StringUtils.isNotBlank(memLimitOverride)) memLimitOverride else originalReqs.getMemoryLimit()
    Assertions.assertEquals(expectedMemLimit, destConfigValues.getMemoryLimit())
  }

  @ParameterizedTest
  @MethodSource("resourceOverrideMatrix")
  @Throws(IOException::class)
  fun testOrchestratorResourceReqsOverrides(
    cpuReqOverride: String?,
    cpuLimitOverride: String?,
    memReqOverride: String?,
    memLimitOverride: String?,
  ) {
    val overrides = HashMap<Any?, Any?>()
    if (cpuReqOverride != null) {
      overrides.put("cpu_request", cpuReqOverride)
    }
    if (cpuLimitOverride != null) {
      overrides.put("cpu_limit", cpuLimitOverride)
    }
    if (memReqOverride != null) {
      overrides.put("memory_request", memReqOverride)
    }
    if (memLimitOverride != null) {
      overrides.put("memory_limit", memLimitOverride)
    }

    val originalReqs =
      ResourceRequirements()
        .withCpuLimit("0.8")
        .withCpuRequest("0.8")
        .withMemoryLimit("800Mi")
        .withMemoryRequest("800Mi")

    val jobCreator =
      DefaultJobCreator(
        jobPersistence!!,
        resourceRequirementsProvider!!,
        TestClient(Map.of<String, String?>(OrchestratorResourceOverrides.key, serialize<HashMap<Any?, Any?>?>(overrides))),
        streamRefreshesRepository!!,
        null,
      )

    val standardSync =
      StandardSync()
        .withConnectionId(UUID.randomUUID())
        .withName("presto to hudi")
        .withNamespaceDefinition(JobSyncConfig.NamespaceDefinitionType.SOURCE)
        .withNamespaceFormat(null)
        .withPrefix("presto_to_hudi")
        .withStatus(StandardSync.Status.ACTIVE)
        .withCatalog(CONFIGURED_AIRBYTE_CATALOG)
        .withSourceId(UUID.randomUUID())
        .withDestinationId(UUID.randomUUID())
        .withOperationIds(List.of<UUID?>(UUID.randomUUID()))
        .withResourceRequirements(originalReqs)

    jobCreator.createSyncJob(
      SOURCE_CONNECTION,
      DESTINATION_CONNECTION,
      standardSync,
      SOURCE_IMAGE_NAME,
      SOURCE_IMAGE_IS_DEFAULT,
      SOURCE_PROTOCOL_VERSION,
      DESTINATION_IMAGE_NAME,
      DESTINATION_IMAGE_IS_DEFAULT,
      DESTINATION_PROTOCOL_VERSION,
      listOf(STANDARD_SYNC_OPERATION),
      null,
      StandardSourceDefinition()
        .withSourceDefinitionId(UUID.randomUUID())
        .withResourceRequirements(ScopedResourceRequirements().withDefault(sourceResourceRequirements)),
      StandardDestinationDefinition()
        .withDestinationDefinitionId(UUID.randomUUID())
        .withResourceRequirements(ScopedResourceRequirements().withDefault(destResourceRequirements)),
      SOURCE_DEFINITION_VERSION,
      DESTINATION_DEFINITION_VERSION,
      WORKSPACE_ID,
      true,
    )

    val configCaptor = argumentCaptor<JobConfig>()
    verify(jobPersistence, times(1)).enqueueJob(any<String>(), configCaptor.capture(), eq(true))

    val orchestratorConfigValues =
      configCaptor.firstValue
        .getSync()
        .getSyncResourceRequirements()
        .getOrchestrator()

    val expectedCpuReq = if (StringUtils.isNotBlank(cpuReqOverride)) cpuReqOverride else originalReqs.getCpuRequest()
    Assertions.assertEquals(expectedCpuReq, orchestratorConfigValues.getCpuRequest())

    val expectedCpuLimit = if (StringUtils.isNotBlank(cpuLimitOverride)) cpuLimitOverride else originalReqs.getCpuLimit()
    Assertions.assertEquals(expectedCpuLimit, orchestratorConfigValues.getCpuLimit())

    val expectedMemReq = if (StringUtils.isNotBlank(memReqOverride)) memReqOverride else originalReqs.getMemoryRequest()
    Assertions.assertEquals(expectedMemReq, orchestratorConfigValues.getMemoryRequest())

    val expectedMemLimit = if (StringUtils.isNotBlank(memLimitOverride)) memLimitOverride else originalReqs.getMemoryLimit()
    Assertions.assertEquals(expectedMemLimit, orchestratorConfigValues.getMemoryLimit())
  }

  @ParameterizedTest
  @MethodSource("resourceOverrideMatrix")
  @Throws(IOException::class)
  fun testSourceResourceReqsOverrides(
    cpuReqOverride: String?,
    cpuLimitOverride: String?,
    memReqOverride: String?,
    memLimitOverride: String?,
  ) {
    val overrides = HashMap<Any?, Any?>()
    if (cpuReqOverride != null) {
      overrides.put("cpu_request", cpuReqOverride)
    }
    if (cpuLimitOverride != null) {
      overrides.put("cpu_limit", cpuLimitOverride)
    }
    if (memReqOverride != null) {
      overrides.put("memory_request", memReqOverride)
    }
    if (memLimitOverride != null) {
      overrides.put("memory_limit", memLimitOverride)
    }

    val originalReqs =
      ResourceRequirements()
        .withCpuLimit("0.8")
        .withCpuRequest("0.8")
        .withMemoryLimit("800Mi")
        .withMemoryRequest("800Mi")

    val jobCreator =
      DefaultJobCreator(
        jobPersistence!!,
        resourceRequirementsProvider!!,
        TestClient(Map.of<String, String?>(SourceResourceOverrides.key, serialize<HashMap<Any?, Any?>?>(overrides))),
        streamRefreshesRepository!!,
        null,
      )

    jobCreator.createSyncJob(
      SOURCE_CONNECTION,
      DESTINATION_CONNECTION,
      STANDARD_SYNC,
      SOURCE_IMAGE_NAME,
      SOURCE_IMAGE_IS_DEFAULT,
      SOURCE_PROTOCOL_VERSION,
      DESTINATION_IMAGE_NAME,
      DESTINATION_IMAGE_IS_DEFAULT,
      DESTINATION_PROTOCOL_VERSION,
      listOf(STANDARD_SYNC_OPERATION),
      null,
      StandardSourceDefinition()
        .withSourceDefinitionId(UUID.randomUUID())
        .withResourceRequirements(
          ScopedResourceRequirements().withJobSpecific(
            List.of<JobTypeResourceLimit?>(
              JobTypeResourceLimit().withJobType(JobTypeResourceLimit.JobType.SYNC).withResourceRequirements(originalReqs),
            ),
          ),
        ),
      StandardDestinationDefinition()
        .withDestinationDefinitionId(UUID.randomUUID())
        .withResourceRequirements(ScopedResourceRequirements().withDefault(destResourceRequirements)),
      SOURCE_DEFINITION_VERSION,
      DESTINATION_DEFINITION_VERSION,
      WORKSPACE_ID,
      true,
    )

    val configCaptor = argumentCaptor<JobConfig>()
    verify(jobPersistence, times(1)).enqueueJob(any<String>(), configCaptor.capture(), eq(true))

    val sourceConfigValues =
      configCaptor.firstValue
        .getSync()
        .getSyncResourceRequirements()
        .getSource()

    val expectedCpuReq = if (StringUtils.isNotBlank(cpuReqOverride)) cpuReqOverride else originalReqs.getCpuRequest()
    Assertions.assertEquals(expectedCpuReq, sourceConfigValues.getCpuRequest())

    val expectedCpuLimit = if (StringUtils.isNotBlank(cpuLimitOverride)) cpuLimitOverride else originalReqs.getCpuLimit()
    Assertions.assertEquals(expectedCpuLimit, sourceConfigValues.getCpuLimit())

    val expectedMemReq = if (StringUtils.isNotBlank(memReqOverride)) memReqOverride else originalReqs.getMemoryRequest()
    Assertions.assertEquals(expectedMemReq, sourceConfigValues.getMemoryRequest())

    val expectedMemLimit = if (StringUtils.isNotBlank(memLimitOverride)) memLimitOverride else originalReqs.getMemoryLimit()
    Assertions.assertEquals(expectedMemLimit, sourceConfigValues.getMemoryLimit())
  }

  @ParameterizedTest
  @MethodSource("weirdnessOverrideMatrix")
  @Throws(IOException::class)
  fun ignoresOverridesIfJsonStringWeird(weirdness: String?) {
    val originalReqs =
      ResourceRequirements()
        .withCpuLimit("0.8")
        .withCpuRequest("0.8")
        .withMemoryLimit("800Mi")
        .withMemoryRequest("800Mi")

    val jobCreator =
      DefaultJobCreator(
        jobPersistence!!,
        resourceRequirementsProvider!!,
        TestClient(Map.of<String, String?>(DestResourceOverrides.key, serialize<String?>(weirdness))),
        streamRefreshesRepository!!,
        null,
      )

    jobCreator.createSyncJob(
      SOURCE_CONNECTION,
      DESTINATION_CONNECTION,
      STANDARD_SYNC,
      SOURCE_IMAGE_NAME,
      SOURCE_IMAGE_IS_DEFAULT,
      SOURCE_PROTOCOL_VERSION,
      DESTINATION_IMAGE_NAME,
      DESTINATION_IMAGE_IS_DEFAULT,
      DESTINATION_PROTOCOL_VERSION,
      listOf(STANDARD_SYNC_OPERATION),
      null,
      StandardSourceDefinition()
        .withSourceDefinitionId(UUID.randomUUID())
        .withResourceRequirements(ScopedResourceRequirements().withDefault(sourceResourceRequirements)),
      StandardDestinationDefinition()
        .withDestinationDefinitionId(UUID.randomUUID())
        .withResourceRequirements(
          ScopedResourceRequirements().withJobSpecific(
            List.of<JobTypeResourceLimit?>(
              JobTypeResourceLimit().withJobType(JobTypeResourceLimit.JobType.SYNC).withResourceRequirements(originalReqs),
            ),
          ),
        ),
      SOURCE_DEFINITION_VERSION,
      DESTINATION_DEFINITION_VERSION,
      WORKSPACE_ID,
      true,
    )

    val configCaptor = argumentCaptor<JobConfig>()
    verify(jobPersistence, times(1))
      .enqueueJob(any<String>(), configCaptor.capture(), eq(true))
    val destConfigValues =
      configCaptor.firstValue
        .getSync()
        .getSyncResourceRequirements()
        .getDestination()

    Assertions.assertEquals(originalReqs.getCpuRequest(), destConfigValues.getCpuRequest())
    Assertions.assertEquals(originalReqs.getCpuLimit(), destConfigValues.getCpuLimit())
    Assertions.assertEquals(originalReqs.getMemoryRequest(), destConfigValues.getMemoryRequest())
    Assertions.assertEquals(originalReqs.getMemoryLimit(), destConfigValues.getMemoryLimit())
  }

  @Test
  @Throws(IOException::class)
  fun testCreateResetConnectionJob() {
    whenever(
      resourceRequirementsProvider!!.getResourceRequirements(
        ResourceRequirementsType.ORCHESTRATOR,
        null,
        DEFAULT_VARIANT,
      ),
    ).thenReturn(workerResourceRequirements)
    whenever(
      resourceRequirementsProvider!!.getResourceRequirements(
        ResourceRequirementsType.SOURCE,
        null,
        DEFAULT_VARIANT,
      ),
    ).thenReturn(sourceResourceRequirements)
    whenever(
      resourceRequirementsProvider!!.getResourceRequirements(
        ResourceRequirementsType.DESTINATION,
        null,
        DEFAULT_VARIANT,
      ),
    ).thenReturn(destResourceRequirements)

    val streamsToReset = List.of<StreamDescriptor?>(STREAM1_DESCRIPTOR, STREAM2_DESCRIPTOR)
    val expectedCatalog =
      ConfiguredAirbyteCatalog().withStreams(
        List.of<ConfiguredAirbyteStream>(
          ConfiguredAirbyteStream(
            createAirbyteStream(STREAM1_NAME, Field.of(FIELD_NAME, JsonSchemaType.STRING)),
            SyncMode.FULL_REFRESH,
            DestinationSyncMode.OVERWRITE,
          ),
          ConfiguredAirbyteStream(
            createAirbyteStream(STREAM2_NAME, NAMESPACE, Field.of(FIELD_NAME, JsonSchemaType.STRING)),
            SyncMode.FULL_REFRESH,
            DestinationSyncMode.OVERWRITE,
          ),
        ),
      )

    val expectedSyncResourceRequirements =
      SyncResourceRequirements()
        .withConfigKey(SyncResourceRequirementsKey().withVariant(DEFAULT_VARIANT))
        .withDestination(destResourceRequirements)
        .withOrchestrator(workerResourceRequirements)

    val jobResetConnectionConfig =
      JobResetConnectionConfig()
        .withNamespaceDefinition(STANDARD_SYNC.namespaceDefinition)
        .withNamespaceFormat(STANDARD_SYNC.namespaceFormat)
        .withPrefix(STANDARD_SYNC.prefix)
        .withDestinationDockerImage(DESTINATION_IMAGE_NAME)
        .withDestinationProtocolVersion(DESTINATION_PROTOCOL_VERSION)
        .withConfiguredAirbyteCatalog(expectedCatalog)
        .withOperationSequence(listOf(STANDARD_SYNC_OPERATION))
        .withResourceRequirements(workerResourceRequirements)
        .withSyncResourceRequirements(expectedSyncResourceRequirements)
        .withResetSourceConfiguration(ResetSourceConfiguration().withStreamsToReset(streamsToReset))
        .withIsSourceCustomConnector(false)
        .withIsDestinationCustomConnector(false)
        .withWorkspaceId(DESTINATION_CONNECTION.getWorkspaceId())
        .withDestinationDefinitionVersionId(DESTINATION_DEFINITION_VERSION.getVersionId())

    val jobConfig =
      JobConfig()
        .withConfigType(ConfigType.RESET_CONNECTION)
        .withResetConnection(jobResetConnectionConfig)

    val expectedScope = STANDARD_SYNC.connectionId.toString()
    whenever(jobPersistence.enqueueJob(expectedScope, jobConfig, false)).thenReturn(Optional.of(JOB_ID))

    val jobId =
      jobCreator.createResetConnectionJob(
        DESTINATION_CONNECTION,
        STANDARD_SYNC,
        StandardDestinationDefinition(),
        DESTINATION_DEFINITION_VERSION,
        DESTINATION_IMAGE_NAME,
        DESTINATION_PROTOCOL_VERSION,
        false,
        listOf(STANDARD_SYNC_OPERATION),
        streamsToReset,
        WORKSPACE_ID,
      )

    verify(jobPersistence).enqueueJob(expectedScope, jobConfig, false)
    Assertions.assertTrue(jobId.isPresent())
    Assertions.assertEquals(JOB_ID, jobId.get())
  }

  @Test
  @Throws(IOException::class)
  fun testCreateResetConnectionJobEnsureNoQueuing() {
    val streamsToReset = List.of<StreamDescriptor?>(STREAM1_DESCRIPTOR, STREAM2_DESCRIPTOR)
    val expectedCatalog =
      ConfiguredAirbyteCatalog().withStreams(
        List.of<ConfiguredAirbyteStream>(
          ConfiguredAirbyteStream(
            createAirbyteStream(STREAM1_NAME, Field.of(FIELD_NAME, JsonSchemaType.STRING)),
            SyncMode.FULL_REFRESH,
            DestinationSyncMode.OVERWRITE,
          ),
          ConfiguredAirbyteStream(
            createAirbyteStream(STREAM2_NAME, NAMESPACE, Field.of(FIELD_NAME, JsonSchemaType.STRING)),
            SyncMode.FULL_REFRESH,
            DestinationSyncMode.OVERWRITE,
          ),
        ),
      )

    val expectedSyncResourceRequirements =
      SyncResourceRequirements()
        .withConfigKey(SyncResourceRequirementsKey().withVariant(DEFAULT_VARIANT))
        .withDestination(workerResourceRequirements)
        .withOrchestrator(workerResourceRequirements)

    val jobResetConnectionConfig =
      JobResetConnectionConfig()
        .withNamespaceDefinition(STANDARD_SYNC.namespaceDefinition)
        .withNamespaceFormat(STANDARD_SYNC.namespaceFormat)
        .withPrefix(STANDARD_SYNC.prefix)
        .withDestinationDockerImage(DESTINATION_IMAGE_NAME)
        .withDestinationProtocolVersion(DESTINATION_PROTOCOL_VERSION)
        .withConfiguredAirbyteCatalog(expectedCatalog)
        .withOperationSequence(listOf(STANDARD_SYNC_OPERATION))
        .withResourceRequirements(workerResourceRequirements)
        .withSyncResourceRequirements(expectedSyncResourceRequirements)
        .withResetSourceConfiguration(ResetSourceConfiguration().withStreamsToReset(streamsToReset))
        .withIsSourceCustomConnector(false)
        .withIsDestinationCustomConnector(false)
        .withWorkspaceId(DESTINATION_CONNECTION.getWorkspaceId())
        .withDestinationDefinitionVersionId(DESTINATION_DEFINITION_VERSION.getVersionId())

    val jobConfig =
      JobConfig()
        .withConfigType(ConfigType.RESET_CONNECTION)
        .withResetConnection(jobResetConnectionConfig)

    val expectedScope = STANDARD_SYNC.connectionId.toString()
    whenever(jobPersistence.enqueueJob(expectedScope, jobConfig, true)).thenReturn(Optional.empty())

    val jobId =
      jobCreator.createResetConnectionJob(
        DESTINATION_CONNECTION,
        STANDARD_SYNC,
        StandardDestinationDefinition(),
        DESTINATION_DEFINITION_VERSION,
        DESTINATION_IMAGE_NAME,
        DESTINATION_PROTOCOL_VERSION,
        false,
        listOf(STANDARD_SYNC_OPERATION),
        streamsToReset,
        WORKSPACE_ID,
      )

    verify(jobPersistence).enqueueJob(expectedScope, jobConfig, false)
    Assertions.assertTrue(jobId.isEmpty())
  }

  @Test
  fun testGetResumableFullRefresh() {
    val standardSync =
      StandardSync()
        .withCatalog(
          ConfiguredAirbyteCatalog().withStreams(
            List.of<ConfiguredAirbyteStream>(
              ConfiguredAirbyteStream(
                AirbyteStream("no1", emptyObject(), List.of<SyncMode?>(SyncMode.INCREMENTAL)).withIsResumable(true),
                SyncMode.INCREMENTAL,
                DestinationSyncMode.APPEND,
              ),
              ConfiguredAirbyteStream(
                AirbyteStream("no2", emptyObject(), List.of<SyncMode?>(SyncMode.FULL_REFRESH)).withIsResumable(false),
                SyncMode.FULL_REFRESH,
                DestinationSyncMode.APPEND,
              ),
              ConfiguredAirbyteStream(
                AirbyteStream("yes", emptyObject(), List.of<SyncMode?>(SyncMode.FULL_REFRESH)).withIsResumable(true),
                SyncMode.FULL_REFRESH,
                DestinationSyncMode.APPEND,
              ),
            ),
          ),
        )

    var streamDescriptors = jobCreator!!.getResumableFullRefresh(standardSync, true)
    Assertions.assertEquals(1, streamDescriptors.size)
    Assertions.assertEquals(
      "yes",
      streamDescriptors
        .stream()
        .findFirst()
        .get()
        .getName(),
    )

    streamDescriptors = jobCreator!!.getResumableFullRefresh(standardSync, false)
    Assertions.assertTrue(streamDescriptors.isEmpty())
  }

  companion object {
    private const val DEFAULT_VARIANT = "default"
    private const val STREAM1_NAME = "stream1"
    private const val STREAM2_NAME = "stream2"
    private const val STREAM3_NAME = "stream3"
    private const val NAMESPACE = "namespace"
    private const val FIELD_NAME = "id"
    private val STREAM1_DESCRIPTOR: StreamDescriptor = StreamDescriptor().withName(STREAM1_NAME)
    private val STREAM2_DESCRIPTOR: StreamDescriptor = StreamDescriptor().withName(STREAM2_NAME).withNamespace(NAMESPACE)

    private const val SOURCE_IMAGE_NAME = "daxtarity/sourceimagename"
    private const val SOURCE_IMAGE_IS_DEFAULT = true
    private val SOURCE_PROTOCOL_VERSION = Version("0.2.2")
    private const val DESTINATION_IMAGE_NAME = "daxtarity/destinationimagename"
    private const val DESTINATION_IMAGE_IS_DEFAULT = true
    private val DESTINATION_PROTOCOL_VERSION = Version("0.2.3")
    private val SOURCE_CONNECTION: SourceConnection
    private val DESTINATION_CONNECTION: DestinationConnection
    private val STANDARD_SYNC: StandardSync
    private val STANDARD_SYNC_OPERATION: StandardSyncOperation

    private val STANDARD_SOURCE_DEFINITION: StandardSourceDefinition
    private val STANDARD_SOURCE_DEFINITION_WITH_SOURCE_TYPE: StandardSourceDefinition
    private val STANDARD_DESTINATION_DEFINITION: StandardDestinationDefinition
    private val SOURCE_DEFINITION_VERSION: ActorDefinitionVersion
    private val DESTINATION_DEFINITION_VERSION: ActorDefinitionVersion
    private val CONFIGURED_AIRBYTE_CATALOG: ConfiguredAirbyteCatalog
    private const val JOB_ID = 12L
    private val WORKSPACE_ID: UUID = UUID.randomUUID()

    private val PERSISTED_WEBHOOK_CONFIGS: JsonNode

    private val WEBHOOK_CONFIG_ID: UUID
    private val WEBHOOK_NAME: String

    init {
      val workspaceId = UUID.randomUUID()
      val sourceId = UUID.randomUUID()
      val sourceDefinitionId = UUID.randomUUID()
      WEBHOOK_CONFIG_ID = UUID.randomUUID()
      WEBHOOK_NAME = "test-name"

      val implementationJson =
        jsonNode<ImmutableMap<Any?, Any?>?>(
          ImmutableMap
            .builder<Any?, Any?>()
            .put("apiKey", "123-abc")
            .put("hostname", "airbyte.io")
            .build(),
        )

      SOURCE_CONNECTION =
        SourceConnection()
          .withWorkspaceId(workspaceId)
          .withSourceDefinitionId(sourceDefinitionId)
          .withSourceId(sourceId)
          .withConfiguration(implementationJson)
          .withTombstone(false)

      val destinationId = UUID.randomUUID()
      val destinationDefinitionId = UUID.randomUUID()

      DESTINATION_CONNECTION =
        DestinationConnection()
          .withWorkspaceId(workspaceId)
          .withDestinationDefinitionId(destinationDefinitionId)
          .withDestinationId(destinationId)
          .withConfiguration(implementationJson)
          .withTombstone(false)

      val connectionId = UUID.randomUUID()
      val operationId = UUID.randomUUID()

      val stream1 =
        ConfiguredAirbyteStream(
          createAirbyteStream(STREAM1_NAME, Field.of(FIELD_NAME, JsonSchemaType.STRING)),
          SyncMode.FULL_REFRESH,
          DestinationSyncMode.APPEND,
        )
      val stream2 =
        ConfiguredAirbyteStream(
          createAirbyteStream(STREAM2_NAME, NAMESPACE, Field.of(FIELD_NAME, JsonSchemaType.STRING)),
          SyncMode.INCREMENTAL,
          DestinationSyncMode.APPEND,
        )
      val stream3 =
        ConfiguredAirbyteStream(
          createAirbyteStream(STREAM3_NAME, NAMESPACE, Field.of(FIELD_NAME, JsonSchemaType.STRING)).withIsResumable(true),
          SyncMode.FULL_REFRESH,
          DestinationSyncMode.OVERWRITE,
        )
      CONFIGURED_AIRBYTE_CATALOG = ConfiguredAirbyteCatalog().withStreams(List.of<ConfiguredAirbyteStream>(stream1, stream2, stream3))

      STANDARD_SYNC =
        StandardSync()
          .withConnectionId(connectionId)
          .withName("presto to hudi")
          .withNamespaceDefinition(JobSyncConfig.NamespaceDefinitionType.SOURCE)
          .withNamespaceFormat(null)
          .withPrefix("presto_to_hudi")
          .withStatus(StandardSync.Status.ACTIVE)
          .withCatalog(CONFIGURED_AIRBYTE_CATALOG)
          .withSourceId(sourceId)
          .withDestinationId(destinationId)
          .withOperationIds(List.of<UUID?>(operationId))

      STANDARD_SYNC_OPERATION =
        StandardSyncOperation()
          .withOperationId(operationId)
          .withName("normalize")
          .withTombstone(false)

      PERSISTED_WEBHOOK_CONFIGS =
        deserialize(
          String.format(
            "{\"webhookConfigs\": [{\"id\": \"%s\", \"name\": \"%s\", \"authToken\": {\"_secret\": \"a-secret_v1\"}}]}",
            WEBHOOK_CONFIG_ID,
            WEBHOOK_NAME,
          ),
        )

      STANDARD_SOURCE_DEFINITION = StandardSourceDefinition().withSourceDefinitionId(UUID.randomUUID()).withCustom(false)
      STANDARD_SOURCE_DEFINITION_WITH_SOURCE_TYPE =
        StandardSourceDefinition()
          .withSourceDefinitionId(UUID.randomUUID())
          .withSourceType(StandardSourceDefinition.SourceType.DATABASE)
          .withCustom(false)
      STANDARD_DESTINATION_DEFINITION = StandardDestinationDefinition().withDestinationDefinitionId(UUID.randomUUID()).withCustom(false)

      SOURCE_DEFINITION_VERSION = ActorDefinitionVersion().withVersionId(UUID.randomUUID())
      DESTINATION_DEFINITION_VERSION = ActorDefinitionVersion().withVersionId(UUID.randomUUID())
    }

    private fun getRefreshConfig(
      expectedSyncResourceRequirements: SyncResourceRequirements?,
      streamToRefresh: List<RefreshStream>,
    ): RefreshConfig =
      RefreshConfig()
        .withNamespaceDefinition(STANDARD_SYNC.namespaceDefinition)
        .withNamespaceFormat(STANDARD_SYNC.namespaceFormat)
        .withPrefix(STANDARD_SYNC.prefix)
        .withSourceDockerImage(SOURCE_IMAGE_NAME)
        .withSourceProtocolVersion(SOURCE_PROTOCOL_VERSION)
        .withDestinationDockerImage(DESTINATION_IMAGE_NAME)
        .withDestinationProtocolVersion(DESTINATION_PROTOCOL_VERSION)
        .withConfiguredAirbyteCatalog(STANDARD_SYNC.catalog)
        .withOperationSequence(listOf(STANDARD_SYNC_OPERATION))
        .withSyncResourceRequirements(expectedSyncResourceRequirements)
        .withWebhookOperationConfigs(PERSISTED_WEBHOOK_CONFIGS)
        .withIsSourceCustomConnector(false)
        .withIsDestinationCustomConnector(false)
        .withWorkspaceId(WORKSPACE_ID)
        .withSourceDefinitionVersionId(SOURCE_DEFINITION_VERSION.versionId)
        .withDestinationDefinitionVersionId(DESTINATION_DEFINITION_VERSION.versionId)
        .withStreamsToRefresh(streamToRefresh as kotlin.collections.List<RefreshStream>)

    @JvmStatic
    private fun resourceOverrideMatrix(): Stream<Arguments?> =
      Stream.of<Arguments?>(
        Arguments.of("0.7", "0.4", "1000Mi", "2000Mi"),
        Arguments.of("0.3", null, "1000Mi", null),
        Arguments.of(null, null, null, null),
        Arguments.of(null, "0.4", null, null),
        Arguments.of("3", "3", "3000Mi", "3000Mi"),
        Arguments.of("4", "5", "6000Mi", "7000Mi"),
      )

    @JvmStatic
    private fun weirdnessOverrideMatrix(): Stream<Arguments?> =
      Stream.of<Arguments?>(
        Arguments.of("0.7"),
        Arguments.of("0.5, 1, 1000Mi, 2000Mi"),
        Arguments.of("cat burglar"),
        Arguments.of("{ \"cpu_limit\": \"2\", \"cpu_request\": \"1\"  "),
        Arguments.of("null"),
        Arguments.of("undefined"),
        Arguments.of(""),
        Arguments.of("{}"),
      )
  }
}
