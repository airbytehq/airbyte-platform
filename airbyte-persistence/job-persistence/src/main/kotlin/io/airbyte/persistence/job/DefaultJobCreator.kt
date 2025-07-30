/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.persistence.job

import com.fasterxml.jackson.databind.JsonNode
import com.google.common.annotations.VisibleForTesting
import io.airbyte.commons.version.Version
import io.airbyte.config.ActorDefinitionVersion
import io.airbyte.config.ConfiguredAirbyteCatalog
import io.airbyte.config.ConfiguredAirbyteStream
import io.airbyte.config.DestinationConnection
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
import io.airbyte.config.SourceConnection
import io.airbyte.config.StandardDestinationDefinition
import io.airbyte.config.StandardSourceDefinition
import io.airbyte.config.StandardSync
import io.airbyte.config.StandardSyncOperation
import io.airbyte.config.StreamDescriptor
import io.airbyte.config.SyncMode
import io.airbyte.config.SyncResourceRequirements
import io.airbyte.config.SyncResourceRequirementsKey
import io.airbyte.config.helpers.CatalogTransforms.updateCatalogForReset
import io.airbyte.config.helpers.ResourceRequirementsUtils.getResourceRequirementsForJobType
import io.airbyte.config.helpers.ResourceRequirementsUtils.mergeResourceRequirements
import io.airbyte.config.helpers.ResourceRequirementsUtils.parse
import io.airbyte.config.persistence.StreamRefreshesRepository
import io.airbyte.config.persistence.domain.StreamRefresh
import io.airbyte.config.provider.ResourceRequirementsProvider
import io.airbyte.db.instance.configs.jooq.generated.enums.RefreshType
import io.airbyte.featureflag.Connection
import io.airbyte.featureflag.Context
import io.airbyte.featureflag.DestResourceOverrides
import io.airbyte.featureflag.Destination
import io.airbyte.featureflag.DestinationDefinition
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.featureflag.Multi
import io.airbyte.featureflag.OrchestratorResourceOverrides
import io.airbyte.featureflag.Source
import io.airbyte.featureflag.SourceDefinition
import io.airbyte.featureflag.SourceResourceOverrides
import io.airbyte.featureflag.UseResourceRequirementsVariant
import io.airbyte.featureflag.Workspace
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.annotation.Nullable
import java.io.IOException
import java.util.Optional
import java.util.UUID
import java.util.function.Consumer
import java.util.function.Function
import java.util.stream.Collectors

/**
 * Default implementation of enqueueing a job. Hides the details of building the Job object and
 * storing it in the jobs db.
 */
class DefaultJobCreator(
  private val jobPersistence: JobPersistence,
  private val resourceRequirementsProvider: ResourceRequirementsProvider,
  private val featureFlagClient: FeatureFlagClient,
  private val streamRefreshesRepository: StreamRefreshesRepository,
  @param:Nullable private val variantOverride: String?,
) : JobCreator {
  @Throws(IOException::class)
  override fun createSyncJob(
    source: SourceConnection,
    destination: DestinationConnection,
    standardSync: StandardSync,
    sourceDockerImageName: String,
    sourceDockerImageIsDefault: Boolean,
    sourceProtocolVersion: Version,
    destinationDockerImageName: String,
    destinationDockerImageIsDefault: Boolean,
    destinationProtocolVersion: Version,
    standardSyncOperations: List<StandardSyncOperation>,
    @Nullable webhookOperationConfigs: JsonNode?,
    sourceDefinition: StandardSourceDefinition,
    destinationDefinition: StandardDestinationDefinition,
    sourceDefinitionVersion: ActorDefinitionVersion,
    destinationDefinitionVersion: ActorDefinitionVersion,
    workspaceId: UUID,
    isScheduled: Boolean,
  ): Optional<Long> {
    val syncResourceRequirements =
      getSyncResourceRequirements(workspaceId, Optional.of(source), destination, standardSync, sourceDefinition, destinationDefinition, false)

    val jobSyncConfig =
      JobSyncConfig()
        .withNamespaceDefinition(standardSync.namespaceDefinition)
        .withNamespaceFormat(standardSync.namespaceFormat)
        .withPrefix(standardSync.prefix)
        .withSourceDockerImage(sourceDockerImageName)
        .withSourceDockerImageIsDefault(sourceDockerImageIsDefault)
        .withSourceProtocolVersion(sourceProtocolVersion)
        .withDestinationDockerImage(destinationDockerImageName)
        .withDestinationDockerImageIsDefault(destinationDockerImageIsDefault)
        .withDestinationProtocolVersion(destinationProtocolVersion)
        .withOperationSequence(standardSyncOperations)
        .withWebhookOperationConfigs(webhookOperationConfigs)
        .withConfiguredAirbyteCatalog(standardSync.catalog)
        .withSyncResourceRequirements(syncResourceRequirements)
        .withIsSourceCustomConnector(sourceDefinition.custom)
        .withIsDestinationCustomConnector(destinationDefinition.custom)
        .withWorkspaceId(workspaceId)
        .withSourceDefinitionVersionId(sourceDefinitionVersion.versionId)
        .withDestinationDefinitionVersionId(destinationDefinitionVersion.versionId)

    val jobConfig =
      JobConfig()
        .withConfigType(ConfigType.SYNC)
        .withSync(jobSyncConfig)

    return jobPersistence.enqueueJob(standardSync.connectionId.toString(), jobConfig, isScheduled)
  }

  @Throws(IOException::class)
  override fun createRefreshConnection(
    source: SourceConnection,
    destination: DestinationConnection,
    standardSync: StandardSync,
    sourceDockerImageName: String,
    sourceProtocolVersion: Version,
    destinationDockerImageName: String,
    destinationProtocolVersion: Version,
    standardSyncOperations: List<StandardSyncOperation>,
    @Nullable webhookOperationConfigs: JsonNode?,
    sourceDefinition: StandardSourceDefinition,
    destinationDefinition: StandardDestinationDefinition,
    sourceDefinitionVersion: ActorDefinitionVersion,
    destinationDefinitionVersion: ActorDefinitionVersion,
    workspaceId: UUID,
    streamsToRefresh: List<StreamRefresh>,
  ): Optional<Long> {
    val canRunRefreshes = destinationDefinitionVersion.supportsRefreshes

    check(canRunRefreshes) { "Trying to create a refresh job for a destination which doesn't support refreshes" }

    val syncResourceRequirements =
      getSyncResourceRequirements(workspaceId, Optional.of(source), destination, standardSync, sourceDefinition, destinationDefinition, false)

    val refreshConfig =
      RefreshConfig()
        .withNamespaceDefinition(standardSync.namespaceDefinition)
        .withNamespaceFormat(standardSync.namespaceFormat)
        .withPrefix(standardSync.prefix)
        .withSourceDockerImage(sourceDockerImageName)
        .withSourceProtocolVersion(sourceProtocolVersion)
        .withDestinationDockerImage(destinationDockerImageName)
        .withDestinationProtocolVersion(destinationProtocolVersion)
        .withOperationSequence(standardSyncOperations)
        .withWebhookOperationConfigs(webhookOperationConfigs)
        .withConfiguredAirbyteCatalog(standardSync.catalog)
        .withSyncResourceRequirements(syncResourceRequirements)
        .withIsSourceCustomConnector(sourceDefinition.custom)
        .withIsDestinationCustomConnector(destinationDefinition.custom)
        .withWorkspaceId(workspaceId)
        .withSourceDefinitionVersionId(sourceDefinitionVersion.versionId)
        .withDestinationDefinitionVersionId(destinationDefinitionVersion.versionId)
        .withStreamsToRefresh(
          streamsToRefresh
            .stream()
            .map { streamRefresh: StreamRefresh ->
              RefreshStream()
                .withRefreshType(convertToApi(streamRefresh.refreshType))
                .withStreamDescriptor(
                  StreamDescriptor()
                    .withName(streamRefresh.streamName)
                    .withNamespace(streamRefresh.streamNamespace),
                )
            }.toList(),
        )

    streamsToRefresh.forEach(
      Consumer { s: StreamRefresh ->
        streamRefreshesRepository.deleteByConnectionIdAndStreamNameAndStreamNamespace(
          standardSync.connectionId,
          s.streamName,
          s.streamNamespace,
        )
      },
    )

    val jobConfig =
      JobConfig()
        .withConfigType(ConfigType.REFRESH)
        .withRefresh(refreshConfig)

    return jobPersistence.enqueueJob(standardSync.connectionId.toString(), jobConfig, false)
  }

  @VisibleForTesting
  fun getResumableFullRefresh(
    standardSync: StandardSync,
    supportResumableFullRefresh: Boolean,
  ): Set<StreamDescriptor> {
    if (!supportResumableFullRefresh) {
      return setOf()
    }

    return standardSync.catalog.streams
      .stream()
      .filter { stream: ConfiguredAirbyteStream ->
        stream.syncMode == SyncMode.FULL_REFRESH &&
          stream.stream.isResumable != null &&
          stream.stream.isResumable!!
      }.map(ConfiguredAirbyteStream::streamDescriptor)
      .collect(Collectors.toSet())
  }

  @Throws(IOException::class)
  override fun createResetConnectionJob(
    destination: DestinationConnection,
    standardSync: StandardSync,
    destinationDefinition: StandardDestinationDefinition,
    destinationDefinitionVersion: ActorDefinitionVersion,
    destinationDockerImage: String,
    destinationProtocolVersion: Version,
    isDestinationCustomConnector: Boolean,
    standardSyncOperations: List<StandardSyncOperation>,
    streamsToReset: List<StreamDescriptor>,
    workspaceId: UUID,
  ): Optional<Long> {
    val immutableConfiguredAirbyteCatalog = standardSync.catalog
    val configuredAirbyteCatalog =
      ConfiguredAirbyteCatalog()
        .withStreams(ArrayList(immutableConfiguredAirbyteCatalog.streams))
    updateCatalogForReset(streamsToReset, configuredAirbyteCatalog)

    val resetResourceRequirements =
      getSyncResourceRequirements(workspaceId, null, destination, standardSync, RESET_SOURCE_DEFINITION, destinationDefinition, true)

    val resetConnectionConfig =
      JobResetConnectionConfig()
        .withNamespaceDefinition(standardSync.namespaceDefinition)
        .withNamespaceFormat(standardSync.namespaceFormat)
        .withPrefix(standardSync.prefix)
        .withDestinationDockerImage(destinationDockerImage)
        .withDestinationProtocolVersion(destinationProtocolVersion)
        .withOperationSequence(standardSyncOperations)
        .withConfiguredAirbyteCatalog(configuredAirbyteCatalog)
        .withResourceRequirements(resetResourceRequirements.orchestrator)
        .withSyncResourceRequirements(resetResourceRequirements)
        .withResetSourceConfiguration(ResetSourceConfiguration().withStreamsToReset(streamsToReset))
        .withIsSourceCustomConnector(false)
        .withIsDestinationCustomConnector(isDestinationCustomConnector)
        .withWorkspaceId(destination.workspaceId)
        .withDestinationDefinitionVersionId(destinationDefinitionVersion.versionId)

    val jobConfig =
      JobConfig()
        .withConfigType(ConfigType.RESET_CONNECTION)
        .withResetConnection(resetConnectionConfig)
    return jobPersistence.enqueueJob(standardSync.connectionId.toString(), jobConfig, false)
  }

  private fun getSyncResourceRequirements(
    workspaceId: UUID,
    source: Optional<SourceConnection>?,
    destination: DestinationConnection,
    standardSync: StandardSync,
    sourceDefinition: StandardSourceDefinition?,
    destinationDefinition: StandardDestinationDefinition,
    isReset: Boolean,
  ): SyncResourceRequirements {
    val ffContext = buildFeatureFlagContext(workspaceId, standardSync, sourceDefinition, destinationDefinition)
    val variant =
      if (variantOverride == null || variantOverride.isBlank()) {
        featureFlagClient.stringVariation(UseResourceRequirementsVariant, ffContext)
      } else {
        variantOverride
      }

    // Note on use of sourceType, throughput is driven by the source, if the source is slow, the rest is
    // going to be slow. With this in mind, we align the resources given to the orchestrator and the
    // destination based on the source to avoid oversizing orchestrator and destination when the source
    // is slow.
    val sourceType = getSourceType(sourceDefinition)
    val mergedOrchestratorResourceReq = getOrchestratorResourceRequirements(standardSync, sourceType, variant, ffContext)
    val mergedDstResourceReq =
      getDestinationResourceRequirements(standardSync, destination, destinationDefinition, sourceType, variant, ffContext)

    val syncResourceRequirements =
      SyncResourceRequirements()
        .withConfigKey(SyncResourceRequirementsKey().withVariant(variant).withSubType(sourceType.orElse(null)))
        .withDestination(mergedDstResourceReq)
        .withOrchestrator(mergedOrchestratorResourceReq)

    if (!isReset) {
      if (source == null || source.isEmpty) {
        log.error(
          "`source` is expected for all jobs except reset, but was not present. " +
            "Resource requirements set for the source are being skipped.",
        )
        return syncResourceRequirements
      }
      val mergedSrcResourceReq =
        getSourceResourceRequirements(standardSync, source.get(), sourceDefinition, variant, ffContext)
      syncResourceRequirements
        .withSource(mergedSrcResourceReq)
    }

    return syncResourceRequirements
  }

  private fun buildFeatureFlagContext(
    workspaceId: UUID,
    standardSync: StandardSync,
    sourceDefinition: StandardSourceDefinition?,
    destinationDefinition: StandardDestinationDefinition,
  ): Context {
    val contextList: MutableList<Context> = ArrayList()
    addIfNotNull(
      contextList,
      workspaceId,
    ) { key: UUID -> Workspace(key) }
    addIfNotNull(
      contextList,
      standardSync.connectionId,
    ) { key: UUID -> Connection(key) }
    addIfNotNull(
      contextList,
      standardSync.sourceId,
    ) { key: UUID -> Source(key) }
    // Resets use an empty source. Account for lack of source definition.
    addIfNotNull(
      contextList,
      sourceDefinition?.sourceDefinitionId,
    ) { key: UUID -> SourceDefinition(key) }
    addIfNotNull(
      contextList,
      standardSync.destinationId,
    ) { key: UUID -> Destination(key) }
    addIfNotNull(
      contextList,
      destinationDefinition.destinationDefinitionId,
    ) { key: UUID -> DestinationDefinition(key) }
    return Multi(contextList)
  }

  private fun getOrchestratorResourceRequirements(
    standardSync: StandardSync,
    sourceType: Optional<String>,
    variant: String,
    ffContext: Context,
  ): ResourceRequirements {
    val defaultOrchestratorRssReqs =
      resourceRequirementsProvider.getResourceRequirements(ResourceRequirementsType.ORCHESTRATOR, sourceType.orElse(null), variant)

    val mergedRrsReqs =
      mergeResourceRequirements(
        standardSync.resourceRequirements,
        defaultOrchestratorRssReqs,
      )

    val overrides = getOrchestratorResourceOverrides(ffContext)

    return mergeResourceRequirements(overrides, mergedRrsReqs)
  }

  private fun getSourceResourceRequirements(
    standardSync: StandardSync,
    source: SourceConnection,
    sourceDefinition: StandardSourceDefinition?,
    variant: String,
    ffContext: Context,
  ): ResourceRequirements {
    val defaultSrcRssReqs =
      resourceRequirementsProvider.getResourceRequirements(
        ResourceRequirementsType.SOURCE,
        getSourceType(sourceDefinition).orElse(null),
        variant,
      )

    val mergedRssReqs =
      getResourceRequirementsForJobType(
        standardSync.resourceRequirements,
        source.resourceRequirements,
        sourceDefinition?.resourceRequirements,
        defaultSrcRssReqs,
        JobTypeResourceLimit.JobType.SYNC,
      )

    val overrides = getSourceResourceOverrides(ffContext)

    return mergeResourceRequirements(overrides, mergedRssReqs)
  }

  private fun getDestinationResourceRequirements(
    standardSync: StandardSync,
    destination: DestinationConnection,
    destinationDefinition: StandardDestinationDefinition,
    sourceType: Optional<String>,
    variant: String,
    ffContext: Context,
  ): ResourceRequirements {
    val defaultDstRssReqs =
      resourceRequirementsProvider.getResourceRequirements(ResourceRequirementsType.DESTINATION, sourceType.orElse(null), variant)

    val mergedRssReqs =
      getResourceRequirementsForJobType(
        standardSync.resourceRequirements,
        destination.resourceRequirements,
        destinationDefinition.resourceRequirements,
        defaultDstRssReqs,
        JobTypeResourceLimit.JobType.SYNC,
      )

    val overrides = getDestinationResourceOverrides(ffContext)

    return mergeResourceRequirements(overrides, mergedRssReqs)
  }

  private fun getDestinationResourceOverrides(ffCtx: Context): ResourceRequirements? {
    val destOverrides = featureFlagClient.stringVariation(DestResourceOverrides, ffCtx)
    try {
      return parse(destOverrides)
    } catch (e: Exception) {
      log.warn("Could not parse DESTINATION resource overrides '{}' from feature flag string: {}", destOverrides, e.message)
      return null
    }
  }

  private fun getOrchestratorResourceOverrides(ffCtx: Context): ResourceRequirements? {
    val orchestratorOverrides = featureFlagClient.stringVariation(OrchestratorResourceOverrides, ffCtx)
    try {
      return parse(orchestratorOverrides)
    } catch (e: Exception) {
      log.warn("Could not parse ORCHESTRATOR resource overrides '{}' from feature flag string: {}", orchestratorOverrides, e.message)
      return null
    }
  }

  private fun getSourceResourceOverrides(ffCtx: Context): ResourceRequirements? {
    val sourceOverrides = featureFlagClient.stringVariation(SourceResourceOverrides, ffCtx)
    try {
      return parse(sourceOverrides)
    } catch (e: Exception) {
      log.warn("Could not parse SOURCE resource overrides '{}' from feature flag string: {}", sourceOverrides, e.message)
      return null
    }
  }

  private fun getSourceType(sourceDefinition: StandardSourceDefinition?): Optional<String> {
    if (sourceDefinition == null) {
      return Optional.empty()
    }
    return Optional.ofNullable(sourceDefinition.sourceType).map { obj: StandardSourceDefinition.SourceType -> obj.toString() }
  }

  companion object {
    private val log = KotlinLogging.logger {}

    // Resets use an empty source which doesn't have a source definition.
    private val RESET_SOURCE_DEFINITION: StandardSourceDefinition? = null

    private fun addIfNotNull(
      contextList: MutableList<Context>,
      uuid: UUID?,
      supplier: Function<UUID, Context>,
    ) {
      if (uuid != null) {
        contextList.add(supplier.apply(uuid))
      }
    }

    private fun convertToApi(type: RefreshType): RefreshStream.RefreshType =
      when (type) {
        RefreshType.MERGE -> {
          RefreshStream.RefreshType.MERGE
        }

        RefreshType.TRUNCATE -> {
          RefreshStream.RefreshType.TRUNCATE
        }

        else -> throw IllegalStateException("Unsupported enum value: " + type.literal)
      }
  }
}
