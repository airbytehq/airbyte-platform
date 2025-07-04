/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.persistence.job;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.annotations.VisibleForTesting;
import io.airbyte.commons.version.Version;
import io.airbyte.config.ActorDefinitionVersion;
import io.airbyte.config.ConfiguredAirbyteCatalog;
import io.airbyte.config.ConfiguredAirbyteStream;
import io.airbyte.config.DestinationConnection;
import io.airbyte.config.JobConfig;
import io.airbyte.config.JobConfig.ConfigType;
import io.airbyte.config.JobResetConnectionConfig;
import io.airbyte.config.JobSyncConfig;
import io.airbyte.config.JobTypeResourceLimit.JobType;
import io.airbyte.config.RefreshConfig;
import io.airbyte.config.RefreshStream;
import io.airbyte.config.RefreshStream.RefreshType;
import io.airbyte.config.ResetSourceConfiguration;
import io.airbyte.config.ResourceRequirements;
import io.airbyte.config.ResourceRequirementsType;
import io.airbyte.config.SourceConnection;
import io.airbyte.config.StandardDestinationDefinition;
import io.airbyte.config.StandardSourceDefinition;
import io.airbyte.config.StandardSourceDefinition.SourceType;
import io.airbyte.config.StandardSync;
import io.airbyte.config.StandardSyncOperation;
import io.airbyte.config.StreamDescriptor;
import io.airbyte.config.SyncMode;
import io.airbyte.config.SyncResourceRequirements;
import io.airbyte.config.SyncResourceRequirementsKey;
import io.airbyte.config.helpers.CatalogTransforms;
import io.airbyte.config.helpers.ResourceRequirementsUtils;
import io.airbyte.config.persistence.StreamRefreshesRepository;
import io.airbyte.config.persistence.domain.StreamRefresh;
import io.airbyte.config.provider.ResourceRequirementsProvider;
import io.airbyte.featureflag.Connection;
import io.airbyte.featureflag.Context;
import io.airbyte.featureflag.DestResourceOverrides;
import io.airbyte.featureflag.Destination;
import io.airbyte.featureflag.DestinationDefinition;
import io.airbyte.featureflag.FeatureFlagClient;
import io.airbyte.featureflag.Multi;
import io.airbyte.featureflag.OrchestratorResourceOverrides;
import io.airbyte.featureflag.Source;
import io.airbyte.featureflag.SourceDefinition;
import io.airbyte.featureflag.SourceResourceOverrides;
import io.airbyte.featureflag.UseResourceRequirementsVariant;
import io.airbyte.featureflag.Workspace;
import jakarta.annotation.Nullable;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default implementation of enqueueing a job. Hides the details of building the Job object and
 * storing it in the jobs db.
 */
public class DefaultJobCreator implements JobCreator {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  // Resets use an empty source which doesn't have a source definition.
  private static final StandardSourceDefinition RESET_SOURCE_DEFINITION = null;
  private final JobPersistence jobPersistence;
  private final ResourceRequirementsProvider resourceRequirementsProvider;
  private final FeatureFlagClient featureFlagClient;
  private final StreamRefreshesRepository streamRefreshesRepository;
  private final String variantOverride;

  public DefaultJobCreator(final JobPersistence jobPersistence,
                           final ResourceRequirementsProvider resourceRequirementsProvider,
                           final FeatureFlagClient featureFlagClient,
                           final StreamRefreshesRepository streamRefreshesRepository,
                           @Nullable final String variantOverride) {
    this.jobPersistence = jobPersistence;
    this.resourceRequirementsProvider = resourceRequirementsProvider;
    this.featureFlagClient = featureFlagClient;
    this.streamRefreshesRepository = streamRefreshesRepository;
    this.variantOverride = variantOverride;
  }

  @Override
  public Optional<Long> createSyncJob(final SourceConnection source,
                                      final DestinationConnection destination,
                                      final StandardSync standardSync,
                                      final String sourceDockerImageName,
                                      final Boolean sourceDockerImageIsDefault,
                                      final Version sourceProtocolVersion,
                                      final String destinationDockerImageName,
                                      final Boolean destinationDockerImageIsDefault,
                                      final Version destinationProtocolVersion,
                                      final List<StandardSyncOperation> standardSyncOperations,
                                      @Nullable final JsonNode webhookOperationConfigs,
                                      final StandardSourceDefinition sourceDefinition,
                                      final StandardDestinationDefinition destinationDefinition,
                                      final ActorDefinitionVersion sourceDefinitionVersion,
                                      final ActorDefinitionVersion destinationDefinitionVersion,
                                      final UUID workspaceId,
                                      final boolean isScheduled)
      throws IOException {
    final SyncResourceRequirements syncResourceRequirements =
        getSyncResourceRequirements(workspaceId, Optional.of(source), destination, standardSync, sourceDefinition, destinationDefinition, false);

    final JobSyncConfig jobSyncConfig = new JobSyncConfig()
        .withNamespaceDefinition(standardSync.getNamespaceDefinition())
        .withNamespaceFormat(standardSync.getNamespaceFormat())
        .withPrefix(standardSync.getPrefix())
        .withSourceDockerImage(sourceDockerImageName)
        .withSourceDockerImageIsDefault(sourceDockerImageIsDefault)
        .withSourceProtocolVersion(sourceProtocolVersion)
        .withDestinationDockerImage(destinationDockerImageName)
        .withDestinationDockerImageIsDefault(destinationDockerImageIsDefault)
        .withDestinationProtocolVersion(destinationProtocolVersion)
        .withOperationSequence(standardSyncOperations)
        .withWebhookOperationConfigs(webhookOperationConfigs)
        .withConfiguredAirbyteCatalog(standardSync.getCatalog())
        .withSyncResourceRequirements(syncResourceRequirements)
        .withIsSourceCustomConnector(sourceDefinition.getCustom())
        .withIsDestinationCustomConnector(destinationDefinition.getCustom())
        .withWorkspaceId(workspaceId)
        .withSourceDefinitionVersionId(sourceDefinitionVersion.getVersionId())
        .withDestinationDefinitionVersionId(destinationDefinitionVersion.getVersionId());

    final JobConfig jobConfig = new JobConfig()
        .withConfigType(ConfigType.SYNC)
        .withSync(jobSyncConfig);

    return jobPersistence.enqueueJob(standardSync.getConnectionId().toString(), jobConfig, isScheduled);
  }

  @Override
  public Optional<Long> createRefreshConnection(final SourceConnection source,
                                                final DestinationConnection destination,
                                                final StandardSync standardSync,
                                                final String sourceDockerImageName,
                                                final Version sourceProtocolVersion,
                                                final String destinationDockerImageName,
                                                final Version destinationProtocolVersion,
                                                final List<StandardSyncOperation> standardSyncOperations,
                                                @Nullable final JsonNode webhookOperationConfigs,
                                                final StandardSourceDefinition sourceDefinition,
                                                final StandardDestinationDefinition destinationDefinition,
                                                final ActorDefinitionVersion sourceDefinitionVersion,
                                                final ActorDefinitionVersion destinationDefinitionVersion,
                                                final UUID workspaceId,
                                                final List<StreamRefresh> streamsToRefresh)
      throws IOException {
    final boolean canRunRefreshes = destinationDefinitionVersion.getSupportsRefreshes();

    if (!canRunRefreshes) {
      throw new IllegalStateException("Trying to create a refresh job for a destination which doesn't support refreshes");
    }

    final SyncResourceRequirements syncResourceRequirements =
        getSyncResourceRequirements(workspaceId, Optional.of(source), destination, standardSync, sourceDefinition, destinationDefinition, false);

    final RefreshConfig refreshConfig = new RefreshConfig()
        .withNamespaceDefinition(standardSync.getNamespaceDefinition())
        .withNamespaceFormat(standardSync.getNamespaceFormat())
        .withPrefix(standardSync.getPrefix())
        .withSourceDockerImage(sourceDockerImageName)
        .withSourceProtocolVersion(sourceProtocolVersion)
        .withDestinationDockerImage(destinationDockerImageName)
        .withDestinationProtocolVersion(destinationProtocolVersion)
        .withOperationSequence(standardSyncOperations)
        .withWebhookOperationConfigs(webhookOperationConfigs)
        .withConfiguredAirbyteCatalog(standardSync.getCatalog())
        .withSyncResourceRequirements(syncResourceRequirements)
        .withIsSourceCustomConnector(sourceDefinition.getCustom())
        .withIsDestinationCustomConnector(destinationDefinition.getCustom())
        .withWorkspaceId(workspaceId)
        .withSourceDefinitionVersionId(sourceDefinitionVersion.getVersionId())
        .withDestinationDefinitionVersionId(destinationDefinitionVersion.getVersionId())
        .withStreamsToRefresh(
            streamsToRefresh.stream().map(streamRefresh -> new RefreshStream()
                .withRefreshType(convertToApi(streamRefresh.getRefreshType()))
                .withStreamDescriptor(new StreamDescriptor()
                    .withName(streamRefresh.getStreamName())
                    .withNamespace(streamRefresh.getStreamNamespace())))
                .toList());

    streamsToRefresh.forEach(
        s -> streamRefreshesRepository.deleteByConnectionIdAndStreamNameAndStreamNamespace(standardSync.getConnectionId(), s.getStreamName(),
            s.getStreamNamespace()));

    final JobConfig jobConfig = new JobConfig()
        .withConfigType(ConfigType.REFRESH)
        .withRefresh(refreshConfig);

    return jobPersistence.enqueueJob(standardSync.getConnectionId().toString(), jobConfig, false);
  }

  @VisibleForTesting
  Set<StreamDescriptor> getResumableFullRefresh(final StandardSync standardSync, final boolean supportResumableFullRefresh) {
    if (!supportResumableFullRefresh) {
      return Set.of();
    }

    return standardSync.getCatalog().getStreams().stream()
        .filter(stream -> stream.getSyncMode() == SyncMode.FULL_REFRESH
            && stream.getStream().isResumable() != null
            && stream.getStream().isResumable())
        .map(ConfiguredAirbyteStream::getStreamDescriptor)
        .collect(Collectors.toSet());
  }

  @Override
  public Optional<Long> createResetConnectionJob(final DestinationConnection destination,
                                                 final StandardSync standardSync,
                                                 final StandardDestinationDefinition destinationDefinition,
                                                 final ActorDefinitionVersion destinationDefinitionVersion,
                                                 final String destinationDockerImage,
                                                 final Version destinationProtocolVersion,
                                                 final boolean isDestinationCustomConnector,
                                                 final List<StandardSyncOperation> standardSyncOperations,
                                                 final List<StreamDescriptor> streamsToReset,
                                                 final UUID workspaceId)
      throws IOException {
    final ConfiguredAirbyteCatalog immutableConfiguredAirbyteCatalog = standardSync.getCatalog();
    final ConfiguredAirbyteCatalog configuredAirbyteCatalog = new ConfiguredAirbyteCatalog()
        .withStreams(new ArrayList<>(immutableConfiguredAirbyteCatalog.getStreams()));
    CatalogTransforms.updateCatalogForReset(streamsToReset, configuredAirbyteCatalog);

    final var resetResourceRequirements =
        getSyncResourceRequirements(workspaceId, null, destination, standardSync, RESET_SOURCE_DEFINITION, destinationDefinition, true);

    final JobResetConnectionConfig resetConnectionConfig = new JobResetConnectionConfig()
        .withNamespaceDefinition(standardSync.getNamespaceDefinition())
        .withNamespaceFormat(standardSync.getNamespaceFormat())
        .withPrefix(standardSync.getPrefix())
        .withDestinationDockerImage(destinationDockerImage)
        .withDestinationProtocolVersion(destinationProtocolVersion)
        .withOperationSequence(standardSyncOperations)
        .withConfiguredAirbyteCatalog(configuredAirbyteCatalog)
        .withResourceRequirements(resetResourceRequirements.getOrchestrator())
        .withSyncResourceRequirements(resetResourceRequirements)
        .withResetSourceConfiguration(new ResetSourceConfiguration().withStreamsToReset(streamsToReset))
        .withIsSourceCustomConnector(false)
        .withIsDestinationCustomConnector(isDestinationCustomConnector)
        .withWorkspaceId(destination.getWorkspaceId())
        .withDestinationDefinitionVersionId(destinationDefinitionVersion.getVersionId());

    final JobConfig jobConfig = new JobConfig()
        .withConfigType(ConfigType.RESET_CONNECTION)
        .withResetConnection(resetConnectionConfig);
    return jobPersistence.enqueueJob(standardSync.getConnectionId().toString(), jobConfig, false);
  }

  private SyncResourceRequirements getSyncResourceRequirements(final UUID workspaceId,
                                                               final Optional<SourceConnection> source,
                                                               final DestinationConnection destination,
                                                               final StandardSync standardSync,
                                                               final StandardSourceDefinition sourceDefinition,
                                                               final StandardDestinationDefinition destinationDefinition,
                                                               final boolean isReset) {
    final var ffContext = buildFeatureFlagContext(workspaceId, standardSync, sourceDefinition, destinationDefinition);
    final String variant =
        variantOverride == null || variantOverride.isBlank() ? featureFlagClient.stringVariation(UseResourceRequirementsVariant.INSTANCE, ffContext)
            : variantOverride;

    // Note on use of sourceType, throughput is driven by the source, if the source is slow, the rest is
    // going to be slow. With this in mind, we align the resources given to the orchestrator and the
    // destination based on the source to avoid oversizing orchestrator and destination when the source
    // is slow.
    final Optional<String> sourceType = getSourceType(sourceDefinition);
    final ResourceRequirements mergedOrchestratorResourceReq = getOrchestratorResourceRequirements(standardSync, sourceType, variant, ffContext);
    final ResourceRequirements mergedDstResourceReq =
        getDestinationResourceRequirements(standardSync, destination, destinationDefinition, sourceType, variant, ffContext);

    final var syncResourceRequirements = new SyncResourceRequirements()
        .withConfigKey(new SyncResourceRequirementsKey().withVariant(variant).withSubType(sourceType.orElse(null)))
        .withDestination(mergedDstResourceReq)
        .withOrchestrator(mergedOrchestratorResourceReq);

    if (!isReset) {
      if (source == null || source.isEmpty()) {
        log.error(
            "`source` is expected for all jobs except reset, but was not present. "
                + "Resource requirements set for the source are being skipped.");
        return syncResourceRequirements;
      }
      final ResourceRequirements mergedSrcResourceReq =
          getSourceResourceRequirements(standardSync, source.get(), sourceDefinition, variant, ffContext);
      syncResourceRequirements
          .withSource(mergedSrcResourceReq);
    }

    return syncResourceRequirements;
  }

  private Context buildFeatureFlagContext(final UUID workspaceId,
                                          final StandardSync standardSync,
                                          final StandardSourceDefinition sourceDefinition,
                                          final StandardDestinationDefinition destinationDefinition) {
    final List<Context> contextList = new ArrayList<>();
    addIfNotNull(contextList, workspaceId, Workspace::new);
    addIfNotNull(contextList, standardSync.getConnectionId(), Connection::new);
    addIfNotNull(contextList, standardSync.getSourceId(), Source::new);
    // Resets use an empty source. Account for lack of source definition.
    addIfNotNull(contextList, sourceDefinition != null ? sourceDefinition.getSourceDefinitionId() : null, SourceDefinition::new);
    addIfNotNull(contextList, standardSync.getDestinationId(), Destination::new);
    addIfNotNull(contextList, destinationDefinition.getDestinationDefinitionId(), DestinationDefinition::new);
    return new Multi(contextList);
  }

  private static void addIfNotNull(final List<Context> contextList, final UUID uuid, final Function<UUID, Context> supplier) {
    if (uuid != null) {
      contextList.add(supplier.apply(uuid));
    }
  }

  private ResourceRequirements getOrchestratorResourceRequirements(final StandardSync standardSync,
                                                                   final Optional<String> sourceType,
                                                                   final String variant,
                                                                   final Context ffContext) {
    final ResourceRequirements defaultOrchestratorRssReqs =
        resourceRequirementsProvider.getResourceRequirements(ResourceRequirementsType.ORCHESTRATOR, sourceType.orElse(null), variant);

    final var mergedRrsReqs = ResourceRequirementsUtils.mergeResourceRequirements(
        standardSync.getResourceRequirements(),
        defaultOrchestratorRssReqs);

    final var overrides = getOrchestratorResourceOverrides(ffContext);

    return ResourceRequirementsUtils.mergeResourceRequirements(overrides, mergedRrsReqs);
  }

  private ResourceRequirements getSourceResourceRequirements(final StandardSync standardSync,
                                                             final SourceConnection source,
                                                             final StandardSourceDefinition sourceDefinition,
                                                             final String variant,
                                                             final Context ffContext) {
    final ResourceRequirements defaultSrcRssReqs =
        resourceRequirementsProvider.getResourceRequirements(ResourceRequirementsType.SOURCE, getSourceType(sourceDefinition).orElse(null), variant);

    final var mergedRssReqs = ResourceRequirementsUtils.getResourceRequirementsForJobType(
        standardSync.getResourceRequirements(),
        source.getResourceRequirements(),
        sourceDefinition != null ? sourceDefinition.getResourceRequirements() : null,
        defaultSrcRssReqs,
        JobType.SYNC);

    final var overrides = getSourceResourceOverrides(ffContext);

    return ResourceRequirementsUtils.mergeResourceRequirements(overrides, mergedRssReqs);
  }

  private ResourceRequirements getDestinationResourceRequirements(final StandardSync standardSync,
                                                                  final DestinationConnection destination,
                                                                  final StandardDestinationDefinition destinationDefinition,
                                                                  final Optional<String> sourceType,
                                                                  final String variant,
                                                                  final Context ffContext) {
    final ResourceRequirements defaultDstRssReqs =
        resourceRequirementsProvider.getResourceRequirements(ResourceRequirementsType.DESTINATION, sourceType.orElse(null), variant);

    final var mergedRssReqs = ResourceRequirementsUtils.getResourceRequirementsForJobType(
        standardSync.getResourceRequirements(),
        destination.getResourceRequirements(),
        destinationDefinition.getResourceRequirements(),
        defaultDstRssReqs,
        JobType.SYNC);

    final var overrides = getDestinationResourceOverrides(ffContext);

    return ResourceRequirementsUtils.mergeResourceRequirements(overrides, mergedRssReqs);
  }

  private ResourceRequirements getDestinationResourceOverrides(final Context ffCtx) {
    final String destOverrides = featureFlagClient.stringVariation(DestResourceOverrides.INSTANCE, ffCtx);
    try {
      return ResourceRequirementsUtils.parse(destOverrides);
    } catch (final Exception e) {
      log.warn("Could not parse DESTINATION resource overrides '{}' from feature flag string: {}", destOverrides, e.getMessage());
      return null;
    }
  }

  private ResourceRequirements getOrchestratorResourceOverrides(final Context ffCtx) {
    final String orchestratorOverrides = featureFlagClient.stringVariation(OrchestratorResourceOverrides.INSTANCE, ffCtx);
    try {
      return ResourceRequirementsUtils.parse(orchestratorOverrides);
    } catch (final Exception e) {
      log.warn("Could not parse ORCHESTRATOR resource overrides '{}' from feature flag string: {}", orchestratorOverrides, e.getMessage());
      return null;
    }
  }

  private ResourceRequirements getSourceResourceOverrides(final Context ffCtx) {
    final String sourceOverrides = featureFlagClient.stringVariation(SourceResourceOverrides.INSTANCE, ffCtx);
    try {
      return ResourceRequirementsUtils.parse(sourceOverrides);
    } catch (final Exception e) {
      log.warn("Could not parse SOURCE resource overrides '{}' from feature flag string: {}", sourceOverrides, e.getMessage());
      return null;
    }
  }

  private Optional<String> getSourceType(final StandardSourceDefinition sourceDefinition) {
    if (sourceDefinition == null) {
      return Optional.empty();
    }
    return Optional.ofNullable(sourceDefinition.getSourceType()).map(SourceType::toString);
  }

  private static RefreshType convertToApi(final io.airbyte.db.instance.configs.jooq.generated.enums.RefreshType type) {
    switch (type) {
      case MERGE -> {
        return RefreshType.MERGE;
      }
      case TRUNCATE -> {
        return RefreshType.TRUNCATE;
      }
      default -> throw new IllegalStateException("Unsupported enum value: " + type.getLiteral());
    }
  }

}
