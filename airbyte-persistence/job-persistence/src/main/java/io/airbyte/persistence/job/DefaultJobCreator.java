/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.persistence.job;

import com.fasterxml.jackson.databind.JsonNode;
import io.airbyte.commons.protocol.CatalogTransforms;
import io.airbyte.commons.version.Version;
import io.airbyte.config.ActorDefinitionVersion;
import io.airbyte.config.DestinationConnection;
import io.airbyte.config.JobConfig;
import io.airbyte.config.JobConfig.ConfigType;
import io.airbyte.config.JobResetConnectionConfig;
import io.airbyte.config.JobSyncConfig;
import io.airbyte.config.JobTypeResourceLimit.JobType;
import io.airbyte.config.ResetSourceConfiguration;
import io.airbyte.config.ResourceRequirements;
import io.airbyte.config.ResourceRequirementsType;
import io.airbyte.config.SourceConnection;
import io.airbyte.config.StandardDestinationDefinition;
import io.airbyte.config.StandardSourceDefinition;
import io.airbyte.config.StandardSourceDefinition.SourceType;
import io.airbyte.config.StandardSync;
import io.airbyte.config.StandardSyncOperation;
import io.airbyte.config.SyncResourceRequirements;
import io.airbyte.config.SyncResourceRequirementsKey;
import io.airbyte.config.helpers.ResourceRequirementsUtils;
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
import io.airbyte.protocol.models.ConfiguredAirbyteCatalog;
import io.airbyte.protocol.models.StreamDescriptor;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Function;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/**
 * Default implementation of enqueueing a job. Hides the details of building the Job object and
 * storing it in the jobs db.
 */
@Slf4j
public class DefaultJobCreator implements JobCreator {

  // Resets use an empty source which doesn't have a source definition.
  private static final StandardSourceDefinition RESET_SOURCE_DEFINITION = null;
  private final JobPersistence jobPersistence;
  private final ResourceRequirementsProvider resourceRequirementsProvider;
  private final FeatureFlagClient featureFlagClient;

  public DefaultJobCreator(final JobPersistence jobPersistence,
                           final ResourceRequirementsProvider resourceRequirementsProvider,
                           final FeatureFlagClient featureFlagClient) {
    this.jobPersistence = jobPersistence;
    this.resourceRequirementsProvider = resourceRequirementsProvider;
    this.featureFlagClient = featureFlagClient;
  }

  @Override
  public Optional<Long> createSyncJob(final SourceConnection source,
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
                                      final UUID workspaceId)
      throws IOException {
    final SyncResourceRequirements syncResourceRequirements =
        getSyncResourceRequirements(workspaceId, standardSync, sourceDefinition, destinationDefinition, false);

    final JobSyncConfig jobSyncConfig = new JobSyncConfig()
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
        .withDestinationDefinitionVersionId(destinationDefinitionVersion.getVersionId());

    final JobConfig jobConfig = new JobConfig()
        .withConfigType(ConfigType.SYNC)
        .withSync(jobSyncConfig);
    return jobPersistence.enqueueJob(standardSync.getConnectionId().toString(), jobConfig);
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
        getSyncResourceRequirements(workspaceId, standardSync, RESET_SOURCE_DEFINITION, destinationDefinition, true);

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
    return jobPersistence.enqueueJob(standardSync.getConnectionId().toString(), jobConfig);
  }

  private SyncResourceRequirements getSyncResourceRequirements(final UUID workspaceId,
                                                               final StandardSync standardSync,
                                                               final StandardSourceDefinition sourceDefinition,
                                                               final StandardDestinationDefinition destinationDefinition,
                                                               final boolean isReset) {
    final var ffContext = buildFeatureFlagContext(workspaceId, standardSync, sourceDefinition, destinationDefinition);
    final String variant = featureFlagClient.stringVariation(UseResourceRequirementsVariant.INSTANCE, ffContext);

    // Note on use of sourceType, throughput is driven by the source, if the source is slow, the rest is
    // going to be slow. With this in mind, we align the resources given to the orchestrator and the
    // destination based on the source to avoid oversizing orchestrator and destination when the source
    // is slow.
    final Optional<String> sourceType = getSourceType(sourceDefinition);
    final ResourceRequirements mergedOrchestratorResourceReq = getOrchestratorResourceRequirements(standardSync, sourceType, variant, ffContext);
    final ResourceRequirements mergedDstResourceReq =
        getDestinationResourceRequirements(standardSync, destinationDefinition, sourceType, variant, ffContext);

    final var syncResourceRequirements = new SyncResourceRequirements()
        .withConfigKey(new SyncResourceRequirementsKey().withVariant(variant).withSubType(sourceType.orElse(null)))
        .withDestination(mergedDstResourceReq)
        .withDestinationStdErr(resourceRequirementsProvider.getResourceRequirements(ResourceRequirementsType.DESTINATION_STDERR, sourceType, variant))
        .withDestinationStdIn(resourceRequirementsProvider.getResourceRequirements(ResourceRequirementsType.DESTINATION_STDIN, sourceType, variant))
        .withDestinationStdOut(resourceRequirementsProvider.getResourceRequirements(ResourceRequirementsType.DESTINATION_STDOUT, sourceType, variant))
        .withHeartbeat(resourceRequirementsProvider.getResourceRequirements(ResourceRequirementsType.HEARTBEAT, sourceType, variant))
        .withOrchestrator(mergedOrchestratorResourceReq);

    if (!isReset) {
      final ResourceRequirements mergedSrcResourceReq = getSourceResourceRequirements(standardSync, sourceDefinition, variant, ffContext);
      syncResourceRequirements
          .withSource(mergedSrcResourceReq)
          .withSourceStdErr(resourceRequirementsProvider.getResourceRequirements(ResourceRequirementsType.SOURCE_STDERR, sourceType, variant))
          .withSourceStdOut(resourceRequirementsProvider.getResourceRequirements(ResourceRequirementsType.SOURCE_STDOUT, sourceType, variant));
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
        resourceRequirementsProvider.getResourceRequirements(ResourceRequirementsType.ORCHESTRATOR, sourceType, variant);

    final var mergedRrsReqs = ResourceRequirementsUtils.getResourceRequirements(
        standardSync.getResourceRequirements(),
        defaultOrchestratorRssReqs);

    final var overrides = getOrchestratorResourceOverrides(ffContext);

    return ResourceRequirementsUtils.getResourceRequirements(overrides, mergedRrsReqs);
  }

  private ResourceRequirements getSourceResourceRequirements(final StandardSync standardSync,
                                                             final StandardSourceDefinition sourceDefinition,
                                                             final String variant,
                                                             final Context ffContext) {
    final ResourceRequirements defaultSrcRssReqs =
        resourceRequirementsProvider.getResourceRequirements(ResourceRequirementsType.SOURCE, getSourceType(sourceDefinition), variant);

    final var mergedRssReqs = ResourceRequirementsUtils.getResourceRequirements(
        standardSync.getResourceRequirements(),
        sourceDefinition != null ? sourceDefinition.getResourceRequirements() : null,
        defaultSrcRssReqs,
        JobType.SYNC);

    final var overrides = getSourceResourceOverrides(ffContext);

    return ResourceRequirementsUtils.getResourceRequirements(overrides, mergedRssReqs);
  }

  private ResourceRequirements getDestinationResourceRequirements(final StandardSync standardSync,
                                                                  final StandardDestinationDefinition destinationDefinition,
                                                                  final Optional<String> sourceType,
                                                                  final String variant,
                                                                  final Context ffContext) {
    final ResourceRequirements defaultDstRssReqs =
        resourceRequirementsProvider.getResourceRequirements(ResourceRequirementsType.DESTINATION, sourceType, variant);

    final var mergedRssReqs = ResourceRequirementsUtils.getResourceRequirements(
        standardSync.getResourceRequirements(),
        destinationDefinition.getResourceRequirements(),
        defaultDstRssReqs,
        JobType.SYNC);

    final var overrides = getDestinationResourceOverrides(ffContext);

    return ResourceRequirementsUtils.getResourceRequirements(overrides, mergedRssReqs);
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

}
