/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.persistence.job;

import static io.airbyte.config.provider.ResourceRequirementsProvider.DEFAULT_VARIANT;

import com.fasterxml.jackson.databind.JsonNode;
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
import io.airbyte.config.provider.ResourceRequirementsProvider;
import io.airbyte.featureflag.Connection;
import io.airbyte.featureflag.Context;
import io.airbyte.featureflag.Destination;
import io.airbyte.featureflag.DestinationDefinition;
import io.airbyte.featureflag.FeatureFlagClient;
import io.airbyte.featureflag.Multi;
import io.airbyte.featureflag.Source;
import io.airbyte.featureflag.SourceDefinition;
import io.airbyte.featureflag.UseResourceRequirementsVariant;
import io.airbyte.featureflag.Workspace;
import io.airbyte.protocol.models.CatalogHelpers;
import io.airbyte.protocol.models.ConfiguredAirbyteCatalog;
import io.airbyte.protocol.models.DestinationSyncMode;
import io.airbyte.protocol.models.StreamDescriptor;
import io.airbyte.protocol.models.SyncMode;
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
        getSyncResourceRequirements(workspaceId, standardSync, sourceDefinition, destinationDefinition);

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
        .withResourceRequirements(syncResourceRequirements.getOrchestrator())
        .withSourceResourceRequirements(syncResourceRequirements.getSource())
        .withDestinationResourceRequirements(syncResourceRequirements.getDestination())
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
                                                 final ActorDefinitionVersion destinationDefinitionVersion,
                                                 final String destinationDockerImage,
                                                 final Version destinationProtocolVersion,
                                                 final boolean isDestinationCustomConnector,
                                                 final List<StandardSyncOperation> standardSyncOperations,
                                                 final List<StreamDescriptor> streamsToReset)
      throws IOException {
    final ConfiguredAirbyteCatalog configuredAirbyteCatalog = standardSync.getCatalog();
    configuredAirbyteCatalog.getStreams().forEach(configuredAirbyteStream -> {
      final StreamDescriptor streamDescriptor = CatalogHelpers.extractDescriptor(configuredAirbyteStream);
      if (streamsToReset.contains(streamDescriptor)) {
        // The Reset Source will emit no record messages for any streams, so setting the destination sync
        // mode to OVERWRITE will empty out this stream in the destination.
        // Note: streams in streamsToReset that are NOT in this configured catalog (i.e. deleted streams)
        // will still have their state reset by the Reset Source, but will not be modified in the
        // destination since they are not present in the catalog that is sent to the destination.
        configuredAirbyteStream.setSyncMode(SyncMode.FULL_REFRESH);
        configuredAirbyteStream.setDestinationSyncMode(DestinationSyncMode.OVERWRITE);
      } else {
        // Set streams that are not being reset to APPEND so that they are not modified in the destination
        if (configuredAirbyteStream.getDestinationSyncMode() == DestinationSyncMode.OVERWRITE) {
          configuredAirbyteStream.setDestinationSyncMode(DestinationSyncMode.APPEND);
        }
      }
    });

    final JobResetConnectionConfig resetConnectionConfig = new JobResetConnectionConfig()
        .withNamespaceDefinition(standardSync.getNamespaceDefinition())
        .withNamespaceFormat(standardSync.getNamespaceFormat())
        .withPrefix(standardSync.getPrefix())
        .withDestinationDockerImage(destinationDockerImage)
        .withDestinationProtocolVersion(destinationProtocolVersion)
        .withOperationSequence(standardSyncOperations)
        .withConfiguredAirbyteCatalog(configuredAirbyteCatalog)
        // We should lookup variant here but we are missing some information such as workspaceId
        .withResourceRequirements(getOrchestratorResourceRequirements(standardSync, Optional.empty(), DEFAULT_VARIANT))
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
                                                               final StandardDestinationDefinition destinationDefinition) {
    final String variant = getResourceRequirementsVariant(workspaceId, standardSync, sourceDefinition, destinationDefinition);

    // Note on use of sourceType, throughput is driven by the source, if the source is slow, the rest is
    // going to be slow. With this in mind, we align the resources given to the orchestrator and the
    // destination based on the source to avoid oversizing orchestrator and destination when the source
    // is slow.
    final Optional<String> sourceType = getSourceType(sourceDefinition);
    final ResourceRequirements mergedOrchestratorResourceReq = getOrchestratorResourceRequirements(standardSync, sourceType, variant);
    final ResourceRequirements mergedSrcResourceReq = getSourceResourceRequirements(standardSync, sourceDefinition, variant);
    final ResourceRequirements mergedDstResourceReq = getDestinationResourceRequirements(standardSync, destinationDefinition, sourceType, variant);

    return new SyncResourceRequirements()
        .withDestination(mergedDstResourceReq)
        .withDestinationStdErr(resourceRequirementsProvider.getResourceRequirements(ResourceRequirementsType.DESTINATION_STDERR, sourceType, variant))
        .withDestinationStdIn(resourceRequirementsProvider.getResourceRequirements(ResourceRequirementsType.DESTINATION_STDIN, sourceType, variant))
        .withDestinationStdOut(resourceRequirementsProvider.getResourceRequirements(ResourceRequirementsType.DESTINATION_STDOUT, sourceType, variant))
        .withHeartbeat(resourceRequirementsProvider.getResourceRequirements(ResourceRequirementsType.HEARTBEAT, sourceType, variant))
        .withOrchestrator(mergedOrchestratorResourceReq)
        .withSource(mergedSrcResourceReq)
        .withSourceStdErr(resourceRequirementsProvider.getResourceRequirements(ResourceRequirementsType.SOURCE_STDERR, sourceType, variant))
        .withSourceStdOut(resourceRequirementsProvider.getResourceRequirements(ResourceRequirementsType.SOURCE_STDOUT, sourceType, variant));
  }

  private String getResourceRequirementsVariant(final UUID workspaceId,
                                                final StandardSync standardSync,
                                                final StandardSourceDefinition sourceDefinition,
                                                final StandardDestinationDefinition destinationDefinition) {
    final List<Context> contextList = new ArrayList<>();
    addIfNotNull(contextList, workspaceId, Workspace::new);
    addIfNotNull(contextList, standardSync.getConnectionId(), Connection::new);
    addIfNotNull(contextList, standardSync.getSourceId(), Source::new);
    addIfNotNull(contextList, sourceDefinition.getSourceDefinitionId(), SourceDefinition::new);
    addIfNotNull(contextList, standardSync.getDestinationId(), Destination::new);
    addIfNotNull(contextList, destinationDefinition.getDestinationDefinitionId(), DestinationDefinition::new);
    return featureFlagClient.stringVariation(UseResourceRequirementsVariant.INSTANCE, new Multi(contextList));
  }

  private static void addIfNotNull(final List<Context> contextList, final UUID uuid, final Function<UUID, Context> supplier) {
    if (uuid != null) {
      contextList.add(supplier.apply(uuid));
    }
  }

  private ResourceRequirements getOrchestratorResourceRequirements(final StandardSync standardSync,
                                                                   final Optional<String> sourceType,
                                                                   final String variant) {
    final ResourceRequirements defaultOrchestratorRssReqs =
        resourceRequirementsProvider.getResourceRequirements(ResourceRequirementsType.ORCHESTRATOR, sourceType, variant);
    return ResourceRequirementsUtils.getResourceRequirements(
        standardSync.getResourceRequirements(),
        defaultOrchestratorRssReqs);
  }

  private ResourceRequirements getSourceResourceRequirements(final StandardSync standardSync,
                                                             final StandardSourceDefinition sourceDefinition,
                                                             final String variant) {
    final ResourceRequirements defaultSrcRssReqs =
        resourceRequirementsProvider.getResourceRequirements(ResourceRequirementsType.SOURCE, getSourceType(sourceDefinition), variant);
    return ResourceRequirementsUtils.getResourceRequirements(
        standardSync.getResourceRequirements(),
        sourceDefinition.getResourceRequirements(),
        defaultSrcRssReqs,
        JobType.SYNC);
  }

  private ResourceRequirements getDestinationResourceRequirements(final StandardSync standardSync,
                                                                  final StandardDestinationDefinition destinationDefinition,
                                                                  final Optional<String> sourceType,
                                                                  final String variant) {
    final ResourceRequirements defaultDstRssReqs =
        resourceRequirementsProvider.getResourceRequirements(ResourceRequirementsType.DESTINATION, sourceType, variant);
    return ResourceRequirementsUtils.getResourceRequirements(
        standardSync.getResourceRequirements(),
        destinationDefinition.getResourceRequirements(),
        defaultDstRssReqs,
        JobType.SYNC);
  }

  private Optional<String> getSourceType(final StandardSourceDefinition sourceDefinition) {
    return Optional.ofNullable(sourceDefinition.getSourceType()).map(SourceType::toString);
  }

}
