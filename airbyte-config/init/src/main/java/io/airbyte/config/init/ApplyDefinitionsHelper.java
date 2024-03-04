/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.init;

import static io.airbyte.metrics.lib.OssMetricsRegistry.CONNECTOR_REGISTRY_DEFINITION_PROCESSED;

import io.airbyte.commons.version.AirbyteProtocolVersion;
import io.airbyte.commons.version.AirbyteProtocolVersionRange;
import io.airbyte.config.ActorDefinitionBreakingChange;
import io.airbyte.config.ActorDefinitionVersion;
import io.airbyte.config.ConnectorRegistryDestinationDefinition;
import io.airbyte.config.ConnectorRegistrySourceDefinition;
import io.airbyte.config.StandardDestinationDefinition;
import io.airbyte.config.StandardSourceDefinition;
import io.airbyte.config.helpers.ConnectorRegistryConverters;
import io.airbyte.config.init.ApplyDefinitionMetricsHelper.DefinitionProcessingFailureReason;
import io.airbyte.config.init.ApplyDefinitionMetricsHelper.DefinitionProcessingOutcome;
import io.airbyte.config.init.ApplyDefinitionMetricsHelper.DefinitionProcessingSuccessOutcome;
import io.airbyte.config.specs.DefinitionsProvider;
import io.airbyte.data.exceptions.ConfigNotFoundException;
import io.airbyte.data.services.ActorDefinitionService;
import io.airbyte.data.services.DestinationService;
import io.airbyte.data.services.SourceService;
import io.airbyte.metrics.lib.MetricAttribute;
import io.airbyte.metrics.lib.MetricClient;
import io.airbyte.persistence.job.JobPersistence;
import io.airbyte.validation.json.JsonValidationException;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper class used to apply actor definitions from a DefinitionsProvider to the database. This is
 * here to enable easy reuse of definition application logic in bootloader and cron.
 */
@Singleton
@Requires(bean = JobPersistence.class)
@Requires(bean = MetricClient.class)
@Slf4j
public class ApplyDefinitionsHelper {

  private final DefinitionsProvider definitionsProvider;
  private final JobPersistence jobPersistence;
  private final ActorDefinitionService actorDefinitionService;
  private final SourceService sourceService;
  private final DestinationService destinationService;
  private final SupportStateUpdater supportStateUpdater;
  private final MetricClient metricClient;
  private int newConnectorCount;
  private int changedConnectorCount;
  private static final Logger LOGGER = LoggerFactory.getLogger(ApplyDefinitionsHelper.class);

  public ApplyDefinitionsHelper(@Named("seedDefinitionsProvider") final DefinitionsProvider definitionsProvider,
                                final JobPersistence jobPersistence,
                                final ActorDefinitionService actorDefinitionService,
                                final SourceService sourceService,
                                final DestinationService destinationService,
                                final MetricClient metricClient,
                                final SupportStateUpdater supportStateUpdater) {
    this.definitionsProvider = definitionsProvider;
    this.jobPersistence = jobPersistence;
    this.actorDefinitionService = actorDefinitionService;
    this.sourceService = sourceService;
    this.destinationService = destinationService;
    this.metricClient = metricClient;
    this.supportStateUpdater = supportStateUpdater;
  }

  public void apply() throws JsonValidationException, IOException, ConfigNotFoundException, io.airbyte.config.persistence.ConfigNotFoundException {
    apply(false);
  }

  /**
   * Apply the latest definitions from the provider to the repository.
   *
   * @param updateAll - Whether we should overwrite all stored definitions. If true, we do not
   *        consider whether a definition is in use before updating the definition and default
   *        version.
   */
  public void apply(final boolean updateAll)
      throws JsonValidationException, IOException, ConfigNotFoundException, io.airbyte.config.persistence.ConfigNotFoundException {
    final List<ConnectorRegistrySourceDefinition> latestSourceDefinitions = definitionsProvider.getSourceDefinitions();
    final List<ConnectorRegistryDestinationDefinition> latestDestinationDefinitions = definitionsProvider.getDestinationDefinitions();

    final Optional<AirbyteProtocolVersionRange> currentProtocolRange = jobPersistence.getCurrentProtocolVersionRange();
    final List<ConnectorRegistrySourceDefinition> protocolCompatibleSourceDefinitions =
        filterOutIncompatibleSourceDefs(currentProtocolRange, latestSourceDefinitions);
    final List<ConnectorRegistryDestinationDefinition> protocolCompatibleDestinationDefinitions =
        filterOutIncompatibleDestDefs(currentProtocolRange, latestDestinationDefinitions);

    final Map<UUID, ActorDefinitionVersion> actorDefinitionIdsToDefaultVersionsMap =
        actorDefinitionService.getActorDefinitionIdsToDefaultVersionsMap();
    final Set<UUID> actorDefinitionIdsInUse = actorDefinitionService.getActorDefinitionIdsInUse();

    newConnectorCount = 0;
    changedConnectorCount = 0;

    for (final ConnectorRegistrySourceDefinition def : protocolCompatibleSourceDefinitions) {
      applySourceDefinition(actorDefinitionIdsToDefaultVersionsMap, def, actorDefinitionIdsInUse, updateAll);
    }
    for (final ConnectorRegistryDestinationDefinition def : protocolCompatibleDestinationDefinitions) {
      applyDestinationDefinition(actorDefinitionIdsToDefaultVersionsMap, def, actorDefinitionIdsInUse, updateAll);
    }
    supportStateUpdater.updateSupportStates();

    LOGGER.info("New connectors added: {}", newConnectorCount);
    LOGGER.info("Version changes applied: {}", changedConnectorCount);
  }

  private void applySourceDefinition(final Map<UUID, ActorDefinitionVersion> actorDefinitionIdsAndDefaultVersions,
                                     final ConnectorRegistrySourceDefinition newDef,
                                     final Set<UUID> actorDefinitionIdsInUse,
                                     final boolean updateAll)
      throws IOException, JsonValidationException, ConfigNotFoundException {
    // Skip and log if unable to parse registry entry.
    final StandardSourceDefinition newSourceDef;
    final ActorDefinitionVersion newADV;
    final List<ActorDefinitionBreakingChange> breakingChangesForDef;
    try {
      newSourceDef = ConnectorRegistryConverters.toStandardSourceDefinition(newDef);
      newADV = ConnectorRegistryConverters.toActorDefinitionVersion(newDef);
      breakingChangesForDef = ConnectorRegistryConverters.toActorDefinitionBreakingChanges(newDef);
    } catch (final IllegalArgumentException e) {
      LOGGER.error("Failed to convert source definition: {}", newDef.getName(), e);
      trackDefinitionProcessed(newDef.getDockerRepository(), newDef.getDockerImageTag(),
          DefinitionProcessingFailureReason.DEFINITION_CONVERSION_FAILED);
      return;
    }

    final boolean connectorIsNew = !actorDefinitionIdsAndDefaultVersions.containsKey(newSourceDef.getSourceDefinitionId());
    if (connectorIsNew) {
      LOGGER.info("Adding new connector {}:{}", newDef.getDockerRepository(), newDef.getDockerImageTag());
      sourceService.writeConnectorMetadata(newSourceDef, newADV, breakingChangesForDef);
      newConnectorCount++;
      trackDefinitionProcessed(newDef.getDockerRepository(), newDef.getDockerImageTag(), DefinitionProcessingSuccessOutcome.INITIAL_VERSION_ADDED);
      return;
    }

    final ActorDefinitionVersion currentDefaultADV = actorDefinitionIdsAndDefaultVersions.get(newSourceDef.getSourceDefinitionId());
    final boolean shouldUpdateActorDefinitionDefaultVersion =
        getShouldUpdateActorDefinitionDefaultVersion(currentDefaultADV, newADV, actorDefinitionIdsInUse, updateAll);

    if (shouldUpdateActorDefinitionDefaultVersion) {
      LOGGER.info("Updating default version for connector {}: {} -> {}", currentDefaultADV.getDockerRepository(),
          currentDefaultADV.getDockerImageTag(),
          newADV.getDockerImageTag());
      sourceService.writeConnectorMetadata(newSourceDef, newADV, breakingChangesForDef);
      changedConnectorCount++;
      trackDefinitionProcessed(newDef.getDockerRepository(), newDef.getDockerImageTag(), DefinitionProcessingSuccessOutcome.DEFAULT_VERSION_UPDATED);
    } else {
      sourceService.updateStandardSourceDefinition(newSourceDef);
      trackDefinitionProcessed(newDef.getDockerRepository(), newDef.getDockerImageTag(), DefinitionProcessingSuccessOutcome.VERSION_UNCHANGED);
    }
  }

  private void applyDestinationDefinition(final Map<UUID, ActorDefinitionVersion> actorDefinitionIdsAndDefaultVersions,
                                          final ConnectorRegistryDestinationDefinition newDef,
                                          final Set<UUID> actorDefinitionIdsInUse,
                                          final boolean updateAll)
      throws IOException, JsonValidationException, io.airbyte.data.exceptions.ConfigNotFoundException {
    // Skip and log if unable to parse registry entry.
    final StandardDestinationDefinition newDestinationDef;
    final ActorDefinitionVersion newADV;
    final List<ActorDefinitionBreakingChange> breakingChangesForDef;
    try {
      newDestinationDef = ConnectorRegistryConverters.toStandardDestinationDefinition(newDef);
      newADV = ConnectorRegistryConverters.toActorDefinitionVersion(newDef);
      breakingChangesForDef = ConnectorRegistryConverters.toActorDefinitionBreakingChanges(newDef);
    } catch (final IllegalArgumentException e) {
      LOGGER.error("Failed to convert source definition: {}", newDef.getName(), e);
      trackDefinitionProcessed(newDef.getDockerRepository(), newDef.getDockerImageTag(),
          DefinitionProcessingFailureReason.DEFINITION_CONVERSION_FAILED);
      return;
    }

    final boolean connectorIsNew = !actorDefinitionIdsAndDefaultVersions.containsKey(newDestinationDef.getDestinationDefinitionId());
    if (connectorIsNew) {
      LOGGER.info("Adding new connector {}:{}", newDef.getDockerRepository(), newDef.getDockerImageTag());
      destinationService.writeConnectorMetadata(newDestinationDef, newADV, breakingChangesForDef);
      newConnectorCount++;
      trackDefinitionProcessed(newDef.getDockerRepository(), newDef.getDockerImageTag(), DefinitionProcessingSuccessOutcome.INITIAL_VERSION_ADDED);
      return;
    }

    final ActorDefinitionVersion currentDefaultADV = actorDefinitionIdsAndDefaultVersions.get(newDestinationDef.getDestinationDefinitionId());
    final boolean shouldUpdateActorDefinitionDefaultVersion =
        getShouldUpdateActorDefinitionDefaultVersion(currentDefaultADV, newADV, actorDefinitionIdsInUse, updateAll);

    if (shouldUpdateActorDefinitionDefaultVersion) {
      LOGGER.info("Updating default version for connector {}: {} -> {}", currentDefaultADV.getDockerRepository(),
          currentDefaultADV.getDockerImageTag(),
          newADV.getDockerImageTag());
      destinationService.writeConnectorMetadata(newDestinationDef, newADV, breakingChangesForDef);
      changedConnectorCount++;
      trackDefinitionProcessed(newDef.getDockerRepository(), newDef.getDockerImageTag(), DefinitionProcessingSuccessOutcome.DEFAULT_VERSION_UPDATED);
    } else {
      destinationService.updateStandardDestinationDefinition(newDestinationDef);
      trackDefinitionProcessed(newDef.getDockerRepository(), newDef.getDockerImageTag(), DefinitionProcessingSuccessOutcome.VERSION_UNCHANGED);
    }

  }

  private boolean getShouldUpdateActorDefinitionDefaultVersion(final ActorDefinitionVersion currentDefaultADV,
                                                               final ActorDefinitionVersion newADV,
                                                               final Set<UUID> actorDefinitionIdsInUse,
                                                               final boolean updateAll) {
    final boolean newVersionIsAvailable = !newADV.getDockerImageTag().equals(currentDefaultADV.getDockerImageTag());
    final boolean definitionIsInUse = actorDefinitionIdsInUse.contains(currentDefaultADV.getActorDefinitionId());
    final boolean shouldApplyNewVersion = updateAll || !definitionIsInUse;

    return newVersionIsAvailable && shouldApplyNewVersion;
  }

  private List<ConnectorRegistryDestinationDefinition> filterOutIncompatibleDestDefs(final Optional<AirbyteProtocolVersionRange> protocolVersionRange,
                                                                                     final List<ConnectorRegistryDestinationDefinition> destDefs) {
    if (protocolVersionRange.isEmpty()) {
      return destDefs;
    }

    return destDefs.stream().filter(def -> {
      final boolean isSupported = isProtocolVersionSupported(protocolVersionRange.get(), def.getSpec().getProtocolVersion());
      if (!isSupported) {
        LOGGER.warn("Destination {} {} has an incompatible protocol version ({})... ignoring.",
            def.getDestinationDefinitionId(), def.getName(), def.getSpec().getProtocolVersion());
        trackDefinitionProcessed(def.getDockerRepository(), def.getDockerImageTag(), DefinitionProcessingFailureReason.INCOMPATIBLE_PROTOCOL_VERSION);
      }
      return isSupported;
    }).toList();
  }

  private List<ConnectorRegistrySourceDefinition> filterOutIncompatibleSourceDefs(final Optional<AirbyteProtocolVersionRange> protocolVersionRange,
                                                                                  final List<ConnectorRegistrySourceDefinition> sourceDefs) {
    if (protocolVersionRange.isEmpty()) {
      return sourceDefs;
    }

    return sourceDefs.stream().filter(def -> {
      final boolean isSupported = isProtocolVersionSupported(protocolVersionRange.get(), def.getSpec().getProtocolVersion());
      if (!isSupported) {
        LOGGER.warn("Source {} {} has an incompatible protocol version ({})... ignoring.",
            def.getSourceDefinitionId(), def.getName(), def.getSpec().getProtocolVersion());
        trackDefinitionProcessed(def.getDockerRepository(), def.getDockerImageTag(), DefinitionProcessingFailureReason.INCOMPATIBLE_PROTOCOL_VERSION);
      }
      return isSupported;
    }).toList();
  }

  private boolean isProtocolVersionSupported(final AirbyteProtocolVersionRange protocolVersionRange, final String protocolVersion) {
    return protocolVersionRange.isSupported(AirbyteProtocolVersion.getWithDefault(protocolVersion));
  }

  private void trackDefinitionProcessed(final String dockerRepository, final String dockerImageTag, final DefinitionProcessingOutcome outcome) {
    final MetricAttribute[] attributes = ApplyDefinitionMetricsHelper.getMetricAttributes(dockerRepository, dockerImageTag, outcome);
    metricClient.count(CONNECTOR_REGISTRY_DEFINITION_PROCESSED, 1, attributes);
  }

}
