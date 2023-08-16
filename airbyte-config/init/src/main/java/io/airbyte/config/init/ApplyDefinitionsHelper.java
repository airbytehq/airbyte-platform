/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.init;

import static io.airbyte.featureflag.ContextKt.ANONYMOUS;

import io.airbyte.commons.version.AirbyteProtocolVersion;
import io.airbyte.commons.version.AirbyteProtocolVersionRange;
import io.airbyte.config.ActorDefinitionBreakingChange;
import io.airbyte.config.ActorDefinitionVersion;
import io.airbyte.config.ConnectorRegistryDestinationDefinition;
import io.airbyte.config.ConnectorRegistrySourceDefinition;
import io.airbyte.config.StandardDestinationDefinition;
import io.airbyte.config.StandardSourceDefinition;
import io.airbyte.config.helpers.ConnectorRegistryConverters;
import io.airbyte.config.persistence.ConfigNotFoundException;
import io.airbyte.config.persistence.ConfigRepository;
import io.airbyte.config.specs.DefinitionsProvider;
import io.airbyte.featureflag.FeatureFlagClient;
import io.airbyte.featureflag.IngestBreakingChanges;
import io.airbyte.featureflag.Workspace;
import io.airbyte.persistence.job.JobPersistence;
import io.airbyte.validation.json.JsonValidationException;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import java.io.IOException;
import java.util.ArrayList;
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
@Requires(bean = ConfigRepository.class)
@Slf4j
public class ApplyDefinitionsHelper {

  private final DefinitionsProvider definitionsProvider;
  private final JobPersistence jobPersistence;
  private final ConfigRepository configRepository;
  private final FeatureFlagClient featureFlagClient;
  private int newConnectorCount;
  private int changedConnectorCount;
  private static final Logger LOGGER = LoggerFactory.getLogger(ApplyDefinitionsHelper.class);

  public ApplyDefinitionsHelper(@Named("seedDefinitionsProvider") final DefinitionsProvider definitionsProvider,
                                final JobPersistence jobPersistence,
                                final ConfigRepository configRepository,
                                final FeatureFlagClient featureFlagClient) {
    this.definitionsProvider = definitionsProvider;
    this.jobPersistence = jobPersistence;
    this.configRepository = configRepository;
    this.featureFlagClient = featureFlagClient;
  }

  public void apply() throws JsonValidationException, IOException, ConfigNotFoundException {
    apply(false);
  }

  /**
   * Apply the latest definitions from the provider to the repository.
   *
   * @param updateAll - Whether we should overwrite all stored definitions. If true, we do not
   *        consider whether a definition is in use before updating the definition and default
   *        version.
   */
  public void apply(final boolean updateAll) throws JsonValidationException, IOException, ConfigNotFoundException {
    final List<ConnectorRegistrySourceDefinition> latestSourceDefinitions = definitionsProvider.getSourceDefinitions();
    final List<ConnectorRegistryDestinationDefinition> latestDestinationDefinitions = definitionsProvider.getDestinationDefinitions();

    final Optional<AirbyteProtocolVersionRange> currentProtocolRange = jobPersistence.getCurrentProtocolVersionRange();
    final List<ConnectorRegistrySourceDefinition> protocolCompatibleSourceDefinitions =
        filterOutIncompatibleSourceDefs(currentProtocolRange, latestSourceDefinitions);
    final List<ConnectorRegistryDestinationDefinition> protocolCompatibleDestinationDefinitions =
        filterOutIncompatibleDestDefs(currentProtocolRange, latestDestinationDefinitions);

    final Map<UUID, ActorDefinitionVersion> actorDefinitionIdsToDefaultVersionsMap = configRepository.getActorDefinitionIdsToDefaultVersionsMap();
    final Set<UUID> actorDefinitionIdsInUse = configRepository.getActorDefinitionIdsInUse();

    final List<ActorDefinitionBreakingChange> breakingChanges = new ArrayList<>();

    newConnectorCount = 0;
    changedConnectorCount = 0;
    for (final ConnectorRegistrySourceDefinition def : protocolCompatibleSourceDefinitions) {
      applySourceDefinition(actorDefinitionIdsToDefaultVersionsMap, def, actorDefinitionIdsInUse, updateAll);
      breakingChanges.addAll(ConnectorRegistryConverters.toActorDefinitionBreakingChanges(def));
    }
    for (final ConnectorRegistryDestinationDefinition def : protocolCompatibleDestinationDefinitions) {
      applyDestinationDefinition(actorDefinitionIdsToDefaultVersionsMap, def, actorDefinitionIdsInUse, updateAll);
      breakingChanges.addAll(ConnectorRegistryConverters.toActorDefinitionBreakingChanges(def));
    }
    if (featureFlagClient.boolVariation(IngestBreakingChanges.INSTANCE, new Workspace(ANONYMOUS))) {
      configRepository.writeActorDefinitionBreakingChanges(breakingChanges);
    }

    LOGGER.info("New connectors added: {}", newConnectorCount);
    LOGGER.info("Version changes applied: {}", changedConnectorCount);
  }

  private void applySourceDefinition(final Map<UUID, ActorDefinitionVersion> actorDefinitionIdsAndDefaultVersions,
                                     final ConnectorRegistrySourceDefinition newDef,
                                     final Set<UUID> actorDefinitionIdsInUse,
                                     final boolean updateAll)
      throws IOException, JsonValidationException, ConfigNotFoundException {
    final StandardSourceDefinition newSourceDef = ConnectorRegistryConverters.toStandardSourceDefinition(newDef);
    final ActorDefinitionVersion newADV = ConnectorRegistryConverters.toActorDefinitionVersion(newDef);

    final boolean connectorIsNew = !actorDefinitionIdsAndDefaultVersions.containsKey(newSourceDef.getSourceDefinitionId());
    if (connectorIsNew) {
      LOGGER.info("Adding new connector {}:{}", newDef.getDockerRepository(), newDef.getDockerImageTag());
      newConnectorCount++;
      configRepository.writeSourceDefinitionAndDefaultVersion(newSourceDef, newADV);
      return;
    }

    final ActorDefinitionVersion currentDefaultADV = actorDefinitionIdsAndDefaultVersions.get(newSourceDef.getSourceDefinitionId());
    final boolean shouldUpdateActorDefinitionDefaultVersion =
        getShouldUpdateActorDefinitionDefaultVersion(currentDefaultADV, newADV, actorDefinitionIdsInUse, updateAll);

    if (shouldUpdateActorDefinitionDefaultVersion) {
      LOGGER.info("Updating default version for connector {}: {} -> {}", currentDefaultADV.getDockerRepository(),
          currentDefaultADV.getDockerImageTag(),
          newADV.getDockerImageTag());
      changedConnectorCount++;
      configRepository.writeSourceDefinitionAndDefaultVersion(newSourceDef, newADV);
    } else {
      configRepository.updateStandardSourceDefinition(newSourceDef);
    }
  }

  private void applyDestinationDefinition(final Map<UUID, ActorDefinitionVersion> actorDefinitionIdsAndDefaultVersions,
                                          final ConnectorRegistryDestinationDefinition newDef,
                                          final Set<UUID> actorDefinitionIdsInUse,
                                          final boolean updateAll)
      throws IOException, JsonValidationException, ConfigNotFoundException {
    final StandardDestinationDefinition newDestinationDef = ConnectorRegistryConverters.toStandardDestinationDefinition(newDef);
    final ActorDefinitionVersion newADV = ConnectorRegistryConverters.toActorDefinitionVersion(newDef);

    final boolean connectorIsNew = !actorDefinitionIdsAndDefaultVersions.containsKey(newDestinationDef.getDestinationDefinitionId());
    if (connectorIsNew) {
      LOGGER.info("Adding new connector {}:{}", newDef.getDockerRepository(), newDef.getDockerImageTag());
      newConnectorCount++;
      configRepository.writeDestinationDefinitionAndDefaultVersion(newDestinationDef, newADV);
      return;
    }

    final ActorDefinitionVersion currentDefaultADV = actorDefinitionIdsAndDefaultVersions.get(newDestinationDef.getDestinationDefinitionId());
    final boolean shouldUpdateActorDefinitionDefaultVersion =
        getShouldUpdateActorDefinitionDefaultVersion(currentDefaultADV, newADV, actorDefinitionIdsInUse, updateAll);

    if (shouldUpdateActorDefinitionDefaultVersion) {
      LOGGER.info("Updating default version for connector {}: {} -> {}", currentDefaultADV.getDockerRepository(),
          currentDefaultADV.getDockerImageTag(),
          newADV.getDockerImageTag());
      changedConnectorCount++;
      configRepository.writeDestinationDefinitionAndDefaultVersion(newDestinationDef, newADV);
    } else {
      configRepository.updateStandardDestinationDefinition(newDestinationDef);
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
      }
      return isSupported;
    }).toList();
  }

  private boolean isProtocolVersionSupported(final AirbyteProtocolVersionRange protocolVersionRange, final String protocolVersion) {
    return protocolVersionRange.isSupported(AirbyteProtocolVersion.getWithDefault(protocolVersion));
  }

}
