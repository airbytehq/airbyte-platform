/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.init;

import io.airbyte.commons.version.AirbyteProtocolVersion;
import io.airbyte.commons.version.AirbyteProtocolVersionRange;
import io.airbyte.config.ConnectorRegistryDestinationDefinition;
import io.airbyte.config.ConnectorRegistrySourceDefinition;
import io.airbyte.config.persistence.ActorDefinitionMigrator;
import io.airbyte.persistence.job.JobPersistence;
import io.airbyte.validation.json.JsonValidationException;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Singleton;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;

/**
 * Helper class used to apply actor definitions from a DefinitionsProvider to the database. This is
 * here to enable easy reuse of definition application logic in bootloader and cron.
 */
@Singleton
@Requires(bean = ActorDefinitionMigrator.class)
@Requires(bean = JobPersistence.class)
@Slf4j
public class ApplyDefinitionsHelper {

  private final ActorDefinitionMigrator actorDefinitionMigrator;
  private final Optional<DefinitionsProvider> definitionsProviderOptional;
  private final JobPersistence jobPersistence;

  public ApplyDefinitionsHelper(final ActorDefinitionMigrator actorDefinitionMigrator,
                                final Optional<DefinitionsProvider> definitionsProviderOptional,
                                final JobPersistence jobPersistence) {
    this.actorDefinitionMigrator = actorDefinitionMigrator;
    this.definitionsProviderOptional = definitionsProviderOptional;
    this.jobPersistence = jobPersistence;
  }

  public void apply() throws JsonValidationException, IOException {
    apply(false);
  }

  /**
   * Apply the latest definitions from the provider to the repository.
   *
   * @param updateAll - Whether we should overwrite all stored definitions
   */
  public void apply(final boolean updateAll) throws JsonValidationException, IOException {
    if (definitionsProviderOptional.isEmpty()) {
      log.warn("Skipping application of latest definitions.  Definitions provider not configured.");
      return;
    }
    final DefinitionsProvider definitionsProvider = definitionsProviderOptional.get();
    final Optional<AirbyteProtocolVersionRange> currentProtocolRange = getCurrentProtocolRange();

    actorDefinitionMigrator.migrate(
        filterSourceDefinitions(currentProtocolRange, definitionsProvider.getSourceDefinitions()),
        filterDestinationDefinitions(currentProtocolRange, definitionsProvider.getDestinationDefinitions()),
        updateAll);
  }

  private List<ConnectorRegistryDestinationDefinition> filterDestinationDefinitions(final Optional<AirbyteProtocolVersionRange> protocolVersionRange,
                                                                                    final List<ConnectorRegistryDestinationDefinition> destDefs) {
    if (protocolVersionRange.isEmpty()) {
      return destDefs;
    }

    return destDefs.stream().filter(def -> {
      final boolean isSupported = isProtocolVersionSupported(protocolVersionRange.get(), def.getSpec().getProtocolVersion());
      if (!isSupported) {
        log.warn("Destination {} {} has an incompatible protocol version ({})... ignoring.",
            def.getDestinationDefinitionId(), def.getName(), def.getSpec().getProtocolVersion());
      }
      return isSupported;
    }).toList();
  }

  private List<ConnectorRegistrySourceDefinition> filterSourceDefinitions(final Optional<AirbyteProtocolVersionRange> protocolVersionRange,
                                                                          final List<ConnectorRegistrySourceDefinition> sourceDefs) {
    if (protocolVersionRange.isEmpty()) {
      return sourceDefs;
    }

    return sourceDefs.stream().filter(def -> {
      final boolean isSupported = isProtocolVersionSupported(protocolVersionRange.get(), def.getSpec().getProtocolVersion());
      if (!isSupported) {
        log.warn("Source {} {} has an incompatible protocol version ({})... ignoring.",
            def.getSourceDefinitionId(), def.getName(), def.getSpec().getProtocolVersion());
      }
      return isSupported;
    }).toList();
  }

  private boolean isProtocolVersionSupported(final AirbyteProtocolVersionRange protocolVersionRange, final String protocolVersion) {
    return protocolVersionRange.isSupported(AirbyteProtocolVersion.getWithDefault(protocolVersion));
  }

  private Optional<AirbyteProtocolVersionRange> getCurrentProtocolRange() throws IOException {
    if (jobPersistence == null) {
      // TODO Remove this once cloud has been migrated and job persistence is always defined
      return Optional.empty();
    }

    return jobPersistence.getCurrentProtocolVersionRange();
  }

}
