/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.helpers;

import com.fasterxml.jackson.databind.JsonNode;
import io.airbyte.api.model.generated.SourceRead;
import io.airbyte.api.model.generated.SupportState;
import io.airbyte.commons.json.Jsons;
import io.airbyte.config.ScopedResourceRequirements;
import io.airbyte.config.SourceConnection;
import io.airbyte.config.StandardSourceDefinition;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.UUID;

public class SourceHelpers {

  public static SourceConnection generateSource(final UUID sourceDefinitionId) throws IOException {
    return generateSource(sourceDefinitionId, "my default source name", false, null);
  }

  public static SourceConnection generateSource(final UUID sourceDefinitionId, final String name) throws IOException {
    return generateSource(sourceDefinitionId, name, false, null);
  }

  public static SourceConnection generateSource(final UUID sourceDefinitionId, final boolean tombstone) throws IOException {
    return generateSource(sourceDefinitionId, "my default source name", tombstone, null);
  }

  public static SourceConnection generateSource(final UUID sourceDefinitionId, final ScopedResourceRequirements resourceRequirements)
      throws IOException {
    return generateSource(sourceDefinitionId, "my default source name", false, resourceRequirements);
  }

  public static SourceConnection generateSource(final UUID sourceDefinitionId,
                                                final String name,
                                                final boolean tombstone,
                                                final ScopedResourceRequirements resourceRequirements)
      throws IOException {
    final UUID workspaceId = UUID.randomUUID();
    final UUID sourceId = UUID.randomUUID();

    final JsonNode implementationJson = getTestImplementationJson();

    return new SourceConnection()
        .withName(name)
        .withWorkspaceId(workspaceId)
        .withSourceDefinitionId(sourceDefinitionId)
        .withSourceId(sourceId)
        .withConfiguration(implementationJson)
        .withTombstone(tombstone)
        .withResourceRequirements(resourceRequirements);
  }

  public static JsonNode getTestImplementationJson() throws IOException {
    final Path path = Paths.get(SourceHelpers.class.getClassLoader().getResource("json/TestImplementation.json").getPath());
    return Jsons.deserialize(Files.readString(path));
  }

  public static SourceRead getSourceRead(final SourceConnection source, final StandardSourceDefinition standardSourceDefinition) {
    // sets reasonable defaults for isVersionOverrideApplied and supportState, use below method instead
    // if you want to override them.
    return getSourceRead(source, standardSourceDefinition, false, SupportState.SUPPORTED, null);
  }

  public static SourceRead getSourceRead(final SourceConnection source,
                                         final StandardSourceDefinition standardSourceDefinition,
                                         final boolean isVersionOverrideApplied,
                                         final SupportState supportState,
                                         final io.airbyte.api.model.generated.ScopedResourceRequirements resourceAllocation) {

    return new SourceRead()
        .sourceDefinitionId(standardSourceDefinition.getSourceDefinitionId())
        .workspaceId(source.getWorkspaceId())
        .sourceDefinitionId(source.getSourceDefinitionId())
        .sourceId(source.getSourceId())
        .connectionConfiguration(source.getConfiguration())
        .name(source.getName())
        .sourceName(standardSourceDefinition.getName())
        .icon(standardSourceDefinition.getIconUrl())
        .isVersionOverrideApplied(isVersionOverrideApplied)
        .supportState(supportState)
        .resourceAllocation(resourceAllocation);
  }

}
