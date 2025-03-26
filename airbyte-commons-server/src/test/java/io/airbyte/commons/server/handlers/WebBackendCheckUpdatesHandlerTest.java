/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.airbyte.api.model.generated.DestinationDefinitionRead;
import io.airbyte.api.model.generated.DestinationDefinitionReadList;
import io.airbyte.api.model.generated.SourceDefinitionRead;
import io.airbyte.api.model.generated.SourceDefinitionReadList;
import io.airbyte.api.model.generated.WebBackendCheckUpdatesRead;
import io.airbyte.config.ConnectorRegistryDestinationDefinition;
import io.airbyte.config.ConnectorRegistrySourceDefinition;
import io.airbyte.config.specs.RemoteDefinitionsProvider;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class WebBackendCheckUpdatesHandlerTest {

  SourceDefinitionsHandler sourceDefinitionsHandler;
  DestinationDefinitionsHandler destinationDefinitionsHandler;
  RemoteDefinitionsProvider remoteDefinitionsProvider;
  WebBackendCheckUpdatesHandler webBackendCheckUpdatesHandler;

  @BeforeEach
  void beforeEach() {
    sourceDefinitionsHandler = mock(SourceDefinitionsHandler.class);
    destinationDefinitionsHandler = mock(DestinationDefinitionsHandler.class);
    remoteDefinitionsProvider = mock(RemoteDefinitionsProvider.class);
    webBackendCheckUpdatesHandler =
        new WebBackendCheckUpdatesHandler(sourceDefinitionsHandler, destinationDefinitionsHandler, remoteDefinitionsProvider);
  }

  @Test
  void testCheckWithoutUpdate() throws IOException, InterruptedException {
    final UUID source1 = UUID.randomUUID();
    final UUID source2 = UUID.randomUUID();
    final String sourceTag1 = "1.0.0";
    final String sourceTag2 = "2.0.0";

    final UUID dest1 = UUID.randomUUID();
    final UUID dest2 = UUID.randomUUID();
    final String destTag1 = "0.1.0";
    final String destTag2 = "0.2.0";

    setMocks(
        List.of(Map.entry(source1, sourceTag1), Map.entry(source2, sourceTag2), Map.entry(source2, sourceTag2)),
        List.of(Map.entry(source1, sourceTag1), Map.entry(source2, sourceTag2)),
        List.of(Map.entry(dest1, destTag1), Map.entry(dest2, destTag2)),
        List.of(Map.entry(dest1, destTag1), Map.entry(dest2, destTag2)));

    final WebBackendCheckUpdatesRead actual = webBackendCheckUpdatesHandler.checkUpdates();

    assertEquals(new WebBackendCheckUpdatesRead().destinationDefinitions(0).sourceDefinitions(0), actual);
  }

  @Test
  void testCheckWithUpdate() throws IOException, InterruptedException {
    final UUID source1 = UUID.randomUUID();
    final UUID source2 = UUID.randomUUID();
    final String sourceTag1 = "1.1.0";
    final String sourceTag2 = "2.1.0";

    final UUID dest1 = UUID.randomUUID();
    final UUID dest2 = UUID.randomUUID();
    final String destTag1 = "0.1.0";
    final String destTag2 = "0.2.0";

    setMocks(
        List.of(Map.entry(source1, sourceTag1), Map.entry(source2, sourceTag2), Map.entry(source2, sourceTag2)),
        List.of(Map.entry(source1, "1.1.1"), Map.entry(source2, sourceTag2)),
        List.of(Map.entry(dest1, destTag1), Map.entry(dest2, destTag2), Map.entry(dest2, destTag2)),
        List.of(Map.entry(dest1, destTag1), Map.entry(dest2, "0.3.0")));

    final WebBackendCheckUpdatesRead actual = webBackendCheckUpdatesHandler.checkUpdates();

    assertEquals(new WebBackendCheckUpdatesRead().destinationDefinitions(2).sourceDefinitions(1), actual);
  }

  @Test
  void testCheckWithMissingActorDefFromLatest() throws IOException, InterruptedException {
    final UUID source1 = UUID.randomUUID();
    final UUID source2 = UUID.randomUUID();
    final String sourceTag1 = "1.0.0";
    final String sourceTag2 = "2.0.0";

    final UUID dest1 = UUID.randomUUID();
    final UUID dest2 = UUID.randomUUID();
    final String destTag1 = "0.1.0";
    final String destTag2 = "0.2.0";

    setMocks(
        List.of(Map.entry(source1, sourceTag1), Map.entry(source2, sourceTag2), Map.entry(source2, sourceTag2)),
        List.of(Map.entry(source2, sourceTag2)),
        List.of(Map.entry(dest1, destTag1), Map.entry(dest2, destTag2)),
        List.of(Map.entry(dest1, destTag1)));

    final WebBackendCheckUpdatesRead actual = webBackendCheckUpdatesHandler.checkUpdates();

    assertEquals(new WebBackendCheckUpdatesRead().destinationDefinitions(0).sourceDefinitions(0), actual);
  }

  @Test
  void testCheckErrorNoCurrentDestinations() throws IOException, InterruptedException {
    setMocksForExceptionCases();
    when(destinationDefinitionsHandler.listDestinationDefinitions()).thenThrow(new IOException("unable to read current destinations"));

    final WebBackendCheckUpdatesRead actual = webBackendCheckUpdatesHandler.checkUpdates();

    assertEquals(new WebBackendCheckUpdatesRead().destinationDefinitions(0).sourceDefinitions(1), actual);
  }

  @Test
  void testCheckErrorNoCurrentSources() throws IOException, InterruptedException {
    setMocksForExceptionCases();
    when(sourceDefinitionsHandler.listSourceDefinitions()).thenThrow(new IOException("unable to read current sources"));

    final WebBackendCheckUpdatesRead actual = webBackendCheckUpdatesHandler.checkUpdates();

    assertEquals(new WebBackendCheckUpdatesRead().destinationDefinitions(1).sourceDefinitions(0), actual);
  }

  private void setMocksForExceptionCases() throws IOException, InterruptedException {
    final UUID source1 = UUID.randomUUID();
    final String sourceTag1 = source1.toString();

    final UUID dest1 = UUID.randomUUID();
    final String destTag1 = dest1.toString();

    setMocks(
        List.of(Map.entry(source1, sourceTag1)),
        List.of(Map.entry(source1, UUID.randomUUID().toString())),
        List.of(Map.entry(dest1, destTag1)),
        List.of(Map.entry(dest1, UUID.randomUUID().toString())));
  }

  private void setMocks(final List<Entry<UUID, String>> currentSources,
                        final List<Entry<UUID, String>> latestSources,
                        final List<Entry<UUID, String>> currentDestinations,
                        final List<Entry<UUID, String>> latestDestinations)
      throws IOException, InterruptedException {
    when(sourceDefinitionsHandler.listSourceDefinitions())
        .thenReturn(new SourceDefinitionReadList().sourceDefinitions(currentSources.stream().map(this::createSourceDef).toList()));
    when(remoteDefinitionsProvider.getSourceDefinitions())
        .thenReturn(latestSources.stream().map(this::createRegistrySourceDef).toList());

    when(destinationDefinitionsHandler.listDestinationDefinitions())
        .thenReturn(
            new DestinationDefinitionReadList().destinationDefinitions(currentDestinations.stream().map(this::createDestinationDef).toList()));
    when(remoteDefinitionsProvider.getDestinationDefinitions())
        .thenReturn(latestDestinations.stream().map(this::createRegistryDestinationDef).toList());
  }

  private ConnectorRegistryDestinationDefinition createRegistryDestinationDef(final Entry<UUID, String> idImageTagEntry) {
    return new ConnectorRegistryDestinationDefinition()
        .withDestinationDefinitionId(idImageTagEntry.getKey())
        .withDockerImageTag(idImageTagEntry.getValue());
  }

  private DestinationDefinitionRead createDestinationDef(final Entry<UUID, String> idImageTagEntry) {
    return new DestinationDefinitionRead()
        .destinationDefinitionId(idImageTagEntry.getKey())
        .dockerImageTag(idImageTagEntry.getValue());
  }

  private ConnectorRegistrySourceDefinition createRegistrySourceDef(final Entry<UUID, String> idImageTagEntry) {
    return new ConnectorRegistrySourceDefinition()
        .withSourceDefinitionId(idImageTagEntry.getKey())
        .withDockerImageTag(idImageTagEntry.getValue());
  }

  private SourceDefinitionRead createSourceDef(final Entry<UUID, String> idImageTagEntry) {
    return new SourceDefinitionRead()
        .sourceDefinitionId(idImageTagEntry.getKey())
        .dockerImageTag(idImageTagEntry.getValue());
  }

}
