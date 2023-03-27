/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.init;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.airbyte.config.persistence.ConfigRepository;
import io.airbyte.validation.json.JsonValidationException;
import java.io.IOException;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class DeclarativeSourceUpdaterTest {

  private DeclarativeSourceUpdater declarativeSourceUpdater;

  private ConfigRepository configRepository;
  private CdkVersionProvider cdkVersionProvider;

  @BeforeEach
  void setup() {
    configRepository = mock(ConfigRepository.class);
    cdkVersionProvider = mock(CdkVersionProvider.class);
    when(cdkVersionProvider.getCdkVersion()).thenReturn("1.2.3");

    declarativeSourceUpdater = new DeclarativeSourceUpdater(configRepository, cdkVersionProvider);
  }

  @Test
  void testUpdateDefinitions() throws IOException, JsonValidationException {
    final UUID id1 = UUID.randomUUID();
    final UUID id2 = UUID.randomUUID();
    when(configRepository.getActorDefinitionIdsWithActiveDeclarativeManifest()).thenReturn(Stream.of(id1, id2));
    declarativeSourceUpdater.apply();
    verify(configRepository).updateActorDefinitionsDockerImageTag(List.of(id1, id2), "1.2.3");
  }

  @Test
  void testDoNothing() throws IOException, JsonValidationException {
    when(configRepository.getActorDefinitionIdsWithActiveDeclarativeManifest()).thenReturn(Stream.empty());
    declarativeSourceUpdater.apply();
    verify(configRepository, never()).updateActorDefinitionsDockerImageTag(any(), any());
  }

}
