/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.persistence;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import io.airbyte.commons.version.Version;
import io.airbyte.config.ActorDefinitionBreakingChange;
import io.airbyte.config.ActorDefinitionVersion;
import io.airbyte.data.exceptions.ConfigNotFoundException;
import io.airbyte.data.services.ActorDefinitionService;
import java.io.IOException;
import java.util.List;
import org.junit.jupiter.api.Test;

class BreakingChangesHelperTest {

  @Test
  void testGetLastApplicableBreakingChange() throws ConfigNotFoundException, IOException {
    final ActorDefinitionVersion defaultVersion = new ActorDefinitionVersion()
        .withDockerImageTag("2.0.0");

    final ActorDefinitionBreakingChange firstBreakingChange = new ActorDefinitionBreakingChange()
        .withVersion(new Version("1.0.0"));
    final ActorDefinitionBreakingChange lastBreakingChange = new ActorDefinitionBreakingChange()
        .withVersion(new Version("2.0.0"));
    final ActorDefinitionBreakingChange inapplicableBreakingChange = new ActorDefinitionBreakingChange()
        .withVersion(new Version("3.0.0"));
    final List<ActorDefinitionBreakingChange> breakingChanges = List.of(firstBreakingChange, lastBreakingChange, inapplicableBreakingChange);

    final ActorDefinitionService mActorDefinitionService = mock(ActorDefinitionService.class);
    when(mActorDefinitionService.getActorDefinitionVersion(defaultVersion.getVersionId()))
        .thenReturn(defaultVersion);

    final ActorDefinitionBreakingChange result =
        BreakingChangesHelper.getLastApplicableBreakingChange(mActorDefinitionService, defaultVersion.getVersionId(), breakingChanges);
    assertEquals(lastBreakingChange, result);

    verify(mActorDefinitionService).getActorDefinitionVersion(defaultVersion.getVersionId());
    verifyNoMoreInteractions(mActorDefinitionService);
  }

}
