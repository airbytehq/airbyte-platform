/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers.helpers;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import io.airbyte.api.model.generated.ExistingConnectorBuilderProjectWithWorkspaceId;
import io.airbyte.data.exceptions.ConfigNotFoundException;
import java.io.IOException;
import java.util.List;
import org.junit.Test;
import org.junit.jupiter.api.DisplayName;

public class CompositeBuilderProjectUpdaterTest {

  @Test
  @DisplayName("updateConnectorBuilderProject should call updateConnectorBuilderProject on underlying updaters")
  public void testUpdateCompositeBuilderProjectUpdaterDelegates() throws ConfigNotFoundException, IOException {
    final ExistingConnectorBuilderProjectWithWorkspaceId update = mock(ExistingConnectorBuilderProjectWithWorkspaceId.class);
    final BuilderProjectUpdater updaterA = mock(BuilderProjectUpdater.class);
    final BuilderProjectUpdater updaterB = mock(BuilderProjectUpdater.class);
    final CompositeBuilderProjectUpdater projectUpdater = new CompositeBuilderProjectUpdater(List.of(updaterA, updaterB));
    projectUpdater.persistBuilderProjectUpdate(update);

    verify(updaterA, times(1))
        .persistBuilderProjectUpdate(update);
    verify(updaterB, times(1))
        .persistBuilderProjectUpdate(update);
  }

}
